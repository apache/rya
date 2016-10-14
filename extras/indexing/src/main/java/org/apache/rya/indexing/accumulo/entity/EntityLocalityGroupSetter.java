package org.apache.rya.indexing.accumulo.entity;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

public class EntityLocalityGroupSetter {

    
    String tablePrefix;
    Connector conn;
    Configuration conf;
    
    public EntityLocalityGroupSetter(String tablePrefix, Connector conn, Configuration conf) {
        this.conn = conn;
        this.tablePrefix = tablePrefix;
        this.conf = conf;
    }
    
    
    
    private Iterator<String> getPredicates() {
        
        String auths = conf.get(ConfigUtils.CLOUDBASE_AUTHS);
        BatchScanner bs = null;
        try {
            bs = conn.createBatchScanner(tablePrefix + "prospects", new Authorizations(auths), 10);
        } catch (TableNotFoundException e) {
            e.printStackTrace();
        }
        bs.setRanges(Collections.singleton(Range.prefix(new Text("predicate" + "\u0000"))));
        final Iterator<Entry<Key,Value>> iter = bs.iterator();
        
        return new Iterator<String>() {

            private String next = null;
            private boolean hasNextCalled = false;
            private boolean isEmpty = false;

            @Override
            public boolean hasNext() {

                if (!hasNextCalled && !isEmpty) {
                    while (iter.hasNext()) {
                        Entry<Key,Value> temp = iter.next();
                        String row = temp.getKey().getRow().toString();
                        String[] rowArray = row.split("\u0000");
                        next = rowArray[1];
                        
                        hasNextCalled = true;
                        return true;
                    }
                    isEmpty = true;
                    return false;
                } else if(isEmpty) {
                    return false;
                }else {
                    return true;
                }
            }

            @Override
            public String next() {

                if (hasNextCalled) {
                    hasNextCalled = false;
                    return next;
                } else if(isEmpty) {
                    throw new NoSuchElementException();
                }else {
                    if (this.hasNext()) {
                        hasNextCalled = false;
                        return next;
                    } else {
                        throw new NoSuchElementException();
                    }
                }
            }

            @Override
            public void remove() {

                throw new UnsupportedOperationException("Cannot delete from iterator!");

            }

        }; 
    }
    
    
    
    
    
    
    
    
    public void setLocalityGroups() {
        
        HashMap<String, Set<Text>> localityGroups = new HashMap<String, Set<Text>>();
        Iterator<String> groups = getPredicates();
        
        int i = 1;
        
        while(groups.hasNext()) {
            HashSet<Text> tempColumn = new HashSet<Text>();
            String temp = groups.next();
            tempColumn.add(new Text(temp));
            String groupName = "predicate" + i;
            localityGroups.put(groupName, tempColumn);
            i++;
        }
        

        try {
            conn.tableOperations().setLocalityGroups(tablePrefix + "doc_partitioned_index", localityGroups);
            //conn.tableOperations().compact(tablePrefix + "doc_partitioned_index", null, null, true, true);
        } catch (AccumuloException e) {
            e.printStackTrace();
        } catch (AccumuloSecurityException e) {
            e.printStackTrace();
        } catch (TableNotFoundException e) {
            e.printStackTrace();
        }

        
        
    }
    
    
    
    
    
    
    
}
