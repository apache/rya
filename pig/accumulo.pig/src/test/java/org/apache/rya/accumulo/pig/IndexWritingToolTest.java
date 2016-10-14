package org.apache.rya.accumulo.pig;

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


import java.io.File;
import java.io.IOException;
import java.util.Map;
import junit.framework.Assert;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

public class IndexWritingToolTest {

  
    
    @Test
    public void testIndexWrite() {

        
        
        Connector accCon = null;
        Instance inst;
        
        String[] args = new String[7];
        
        args[0] = "src/test/resources/ResultsFile1.txt";
        args[1] = "src/test/resources/testQuery.txt";
        args[2] = "instance";
        args[3] = "mock";
        args[4] = "user";
        args[5] = "password";
        args[6] = "table";
       
        String query = null;
        try {
            query = FileUtils.readFileToString(new File(args[1]));
        } catch (IOException e1) {
            
            e1.printStackTrace();
        }
        

        try {
            inst = new MockInstance(args[2]);
            accCon = inst.getConnector(args[4], args[5].getBytes());
            if(accCon.tableOperations().exists(args[6])) {
                accCon.tableOperations().delete(args[6]);
            }
            
            accCon.tableOperations().create(args[6]);

        } catch (AccumuloException e) {
            e.printStackTrace();
        } catch (AccumuloSecurityException e) {
            e.printStackTrace();
        } catch (TableExistsException e) {
            e.printStackTrace();
        } catch (TableNotFoundException e) {
            e.printStackTrace();
        }
        
        
        int result = 5;
        try {
            result = ToolRunner.run(new IndexWritingTool(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        Assert.assertEquals(0, result);
        
        Scanner scan = null;
        
        try {
            scan = accCon.createScanner("table", new Authorizations());
        } catch (TableNotFoundException e) {
            
            e.printStackTrace();
        }
        scan.setRange(new Range());

        int count = 0;
        
        for (Map.Entry<Key, Value> entry : scan) {
            String[] k = entry.getKey().getRow().toString().split("\u0000");
            String[] c = entry.getKey().getColumnFamily().toString().split("\u0000");
            
            if(count == 0) {
                Assert.assertEquals(k[0], "person10");
                Assert.assertEquals(k[1], "person8");
                Assert.assertEquals(k[2], "person9");
                Assert.assertEquals(c[0],"z");
                Assert.assertEquals(c[1],"x");
                Assert.assertEquals(c[2],"y");
            }
            else if(count == 2) {
                Assert.assertEquals(k[0], "person2");
                Assert.assertEquals(k[1], "person1");
                Assert.assertEquals(k[2], "person3");
                Assert.assertEquals(c[0],"y");
                Assert.assertEquals(c[1],"x");
                Assert.assertEquals(c[2],"z");
            }
            else if(count == 5) {
                Assert.assertEquals(k[0], "person3");
                Assert.assertEquals(k[1], "person2");
                Assert.assertEquals(k[2], "person4");
                Assert.assertEquals(c[0],"y");
                Assert.assertEquals(c[1],"x");
                Assert.assertEquals(c[2],"z");
            }
            else if(count == 9) {
                Assert.assertEquals(k[0], "person5");
                Assert.assertEquals(k[1], "person3");
                Assert.assertEquals(k[2], "person4");
                Assert.assertEquals(c[0],"z");
                Assert.assertEquals(c[1],"x");
                Assert.assertEquals(c[2],"y");
            }
            else if(count == 13) {
                Assert.assertEquals(k[0], "person6");
                Assert.assertEquals(k[1], "person5");
                Assert.assertEquals(k[2], "person4");
                Assert.assertEquals(c[0],"z");
                Assert.assertEquals(c[1],"y");
                Assert.assertEquals(c[2],"x");
            }
            else if(count == 17) {
                Assert.assertEquals(k[0], "person7");
                Assert.assertEquals(k[1], "person6");
                Assert.assertEquals(k[2], "person8");
                Assert.assertEquals(c[0],"y");
                Assert.assertEquals(c[1],"x");
                Assert.assertEquals(c[2],"z");
            }
            else if(count == 21) {
                Assert.assertEquals(k[0], "person9");
                Assert.assertEquals(k[1], "person7");
                Assert.assertEquals(k[2], "person8");
                Assert.assertEquals(c[0],"z");
                Assert.assertEquals(c[1],"x");
                Assert.assertEquals(c[2],"y");
            } else if(count == 24) {
                Assert.assertEquals(query, entry.getValue().toString());
                String[] varOrders = entry.getKey().getColumnQualifier().toString().split("\u0000");
                Assert.assertEquals(3,varOrders.length);
                Assert.assertEquals(varOrders[0],"z;y;x");
                Assert.assertEquals(varOrders[1],"y;x;z");
                Assert.assertEquals(varOrders[2],"z;x;y");
                
            } else {
                Assert.assertTrue(k[0].startsWith("person"));
                Assert.assertTrue(k[1].startsWith("person"));
                Assert.assertTrue(k[2].startsWith("person"));
             
            }
            
            count ++;
        }
        
        Assert.assertEquals(25, count);
        
        
        

    }
    
    
    
    
    
    
    
    @Test
    public void testIndexWrite2() {

        
        
        Connector accCon = null;
        Instance inst;
        
        String[] args = new String[7];
        
        args[0] = "src/test/resources/ResultsFile1.txt";
        args[1] = "src/test/resources/testQuery2.txt";
        args[2] = "instance";
        args[3] = "mock";
        args[4] = "user";
        args[5] = "password";
        args[6] = "table";
       
        String query = null;
        try {
            query = FileUtils.readFileToString(new File(args[1]));
        } catch (IOException e1) {
            
            e1.printStackTrace();
        }
        

        try {
            inst = new MockInstance(args[2]);
            accCon = inst.getConnector(args[4], args[5].getBytes());
            if(accCon.tableOperations().exists(args[6])) {
                accCon.tableOperations().delete(args[6]);
            }
            
            accCon.tableOperations().create(args[6]);

        } catch (AccumuloException e) {
            
            e.printStackTrace();
        } catch (AccumuloSecurityException e) {
            
            e.printStackTrace();
        } catch (TableExistsException e) {
            
            e.printStackTrace();
        } catch (TableNotFoundException e) {
            
            e.printStackTrace();
        }
        
        
        int result = 5;
        try {
            result = ToolRunner.run(new IndexWritingTool(), args);
        } catch (Exception e) {
            
            e.printStackTrace();
        }
        
        Assert.assertEquals(0, result);
        
        Scanner scan = null;
        
        try {
            scan = accCon.createScanner("table", new Authorizations());
        } catch (TableNotFoundException e) {
            
            e.printStackTrace();
        }
        scan.setRange(new Range());

        int count = 0;
        
        for (Map.Entry<Key, Value> entry : scan) {
            String[] k = entry.getKey().getRow().toString().split("\u0000");
            String[] c = entry.getKey().getColumnFamily().toString().split("\u0000");
            
            if(count == 0) {
                Assert.assertEquals(k[0], "person1");
                Assert.assertEquals(k[1], "person2");
                Assert.assertEquals(k[2], "person3");
                Assert.assertEquals(c[0],"x");
                Assert.assertEquals(c[1],"y");
                Assert.assertEquals(c[2],"z");
            }
            else if(count == 2) {
                Assert.assertEquals(k[0], "person3");
                Assert.assertEquals(k[1], "person4");
                Assert.assertEquals(k[2], "person5");
                Assert.assertEquals(c[0],"x");
                Assert.assertEquals(c[1],"y");
                Assert.assertEquals(c[2],"z");
            }
            else if(count == 5) {
                Assert.assertEquals(k[0], "person6");
                Assert.assertEquals(k[1], "person7");
                Assert.assertEquals(k[2], "person8");
                Assert.assertEquals(c[0],"x");
                Assert.assertEquals(c[1],"y");
                Assert.assertEquals(c[2],"z");
            }
            
            
            count ++;
            System.out.println(count);
        }
        
        Assert.assertEquals(9, count);
        
        
        

    }
    
    
    
    
    
    
    

}
