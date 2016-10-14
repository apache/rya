package org.apache.rya.accumulo;

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



import static org.apache.rya.accumulo.AccumuloRdfConstants.EMPTY_VALUE;

import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.RdfCloudTripleStoreConstants;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.resolver.RyaTripleContext;
import org.apache.rya.api.resolver.triple.TripleRow;
import org.apache.rya.api.resolver.triple.TripleRowResolverException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

public class RyaTableKeyValues {
    public static final ColumnVisibility EMPTY_CV = new ColumnVisibility();
    public static final Text EMPTY_CV_TEXT = new Text(EMPTY_CV.getExpression());

    RyaTripleContext instance;

    private RyaStatement stmt;
    private Collection<Map.Entry<Key, Value>> spo = new ArrayList<Map.Entry<Key, Value>>();
    private Collection<Map.Entry<Key, Value>> po = new ArrayList<Map.Entry<Key, Value>>();
    private Collection<Map.Entry<Key, Value>> osp = new ArrayList<Map.Entry<Key, Value>>();

    public RyaTableKeyValues(RyaStatement stmt, RdfCloudTripleStoreConfiguration conf) {
        this.stmt = stmt;
        this.instance = RyaTripleContext.getInstance(conf);
    }

    public Collection<Map.Entry<Key, Value>> getSpo() {
        return spo;
    }

    public Collection<Map.Entry<Key, Value>> getPo() {
        return po;
    }

    public Collection<Map.Entry<Key, Value>> getOsp() {
        return osp;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public RyaTableKeyValues invoke() throws IOException {
        /**
         * TODO: If there are contexts, do we still replicate the information into the default graph as well
         * as the named graphs?
         */try {
            Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, org.apache.rya.api.resolver.triple.TripleRow> rowMap = instance.serializeTriple(stmt);
            TripleRow tripleRow = rowMap.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.SPO);
            byte[] columnVisibility = tripleRow.getColumnVisibility();
            Text cv = columnVisibility == null ? EMPTY_CV_TEXT : new Text(columnVisibility);
            Long timestamp = tripleRow.getTimestamp();
            timestamp = timestamp == null ? 0l : timestamp;
            byte[] value = tripleRow.getValue();
            Value v = value == null ? EMPTY_VALUE : new Value(value);
            spo.add(new SimpleEntry(new Key(new Text(tripleRow.getRow()),
                    new Text(tripleRow.getColumnFamily()),
                    new Text(tripleRow.getColumnQualifier()),
                    cv, timestamp), v));
            tripleRow = rowMap.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.PO);
            po.add(new SimpleEntry(new Key(new Text(tripleRow.getRow()),
                    new Text(tripleRow.getColumnFamily()),
                    new Text(tripleRow.getColumnQualifier()),
                    cv, timestamp), v));
            tripleRow = rowMap.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.OSP);
            osp.add(new SimpleEntry(new Key(new Text(tripleRow.getRow()),
                    new Text(tripleRow.getColumnFamily()),
                    new Text(tripleRow.getColumnQualifier()),
                    cv, timestamp), v));
        } catch (TripleRowResolverException e) {
            throw new IOException(e);
        }
        return this;
    }

    @Override
    public String toString() {
        return "RyaTableKeyValues{" +
                "statement=" + stmt +
                ", spo=" + spo +
                ", po=" + po +
                ", o=" + osp +
                '}';
    }
}
