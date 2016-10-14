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



import static org.apache.rya.accumulo.AccumuloRdfConstants.EMPTY_CV;
import static org.apache.rya.accumulo.AccumuloRdfConstants.EMPTY_VALUE;
import static org.apache.rya.api.RdfCloudTripleStoreConstants.EMPTY_TEXT;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.RdfCloudTripleStoreConstants;
import org.apache.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.resolver.RyaTripleContext;
import org.apache.rya.api.resolver.triple.TripleRow;
import org.apache.rya.api.resolver.triple.TripleRowResolverException;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

public class RyaTableMutationsFactory {

    RyaTripleContext ryaContext;

    public RyaTableMutationsFactory(RyaTripleContext ryaContext) {
    	this.ryaContext = ryaContext;
    }

    //TODO: Does this still need to be collections
    public Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize(
            RyaStatement stmt) throws IOException {

        Collection<Mutation> spo_muts = new ArrayList<Mutation>();
        Collection<Mutation> po_muts = new ArrayList<Mutation>();
        Collection<Mutation> osp_muts = new ArrayList<Mutation>();
        /**
         * TODO: If there are contexts, do we still replicate the information into the default graph as well
         * as the named graphs?
         */
        try {
            Map<TABLE_LAYOUT, TripleRow> rowMap = ryaContext.serializeTriple(stmt);
            TripleRow tripleRow = rowMap.get(TABLE_LAYOUT.SPO);
            spo_muts.add(createMutation(tripleRow));
            tripleRow = rowMap.get(TABLE_LAYOUT.PO);
            po_muts.add(createMutation(tripleRow));
            tripleRow = rowMap.get(TABLE_LAYOUT.OSP);
            osp_muts.add(createMutation(tripleRow));
        } catch (TripleRowResolverException fe) {
            throw new IOException(fe);
        }

        Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> mutations =
                new HashMap<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>>();
        mutations.put(RdfCloudTripleStoreConstants.TABLE_LAYOUT.SPO, spo_muts);
        mutations.put(RdfCloudTripleStoreConstants.TABLE_LAYOUT.PO, po_muts);
        mutations.put(RdfCloudTripleStoreConstants.TABLE_LAYOUT.OSP, osp_muts);

        return mutations;
    }

    public Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serializeDelete(
            RyaStatement stmt) throws IOException {

        Collection<Mutation> spo_muts = new ArrayList<Mutation>();
        Collection<Mutation> po_muts = new ArrayList<Mutation>();
        Collection<Mutation> osp_muts = new ArrayList<Mutation>();
        /**
         * TODO: If there are contexts, do we still replicate the information into the default graph as well
         * as the named graphs?
         */
        try {
            Map<TABLE_LAYOUT, TripleRow> rowMap = ryaContext.serializeTriple(stmt);
            TripleRow tripleRow = rowMap.get(TABLE_LAYOUT.SPO);
            spo_muts.add(deleteMutation(tripleRow));
            tripleRow = rowMap.get(TABLE_LAYOUT.PO);
            po_muts.add(deleteMutation(tripleRow));
            tripleRow = rowMap.get(TABLE_LAYOUT.OSP);
            osp_muts.add(deleteMutation(tripleRow));
        } catch (TripleRowResolverException fe) {
            throw new IOException(fe);
        }

        Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> mutations =
                new HashMap<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>>();
        mutations.put(RdfCloudTripleStoreConstants.TABLE_LAYOUT.SPO, spo_muts);
        mutations.put(RdfCloudTripleStoreConstants.TABLE_LAYOUT.PO, po_muts);
        mutations.put(RdfCloudTripleStoreConstants.TABLE_LAYOUT.OSP, osp_muts);

        return mutations;

    }

    protected Mutation deleteMutation(TripleRow tripleRow) {
        Mutation m = new Mutation(new Text(tripleRow.getRow()));

        byte[] columnFamily = tripleRow.getColumnFamily();
        Text cfText = columnFamily == null ? EMPTY_TEXT : new Text(columnFamily);

        byte[] columnQualifier = tripleRow.getColumnQualifier();
        Text cqText = columnQualifier == null ? EMPTY_TEXT : new Text(columnQualifier);

        m.putDelete(cfText, cqText, new ColumnVisibility(tripleRow.getColumnVisibility()),
                tripleRow.getTimestamp());
        return m;
    }

    protected Mutation createMutation(TripleRow tripleRow) {
        Mutation mutation = new Mutation(new Text(tripleRow.getRow()));
        byte[] columnVisibility = tripleRow.getColumnVisibility();
        ColumnVisibility cv = columnVisibility == null ? EMPTY_CV : new ColumnVisibility(columnVisibility);
        Long timestamp = tripleRow.getTimestamp();
        byte[] value = tripleRow.getValue();
        Value v = value == null ? EMPTY_VALUE : new Value(value);
        byte[] columnQualifier = tripleRow.getColumnQualifier();
        Text cqText = columnQualifier == null ? EMPTY_TEXT : new Text(columnQualifier);
        byte[] columnFamily = tripleRow.getColumnFamily();
        Text cfText = columnFamily == null ? EMPTY_TEXT : new Text(columnFamily);

        mutation.put(cfText, cqText, cv, timestamp, v);
        return mutation;
    }
}
