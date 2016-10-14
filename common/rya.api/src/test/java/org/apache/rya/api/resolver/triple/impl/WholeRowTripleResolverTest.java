package org.apache.rya.api.resolver.triple.impl;

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



import junit.framework.TestCase;
import org.apache.rya.api.RdfCloudTripleStoreConstants;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.resolver.triple.TripleRow;
import org.apache.rya.api.resolver.triple.TripleRowRegex;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Date: 7/25/12
 * Time: 10:52 AM
 */
public class WholeRowTripleResolverTest extends TestCase {

    WholeRowTripleResolver tripleResolver = new WholeRowTripleResolver();

    public void testSerialize() throws Exception {
        //no context
        RyaURI subj = new RyaURI("urn:test#1234");
        RyaURI pred = new RyaURI("urn:test#pred");
        RyaURI obj = new RyaURI("urn:test#obj");
        RyaURI cntxt = new RyaURI("urn:test#cntxt");
        final RyaStatement stmt = new RyaStatement(subj, pred, obj, null, null, null, null, 100l);
        final RyaStatement stmtContext = new RyaStatement(subj, pred, obj, cntxt, null, null, null, 100l);

        Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, TripleRow> serialize = tripleResolver.serialize(stmt);
        TripleRow tripleRow = serialize.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.SPO);
        RyaStatement deserialize = tripleResolver.deserialize(RdfCloudTripleStoreConstants.TABLE_LAYOUT.SPO, tripleRow);
        assertEquals(stmt, deserialize);

        //context
        serialize = tripleResolver.serialize(stmtContext);
        tripleRow = serialize.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.SPO);
        deserialize = tripleResolver.deserialize(RdfCloudTripleStoreConstants.TABLE_LAYOUT.SPO, tripleRow);
        assertEquals(stmtContext, deserialize);
    }

    public void testSerializePO() throws Exception {
        RdfCloudTripleStoreConstants.TABLE_LAYOUT po = RdfCloudTripleStoreConstants.TABLE_LAYOUT.PO;
        //no context
        RyaURI subj = new RyaURI("urn:test#1234");
        RyaURI pred = new RyaURI("urn:test#pred");
        RyaURI obj = new RyaURI("urn:test#obj");
        RyaURI cntxt = new RyaURI("urn:test#cntxt");
        final RyaStatement stmt = new RyaStatement(subj, pred, obj, null, null, null, null, 100l);
        final RyaStatement stmtContext = new RyaStatement(subj, pred, obj, cntxt, null, null, null, 100l);
        Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, TripleRow> serialize = tripleResolver.serialize(stmt);
        TripleRow tripleRow = serialize.get(po);
        RyaStatement deserialize = tripleResolver.deserialize(po, tripleRow);
        assertEquals(stmt, deserialize);

        //context
        serialize = tripleResolver.serialize(stmtContext);
        tripleRow = serialize.get(po);
        deserialize = tripleResolver.deserialize(po, tripleRow);
        assertEquals(stmtContext, deserialize);
    }

    public void testSerializeOSP() throws Exception {
        RdfCloudTripleStoreConstants.TABLE_LAYOUT po = RdfCloudTripleStoreConstants.TABLE_LAYOUT.OSP;
        //no context
        RyaURI subj = new RyaURI("urn:test#1234");
        RyaURI pred = new RyaURI("urn:test#pred");
        RyaURI obj = new RyaURI("urn:test#obj");
        RyaURI cntxt = new RyaURI("urn:test#cntxt");
        final RyaStatement stmt = new RyaStatement(subj, pred, obj, null, null, null, null, 100l);
        final RyaStatement stmtContext = new RyaStatement(subj, pred, obj, cntxt, null, null, null, 100l);
        Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, TripleRow> serialize = tripleResolver.serialize(stmt);
        TripleRow tripleRow = serialize.get(po);
        RyaStatement deserialize = tripleResolver.deserialize(po, tripleRow);
        assertEquals(stmt, deserialize);

        //context
        serialize = tripleResolver.serialize(stmtContext);
        tripleRow = serialize.get(po);
        deserialize = tripleResolver.deserialize(po, tripleRow);
        assertEquals(stmtContext, deserialize);
    }

    public void testSerializeOSPCustomType() throws Exception {
        RdfCloudTripleStoreConstants.TABLE_LAYOUT po = RdfCloudTripleStoreConstants.TABLE_LAYOUT.OSP;
        //no context
        RyaURI subj = new RyaURI("urn:test#1234");
        RyaURI pred = new RyaURI("urn:test#pred");
        RyaURI obj = new RyaURI("urn:test#obj");
        RyaURI cntxt = new RyaURI("urn:test#cntxt");
        final RyaStatement stmt = new RyaStatement(subj, pred, obj, null, null, null, null, 100l);
        final RyaStatement stmtContext = new RyaStatement(subj, pred, obj, cntxt, null, null, null, 100l);
        Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, TripleRow> serialize = tripleResolver.serialize(stmt);
        TripleRow tripleRow = serialize.get(po);
        RyaStatement deserialize = tripleResolver.deserialize(po, tripleRow);
        assertEquals(stmt, deserialize);

        //context
        serialize = tripleResolver.serialize(stmtContext);
        tripleRow = serialize.get(po);
        deserialize = tripleResolver.deserialize(po, tripleRow);
        assertEquals(stmtContext, deserialize);
    }

}
