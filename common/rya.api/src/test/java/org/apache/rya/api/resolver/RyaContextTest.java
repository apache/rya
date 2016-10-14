package org.apache.rya.api.resolver;

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



import java.util.Map;

import junit.framework.TestCase;
import org.apache.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.query.strategy.AbstractTriplePatternStrategyTest.MockRdfConfiguration;
import org.apache.rya.api.query.strategy.wholerow.MockRdfCloudConfiguration;
import org.apache.rya.api.resolver.triple.TripleRow;

import org.openrdf.model.impl.URIImpl;

/**
 */
public class RyaContextTest extends TestCase {
 
    public void testDefaultSerialization() throws Exception {
        RyaContext instance = RyaContext.getInstance();
        //plain string
        RyaType ryaType = new RyaType("mydata");
        byte[] serialize = instance.serialize(ryaType);
        assertEquals(ryaType, instance.deserialize(serialize));

        //uri
        RyaURI ryaURI = new RyaURI("urn:test#1234");
        serialize = instance.serialize(ryaURI);
        RyaType deserialize = instance.deserialize(serialize);
        assertEquals(ryaURI, deserialize);

        //custom type
        ryaType = new RyaType(new URIImpl("urn:test#customDataType"), "mydata");
        serialize = instance.serialize(ryaType);
        assertEquals(ryaType, instance.deserialize(serialize));
    }

    public void testTripleRowSerialization() throws Exception {
        RyaURI subj = new RyaURI("urn:test#subj");
        RyaURI pred = new RyaURI("urn:test#pred");
        RyaType obj = new RyaType("mydata");
        RyaStatement statement = new RyaStatement(subj, pred, obj);
        RyaTripleContext instance = RyaTripleContext.getInstance(new MockRdfCloudConfiguration());

        Map<TABLE_LAYOUT, TripleRow> map = instance.serializeTriple(statement);
        TripleRow tripleRow = map.get(TABLE_LAYOUT.SPO);
        assertEquals(statement, instance.deserializeTriple(TABLE_LAYOUT.SPO, tripleRow));
    }
    
    public void testHashedTripleRowSerialization() throws Exception {
        RyaURI subj = new RyaURI("urn:test#subj");
        RyaURI pred = new RyaURI("urn:test#pred");
        RyaType obj = new RyaType("mydata");
        RyaStatement statement = new RyaStatement(subj, pred, obj);
    	MockRdfCloudConfiguration config = new MockRdfCloudConfiguration();
    	config.set(MockRdfCloudConfiguration.CONF_PREFIX_ROW_WITH_HASH, Boolean.TRUE.toString());
       RyaTripleContext instance = RyaTripleContext.getInstance(config);

        Map<TABLE_LAYOUT, TripleRow> map = instance.serializeTriple(statement);
        TripleRow tripleRow = map.get(TABLE_LAYOUT.SPO);
        assertEquals(statement, instance.deserializeTriple(TABLE_LAYOUT.SPO, tripleRow));
    }

}
