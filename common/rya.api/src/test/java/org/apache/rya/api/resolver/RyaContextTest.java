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

import org.apache.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;
import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.query.strategy.wholerow.MockRdfConfiguration;
import org.apache.rya.api.resolver.triple.TripleRow;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;

import junit.framework.TestCase;

/**
 */
public class RyaContextTest extends TestCase {

    public void testDefaultSerialization() throws Exception {
        final RyaContext instance = RyaContext.getInstance();
        //plain string
        RyaType ryaType = new RyaType("mydata");
        byte[] serialize = instance.serialize(ryaType);
        assertEquals(ryaType, instance.deserialize(serialize));

        //iri
        final RyaIRI ryaIRI = new RyaIRI("urn:test#1234");
        serialize = instance.serialize(ryaIRI);
        final RyaType deserialize = instance.deserialize(serialize);
        assertEquals(ryaIRI, deserialize);

        //custom type
        ryaType = new RyaType(SimpleValueFactory.getInstance().createIRI("urn:test#customDataType"), "mydata");
        serialize = instance.serialize(ryaType);
        assertEquals(ryaType, instance.deserialize(serialize));

        //language type
        ryaType = new RyaType(RDF.LANGSTRING, "Hello", "en");
        serialize = instance.serialize(ryaType);
        assertEquals(ryaType, instance.deserialize(serialize));
    }

    public void testTripleRowSerialization() throws Exception {
        final RyaIRI subj = new RyaIRI("urn:test#subj");
        final RyaIRI pred = new RyaIRI("urn:test#pred");
        final RyaType obj = new RyaType("mydata");
        final RyaStatement statement = new RyaStatement(subj, pred, obj);
        final RyaTripleContext instance = RyaTripleContext.getInstance(new MockRdfConfiguration());

        final Map<TABLE_LAYOUT, TripleRow> map = instance.serializeTriple(statement);
        final TripleRow tripleRow = map.get(TABLE_LAYOUT.SPO);
        assertEquals(statement, instance.deserializeTriple(TABLE_LAYOUT.SPO, tripleRow));
    }

    public void testHashedTripleRowSerialization() throws Exception {
        final RyaIRI subj = new RyaIRI("urn:test#subj");
        final RyaIRI pred = new RyaIRI("urn:test#pred");
        final RyaType obj = new RyaType("mydata");
        final RyaStatement statement = new RyaStatement(subj, pred, obj);
        final MockRdfConfiguration config = new MockRdfConfiguration();
        config.set(MockRdfConfiguration.CONF_PREFIX_ROW_WITH_HASH, Boolean.TRUE.toString());
        final RyaTripleContext instance = RyaTripleContext.getInstance(config);

        final Map<TABLE_LAYOUT, TripleRow> map = instance.serializeTriple(statement);
        final TripleRow tripleRow = map.get(TABLE_LAYOUT.SPO);
        assertEquals(statement, instance.deserializeTriple(TABLE_LAYOUT.SPO, tripleRow));
    }

}
