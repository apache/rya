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
package org.apache.rya.indexing.pcj.storage.mongo;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.indexing.pcj.storage.PcjMetadata;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.apache.rya.mongodb.MongoITBase;
import org.bson.Document;
import org.junit.Test;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.impl.MapBindingSet;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class MongoPcjDocumentsTest extends MongoITBase {
    @Test
    public void pcjToMetadata() throws Exception {
        final MongoPcjDocuments docConverter = new MongoPcjDocuments(getMongoClient(), conf.getRyaInstanceName());
        final String sparql = "SELECT * WHERE { ?a <http://isA> ?b }";
        final Document actual = docConverter.makeMetadataDocument("pcjTest", sparql);
        final Document expected = new Document()
                .append(MongoPcjDocuments.CARDINALITY_FIELD, 0)
                .append(MongoPcjDocuments.PCJ_METADATA_ID, "pcjTest_METADATA")
                .append(MongoPcjDocuments.SPARQL_FIELD, sparql)
                .append(MongoPcjDocuments.VAR_ORDER_FIELD, Sets.newHashSet(new VariableOrder("a", "b"), new VariableOrder("b", "a")));
        assertEquals(expected, actual);
    }

    @Test
    public void metadataExists() throws Exception {
        final List<VariableOrder> varOrders = Lists.newArrayList(new VariableOrder("b", "a"), new VariableOrder("a", "b"));
        final MongoPcjDocuments docConverter = new MongoPcjDocuments(getMongoClient(), conf.getRyaInstanceName());
        final String sparql = "SELECT * WHERE { ?a <http://isA> ?b }";
        docConverter.createPcj("pcjTest", sparql);

        PcjMetadata actual = docConverter.getPcjMetadata("pcjTest");
        PcjMetadata expected = new PcjMetadata(sparql, 0, varOrders);
        assertEquals(expected, actual);

        // Setup the binding set that will be converted.
        final MapBindingSet originalBindingSet1 = new MapBindingSet();
        originalBindingSet1.addBinding("x", new URIImpl("http://a"));
        originalBindingSet1.addBinding("y", new URIImpl("http://b"));
        originalBindingSet1.addBinding("z", new URIImpl("http://c"));
        final VisibilityBindingSet results1 = new VisibilityBindingSet(originalBindingSet1, "A&B&C");

        // Setup the binding set that will be converted.
        final MapBindingSet originalBindingSet2 = new MapBindingSet();
        originalBindingSet2.addBinding("x", new URIImpl("http://1"));
        originalBindingSet2.addBinding("y", new URIImpl("http://2"));
        originalBindingSet2.addBinding("z", new URIImpl("http://3"));
        final VisibilityBindingSet results2 = new VisibilityBindingSet(originalBindingSet2, "A&B&C");

        final List<VisibilityBindingSet> bindingSets = new ArrayList<>();
        bindingSets.add(results1);
        bindingSets.add(results2);

        docConverter.addResults("pcjTest", bindingSets);
        actual = docConverter.getPcjMetadata("pcjTest");
        expected = new PcjMetadata(sparql, 2, varOrders);
        assertEquals(expected, actual);

        docConverter.purgePcjs("pcjTest");
        actual = docConverter.getPcjMetadata("pcjTest");
        expected = new PcjMetadata(sparql, 0, varOrders);
        assertEquals(expected, actual);
    }
}
