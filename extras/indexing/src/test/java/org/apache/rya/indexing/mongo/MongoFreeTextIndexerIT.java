/**
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
package org.apache.rya.indexing.mongo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;

import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.api.resolver.RyaToRdfConversions;
import org.apache.rya.indexing.StatementConstraints;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.mongodb.freetext.MongoFreeTextIndexer;
import org.apache.rya.mongodb.MongoITBase;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDFS;

import com.google.common.collect.Sets;

import info.aduna.iteration.CloseableIteration;

/**
 * Integration tests the methods of {@link MongoFreeTextIndexer}.
 */
public class MongoFreeTextIndexerIT extends MongoITBase {
    private static final StatementConstraints EMPTY_CONSTRAINTS = new StatementConstraints();

    @Test
    public void testSearch() throws Exception {
        try (MongoFreeTextIndexer f = new MongoFreeTextIndexer()) {
            f.setConf(conf);
            f.init();

            final ValueFactory vf = new ValueFactoryImpl();

            final URI subject = new URIImpl("foo:subj");
            final URI predicate = RDFS.LABEL;
            final Value object = vf.createLiteral("this is a new hat");

            final URI context = new URIImpl("foo:context");

            final Statement statement = vf.createStatement(subject, predicate, object, context);
            f.storeStatement(RdfToRyaConversions.convertStatement(statement));
            f.flush();

            assertEquals(Sets.newHashSet(), getSet(f.queryText("asdf", EMPTY_CONSTRAINTS)));

            assertEquals(Sets.newHashSet(statement), getSet(f.queryText("new", EMPTY_CONSTRAINTS)));
            assertEquals(Sets.newHashSet(statement), getSet(f.queryText("hat new", EMPTY_CONSTRAINTS)));
        }
    }

    @Test
    public void testDelete() throws Exception {
        try (MongoFreeTextIndexer f = new MongoFreeTextIndexer()) {
            f.setConf(conf);
            f.init();

            final ValueFactory vf = new ValueFactoryImpl();

            final URI subject1 = new URIImpl("foo:subj");
            final URI predicate1 = RDFS.LABEL;
            final Value object1 = vf.createLiteral("this is a new hat");

            final URI context1 = new URIImpl("foo:context");

            final Statement statement1 = vf.createStatement(subject1, predicate1, object1, context1);
            f.storeStatement(RdfToRyaConversions.convertStatement(statement1));

            final URI subject2 = new URIImpl("foo:subject");
            final URI predicate2 = RDFS.LABEL;
            final Value object2 = vf.createLiteral("Do you like my new hat?");

            final URI context2 = new URIImpl("foo:context");

            final Statement statement2 = vf.createStatement(subject2, predicate2, object2, context2);
            f.storeStatement(RdfToRyaConversions.convertStatement(statement2));

            f.flush();


            f.deleteStatement(RdfToRyaConversions.convertStatement(statement1));
            assertEquals(Sets.newHashSet(statement2), getSet(f.queryText("Do you like my new hat?", EMPTY_CONSTRAINTS)));

            // Check that "new" didn't get deleted from the term table after "this is a new hat"
            // was deleted since "new" is still in "Do you like my new hat?"
            assertEquals(Sets.newHashSet(statement2), getSet(f.queryText("new", EMPTY_CONSTRAINTS)));

            f.deleteStatement(RdfToRyaConversions.convertStatement(statement2));
            assertEquals(Sets.newHashSet(), getSet(f.queryText("this is a new hat", EMPTY_CONSTRAINTS)));
            assertEquals(Sets.newHashSet(), getSet(f.queryText("Do you like my new hat?", EMPTY_CONSTRAINTS)));
        }
    }

    @Test
    public void testRestrictPredicatesSearch() throws Exception {
        conf.setStrings(ConfigUtils.FREETEXT_PREDICATES_LIST, "pred:1,pred:2");

        try (MongoFreeTextIndexer f = new MongoFreeTextIndexer()) {
            f.setConf(conf);
            f.init();

            // These should not be stored because they are not in the predicate list
            f.storeStatement(new RyaStatement(new RyaURI("foo:subj1"), new RyaURI(RDFS.LABEL.toString()), new RyaType("invalid")));
            f.storeStatement(new RyaStatement(new RyaURI("foo:subj2"), new RyaURI(RDFS.COMMENT.toString()), new RyaType("invalid")));

            final RyaURI pred1 = new RyaURI("pred:1");
            final RyaURI pred2 = new RyaURI("pred:2");

            // These should be stored because they are in the predicate list
            final RyaStatement s3 = new RyaStatement(new RyaURI("foo:subj3"), pred1, new RyaType("valid"));
            final RyaStatement s4 = new RyaStatement(new RyaURI("foo:subj4"), pred2, new RyaType("valid"));
            f.storeStatement(s3);
            f.storeStatement(s4);

            // This should not be stored because the object is not a literal
            f.storeStatement(new RyaStatement(new RyaURI("foo:subj5"), pred1, new RyaURI("in:validURI")));

            f.flush();

            assertEquals(Sets.newHashSet(), getSet(f.queryText("invalid", EMPTY_CONSTRAINTS)));
            assertEquals(Sets.newHashSet(), getSet(f.queryText("in:validURI", EMPTY_CONSTRAINTS)));

            final Set<Statement> actual = getSet(f.queryText("valid", EMPTY_CONSTRAINTS));
            assertEquals(2, actual.size());
            assertTrue(actual.contains(RyaToRdfConversions.convertStatement(s3)));
            assertTrue(actual.contains(RyaToRdfConversions.convertStatement(s4)));
        }
    }

    @Test
    public void testContextSearch() throws Exception {
        try (MongoFreeTextIndexer f = new MongoFreeTextIndexer()) {
            f.setConf(conf);
            f.init();

            final ValueFactory vf = new ValueFactoryImpl();
            final URI subject = new URIImpl("foo:subj");
            final URI predicate = new URIImpl(RDFS.COMMENT.toString());
            final Value object = vf.createLiteral("this is a new hat");
            final URI context = new URIImpl("foo:context");

            final Statement statement = vf.createStatement(subject, predicate, object, context);
            f.storeStatement(RdfToRyaConversions.convertStatement(statement));
            f.flush();

            assertEquals(Sets.newHashSet(statement), getSet(f.queryText("hat", EMPTY_CONSTRAINTS)));
            assertEquals(Sets.newHashSet(statement), getSet(f.queryText("hat", new StatementConstraints().setContext(context))));
            assertEquals(Sets.newHashSet(),
                    getSet(f.queryText("hat", new StatementConstraints().setContext(vf.createURI("foo:context2")))));
        }
    }

    private static <X> Set<X> getSet(final CloseableIteration<X, ?> iter) throws Exception {
        final Set<X> set = new HashSet<>();
        while (iter.hasNext()) {
            set.add(iter.next());
        }
        return set;
    }
}