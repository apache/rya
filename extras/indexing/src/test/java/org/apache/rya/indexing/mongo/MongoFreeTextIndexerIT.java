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
import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.api.resolver.RyaToRdfConversions;
import org.apache.rya.indexing.StatementConstraints;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.mongodb.freetext.MongoFreeTextIndexer;
import org.apache.rya.mongodb.MongoITBase;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.junit.Test;

import com.google.common.collect.Sets;

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

            final ValueFactory vf = SimpleValueFactory.getInstance();

            final IRI subject = vf.createIRI("foo:subj");
            final IRI predicate = RDFS.LABEL;
            final Value object = vf.createLiteral("this is a new hat");

            final IRI context = vf.createIRI("foo:context");

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

            final ValueFactory vf = SimpleValueFactory.getInstance();

            final IRI subject1 = vf.createIRI("foo:subj");
            final IRI predicate1 = RDFS.LABEL;
            final Value object1 = vf.createLiteral("this is a new hat");

            final IRI context1 = vf.createIRI("foo:context");

            final Statement statement1 = vf.createStatement(subject1, predicate1, object1, context1);
            f.storeStatement(RdfToRyaConversions.convertStatement(statement1));

            final IRI subject2 = vf.createIRI("foo:subject");
            final IRI predicate2 = RDFS.LABEL;
            final Value object2 = vf.createLiteral("Do you like my new hat?");

            final IRI context2 = vf.createIRI("foo:context");

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
            f.storeStatement(new RyaStatement(new RyaIRI("foo:subj1"), new RyaIRI(RDFS.LABEL.toString()), new RyaType("invalid")));
            f.storeStatement(new RyaStatement(new RyaIRI("foo:subj2"), new RyaIRI(RDFS.COMMENT.toString()), new RyaType("invalid")));

            final RyaIRI pred1 = new RyaIRI("pred:1");
            final RyaIRI pred2 = new RyaIRI("pred:2");

            // These should be stored because they are in the predicate list
            final RyaStatement s3 = new RyaStatement(new RyaIRI("foo:subj3"), pred1, new RyaType("valid"));
            final RyaStatement s4 = new RyaStatement(new RyaIRI("foo:subj4"), pred2, new RyaType("valid"));
            f.storeStatement(s3);
            f.storeStatement(s4);

            // This should not be stored because the object is not a literal
            f.storeStatement(new RyaStatement(new RyaIRI("foo:subj5"), pred1, new RyaIRI("in:validURI")));

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

            final ValueFactory vf = SimpleValueFactory.getInstance();
            final IRI subject = vf.createIRI("foo:subj");
            final IRI predicate = vf.createIRI(RDFS.COMMENT.toString());
            final Value object = vf.createLiteral("this is a new hat");
            final IRI context = vf.createIRI("foo:context");

            final Statement statement = vf.createStatement(subject, predicate, object, context);
            f.storeStatement(RdfToRyaConversions.convertStatement(statement));
            f.flush();

            assertEquals(Sets.newHashSet(statement), getSet(f.queryText("hat", EMPTY_CONSTRAINTS)));
            assertEquals(Sets.newHashSet(statement), getSet(f.queryText("hat", new StatementConstraints().setContext(context))));
            assertEquals(Sets.newHashSet(),
                    getSet(f.queryText("hat", new StatementConstraints().setContext(vf.createIRI("foo:context2")))));
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