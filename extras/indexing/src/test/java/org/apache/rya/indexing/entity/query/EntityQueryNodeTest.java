/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.indexing.entity.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.domain.VarNameUtils;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.indexing.entity.model.Entity;
import org.apache.rya.indexing.entity.model.Property;
import org.apache.rya.indexing.entity.model.Type;
import org.apache.rya.indexing.entity.storage.EntityStorage;
import org.apache.rya.indexing.entity.storage.mongo.MongoEntityStorage;
import org.apache.rya.mongodb.MongoTestBase;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.helpers.StatementPatternCollector;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

/**
 * Unit tests the methods of {@link EntityQueryNode}.
 */
public class EntityQueryNodeTest extends MongoTestBase {

    private static final Type PERSON_TYPE =
            new Type(new RyaURI("urn:person"),
                ImmutableSet.<RyaURI>builder()
                    .add(new RyaURI("urn:name"))
                    .add(new RyaURI("urn:age"))
                    .add(new RyaURI("urn:eye"))
                    .build());

    private static final Type EMPLOYEE_TYPE =
            new Type(new RyaURI("urn:employee"),
                ImmutableSet.<RyaURI>builder()
                    .add(new RyaURI("urn:name"))
                    .add(new RyaURI("urn:hoursPerWeek"))
                    .build());

    @Test(expected = IllegalStateException.class)
    public void constructor_differentSubjects() throws Exception {
        // A pattern that has two different subjects.
        final List<StatementPattern> patterns = getSPs(
                "SELECT * WHERE { " +
                    "<urn:SSN:111-11-1111> <" + RDF.TYPE + "> <urn:person> ."+
                    "<urn:SSN:111-11-1111> <urn:age> ?age . " +
                    "<urn:SSN:111-11-1111> <urn:eye> ?eye . " +
                    "<urn:SSN:111-11-1111> <urn:name> ?name . " +
                    "<urn:SSN:222-22-2222> <urn:age> ?age . " +
                    "<urn:SSN:222-22-2222> <urn:eye> ?eye . " +
                    "<urn:SSN:222-22-2222> <urn:name> ?name . " +
                "}");


        // This will fail.
        new EntityQueryNode(PERSON_TYPE, patterns, mock(EntityStorage.class));
    }

    @Test(expected = IllegalStateException.class)
    public void constructor_variablePredicate() throws Exception {
        // A pattern that has a variable for its predicate.
        final List<StatementPattern> patterns = getSPs(
                "SELECT * WHERE { " +
                    "?subject <" + RDF.TYPE + "> <urn:person> ."+
                    "?subject ?variableProperty ?value . " +
                    "?subject <urn:eye> ?eye . " +
                    "?subject <urn:name> ?name . " +
                "}");


        // This will fail.
        new EntityQueryNode(PERSON_TYPE, patterns, mock(EntityStorage.class));
    }

    @Test(expected = IllegalStateException.class)
    public void constructor_predicateNotPartOfType() throws Exception {
        // A pattern that does uses a predicate that is not part of the type.
        final List<StatementPattern> patterns = getSPs(
                "SELECT * WHERE { " +
                    "?subject <" + RDF.TYPE + "> <urn:person> ."+
                    "?subject <urn:age> ?age . " +
                    "?subject <urn:eye> ?eye . " +
                    "?subject <urn:name> ?name . " +
                    "?subject <urn:notPartOfType> ?value . " +
                "}");

        // This will fail.
        new EntityQueryNode(PERSON_TYPE, patterns, mock(EntityStorage.class));
    }

    @Test(expected = IllegalStateException.class)
    public void constructor_typeMissing() throws Exception {
        // A pattern that does uses a predicate that is not part of the type.
        final List<StatementPattern> patterns = getSPs(
                "SELECT * WHERE { " +
                    "?subject <urn:age> ?age . " +
                    "?subject <urn:eye> ?eye . " +
                    "?subject <urn:name> ?name . " +
                "}");

        // This will fail.
        new EntityQueryNode(PERSON_TYPE, patterns, mock(EntityStorage.class));
    }

    @Test(expected = IllegalStateException.class)
    public void constructor_wrongType() throws Exception {
        // A pattern that does uses a predicate that is not part of the type.
        final List<StatementPattern> patterns = getSPs(
                "SELECT * WHERE { " +
                    "?subject <" + RDF.TYPE + "> <urn:person> ."+
                    "?subject <urn:age> ?age . " +
                    "?subject <urn:eye> ?eye . " +
                    "?subject <urn:name> ?name . " +
                "}");

        // This will fail.
        new EntityQueryNode(EMPLOYEE_TYPE, patterns, mock(EntityStorage.class));
    }

    @Test
    public void evaluate_constantSubject() throws Exception {
        final EntityStorage storage = new MongoEntityStorage(super.getMongoClient(), "testDB");
        final ValueFactory vf = SimpleValueFactory.getInstance();
        final RyaURI subject = new RyaURI("urn:SSN:111-11-1111");
        final Entity entity = Entity.builder()
            .setSubject(subject)
            .setExplicitType(PERSON_TYPE.getId())
            .setProperty(PERSON_TYPE.getId(), new Property(new RyaURI("urn:age"), RdfToRyaConversions.convertLiteral(vf.createLiteral(BigInteger.valueOf(20)))))
            .setProperty(PERSON_TYPE.getId(), new Property(new RyaURI("urn:eye"), RdfToRyaConversions.convertLiteral(vf.createLiteral("blue"))))
            .setProperty(PERSON_TYPE.getId(), new Property(new RyaURI("urn:name"), RdfToRyaConversions.convertLiteral(vf.createLiteral("Bob"))))
            .build();

        storage.create(entity);
        // A set of patterns that match a sepecific Entity subject.
        final List<StatementPattern> patterns = getSPs(
                "SELECT * WHERE { " +
                    "<urn:SSN:111-11-1111> <" + RDF.TYPE + "> <urn:person> ."+
                    "<urn:SSN:111-11-1111> <urn:age> ?age . " +
                    "<urn:SSN:111-11-1111> <urn:eye> ?eye . " +
                    "<urn:SSN:111-11-1111> <urn:name> ?name . " +
                "}");

        final EntityQueryNode node = new EntityQueryNode(PERSON_TYPE, patterns, storage);
        final CloseableIteration<BindingSet, QueryEvaluationException> rez = node.evaluate(new MapBindingSet());
        final MapBindingSet expected = new MapBindingSet();
        expected.addBinding("age", vf.createLiteral("20"));
        expected.addBinding("eye", vf.createLiteral("blue"));
        expected.addBinding("name", vf.createLiteral("Bob"));
        while(rez.hasNext()) {
            assertEquals(expected, rez.next());
            break;
        }
    }

    @Test
    public void evaluate_variableSubject() throws Exception {
        final EntityStorage storage = new MongoEntityStorage(super.getMongoClient(), "testDB");
        final ValueFactory vf = SimpleValueFactory.getInstance();
        RyaURI subject = new RyaURI("urn:SSN:111-11-1111");
        final Entity bob = Entity.builder()
                .setSubject(subject)
                .setExplicitType(PERSON_TYPE.getId())
                .setProperty(PERSON_TYPE.getId(), new Property(new RyaURI("urn:age"), RdfToRyaConversions.convertLiteral(vf.createLiteral(BigInteger.valueOf(20)))))
                .setProperty(PERSON_TYPE.getId(), new Property(new RyaURI("urn:eye"), RdfToRyaConversions.convertLiteral(vf.createLiteral("blue"))))
                .setProperty(PERSON_TYPE.getId(), new Property(new RyaURI("urn:name"), RdfToRyaConversions.convertLiteral(vf.createLiteral("Bob"))))
                .build();

        subject = new RyaURI("urn:SSN:222-22-2222");
        final Entity fred = Entity.builder()
                .setSubject(subject)
                .setExplicitType(PERSON_TYPE.getId())
                .setProperty(PERSON_TYPE.getId(), new Property(new RyaURI("urn:age"), RdfToRyaConversions.convertLiteral(vf.createLiteral(BigInteger.valueOf(25)))))
                .setProperty(PERSON_TYPE.getId(), new Property(new RyaURI("urn:eye"), RdfToRyaConversions.convertLiteral(vf.createLiteral("brown"))))
                .setProperty(PERSON_TYPE.getId(), new Property(new RyaURI("urn:name"), RdfToRyaConversions.convertLiteral(vf.createLiteral("Fred"))))
                .build();

        storage.create(bob);
        storage.create(fred);
        // A set of patterns that match a sepecific Entity subject.
        final List<StatementPattern> patterns = getSPs(
                "SELECT * WHERE { " +
                    "?ssn <" + RDF.TYPE + "> <urn:person> ."+
                    "?ssn <urn:age> ?age . " +
                    "?ssn <urn:eye> ?eye . " +
                    "?ssn <urn:name> ?name . " +
                "}");

        final EntityQueryNode node = new EntityQueryNode(PERSON_TYPE, patterns, storage);
        final CloseableIteration<BindingSet, QueryEvaluationException> rez = node.evaluate(new MapBindingSet());
        final List<BindingSet> expectedBindings = new ArrayList<>();
        final MapBindingSet expectedBob = new MapBindingSet();
        expectedBob.addBinding("age", vf.createLiteral("20"));
        expectedBob.addBinding("eye", vf.createLiteral("blue"));
        expectedBob.addBinding("name", vf.createLiteral("Bob"));

        final MapBindingSet expectedFred = new MapBindingSet();
        expectedFred.addBinding("age", vf.createLiteral("25"));
        expectedFred.addBinding("eye", vf.createLiteral("brown"));
        expectedFred.addBinding("name", vf.createLiteral("Fred"));
        expectedBindings.add(expectedBob);
        expectedBindings.add(expectedFred);
        while(rez.hasNext()) {
            final BindingSet bs = rez.next();
            assertTrue(expectedBindings.contains(bs));
        }
    }

    @Test
    public void evaluate_constantObject() throws Exception {
        final EntityStorage storage = new MongoEntityStorage(super.getMongoClient(), "testDB");
        final ValueFactory vf = SimpleValueFactory.getInstance();
        final RyaURI subject = new RyaURI("urn:SSN:111-11-1111");
        final Entity entity = Entity.builder()
            .setSubject(subject)
            .setExplicitType(PERSON_TYPE.getId())
            .setProperty(PERSON_TYPE.getId(), new Property(new RyaURI("urn:age"), RdfToRyaConversions.convertLiteral(vf.createLiteral(BigInteger.valueOf(20)))))
            .setProperty(PERSON_TYPE.getId(), new Property(new RyaURI("urn:eye"), RdfToRyaConversions.convertLiteral(vf.createLiteral("blue"))))
            .setProperty(PERSON_TYPE.getId(), new Property(new RyaURI("urn:name"), RdfToRyaConversions.convertLiteral(vf.createLiteral("Bob"))))
            .build();

        storage.create(entity);
        // A set of patterns that match a specific Entity subject.
        final List<StatementPattern> patterns = getSPs(
                "SELECT * WHERE { " +
                    "<urn:SSN:111-11-1111> <" + RDF.TYPE + "> <urn:person> ."+
                    "<urn:SSN:111-11-1111> <urn:age> ?age . " +
                    "<urn:SSN:111-11-1111> <urn:eye> \"blue\" . " +
                    "<urn:SSN:111-11-1111> <urn:name> ?name . " +
                "}");

        final EntityQueryNode node = new EntityQueryNode(PERSON_TYPE, patterns, storage);
        final CloseableIteration<BindingSet, QueryEvaluationException> rez = node.evaluate(new MapBindingSet());
        final MapBindingSet expected = new MapBindingSet();
        expected.addBinding("age", vf.createLiteral("20"));
        expected.addBinding(VarNameUtils.createUniqueConstVarName("blue"), vf.createLiteral("blue"));
        expected.addBinding("name", vf.createLiteral("Bob"));
        while(rez.hasNext()) {
            assertEquals(expected, rez.next());
            break;
        }
    }

    /**
     * TODO doc
     *
     * @param sparql
     * @return
     * @throws MalformedQueryException
     */
    private static List<StatementPattern> getSPs(final String sparql) throws MalformedQueryException {
        final StatementPatternCollector spCollector = new StatementPatternCollector();
        new SPARQLParser().parseQuery(sparql, null).getTupleExpr().visit(spCollector);
        return spCollector.getStatementPatterns();
    }
}