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

import static org.mockito.Mockito.mock;

import java.util.List;

import org.apache.rya.indexing.entity.model.Type;
import org.apache.rya.indexing.entity.query.EntityQueryNode;
import org.apache.rya.indexing.entity.storage.EntityStorage;
import org.junit.Test;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;
import org.openrdf.query.parser.sparql.SPARQLParser;

import com.google.common.collect.ImmutableSet;

import mvm.rya.api.domain.RyaURI;

/**
 * Unit tests the methods of {@link EntityQueryNode}.
 */
public class EntityQueryNodeTest {

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

    // Happy path test.

    // TODO test for all of the types of preconditions
    //      test when a binding set can join
    //      test when a binding set does not join
    //      test when there are constants that are part of the query
    //      test when there are variables that are part of the query.

    @Test
    public void evaluate_constantSubject() throws Exception {
        // A set of patterns that match a sepecific Entity subject.
        final List<StatementPattern> patterns = getSPs(
                "SELECT * WHERE { " +
                    "<urn:SSN:111-11-1111> <" + RDF.TYPE + "> <urn:person> ."+
                    "<urn:SSN:111-11-1111> <urn:age> ?age . " +
                    "<urn:SSN:111-11-1111> <urn:eye> ?eye . " +
                    "<urn:SSN:111-11-1111> <urn:name> ?name . " +
                "}");

        new EntityQueryNode(PERSON_TYPE, patterns, mock(EntityStorage.class));


        // TODO implement
    }

    @Test
    public void evaluate_variableSubject() throws Exception {
        // A set of patterns that matches a variable Entity subject.
        final List<StatementPattern> patterns = getSPs(
                "SELECT * WHERE { " +
                    "?subject <" + RDF.TYPE + "> <urn:person> ."+
                    "?subject <urn:age> ?age . " +
                    "?subject <urn:eye> ?eye . " +
                    "?subject <urn:name> ?name . " +
                "}");

        new EntityQueryNode(PERSON_TYPE, patterns, mock(EntityStorage.class));


        // TODO implement
    }



    /**
     * TODO doc
     *
     * @param sparql
     * @return
     * @throws MalformedQueryException
     */
    private static List<StatementPattern> getSPs(String sparql) throws MalformedQueryException {
        final StatementPatternCollector spCollector = new StatementPatternCollector();
        new SPARQLParser().parseQuery(sparql, null).getTupleExpr().visit(spCollector);
        return spCollector.getStatementPatterns();
    }
}