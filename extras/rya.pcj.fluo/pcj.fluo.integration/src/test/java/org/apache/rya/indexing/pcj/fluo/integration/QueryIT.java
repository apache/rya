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
package org.apache.rya.indexing.pcj.fluo.integration;

import static java.util.Objects.requireNonNull;
import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import javax.xml.datatype.DatatypeFactory;

import org.apache.accumulo.core.client.Connector;
import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.core.client.FluoClientImpl;
import org.apache.rya.api.client.CreatePCJ.ExportStrategy;
import org.apache.rya.api.client.RyaClient;
import org.apache.rya.api.client.accumulo.AccumuloRyaClientFactory;
import org.apache.rya.api.functions.DateTimeWithinPeriod;
import org.apache.rya.api.functions.OWLTime;
import org.apache.rya.api.utils.CloseableIterator;
import org.apache.rya.indexing.pcj.fluo.api.CreateFluoPcj;
import org.apache.rya.indexing.pcj.fluo.app.query.UnsupportedQueryException;
import org.apache.rya.indexing.pcj.storage.PeriodicQueryResultStorage;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPcjStorage;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPeriodicQueryResultStorage;
import org.apache.rya.pcj.fluo.test.base.RyaExportITBase;
import org.junit.Test;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.datatypes.XMLDatatypeUtil;
import org.openrdf.model.impl.BooleanLiteralImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.FN;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.BindingSet;
import org.openrdf.query.algebra.evaluation.ValueExprEvaluationException;
import org.openrdf.query.algebra.evaluation.function.Function;
import org.openrdf.query.algebra.evaluation.function.FunctionRegistry;
import org.openrdf.query.impl.MapBindingSet;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepositoryConnection;

import com.google.common.collect.Sets;

/**
 * Performs integration tests over the Fluo application geared towards various query structures.
 */
public class QueryIT extends RyaExportITBase {

    @Test
    public void optionalStatements() throws Exception {
        // A query that has optional statement patterns. This query is looking for all
        // people who have Law degrees and any BAR exams they have passed (though they
        // do not have to have passed any).
        final String sparql = "SELECT ?person ?exam " + "WHERE {" + "?person <http://hasDegreeIn> <http://Law> . "
                + "OPTIONAL {?person <http://passedExam> ?exam } . " + "}";

        // Create the Statements that will be loaded into Rya.
        final ValueFactory vf = new ValueFactoryImpl();
        final Collection<Statement> statements = Sets.newHashSet(
                vf.createStatement(vf.createURI("http://Alice"), vf.createURI("http://hasDegreeIn"),
                        vf.createURI("http://Computer Science")),
                vf.createStatement(vf.createURI("http://Alice"), vf.createURI("http://passedExam"),
                        vf.createURI("http://Certified Ethical Hacker")),
                vf.createStatement(vf.createURI("http://Bob"), vf.createURI("http://hasDegreeIn"), vf.createURI("http://Law")),
                vf.createStatement(vf.createURI("http://Bob"), vf.createURI("http://passedExam"), vf.createURI("http://MBE")),
                vf.createStatement(vf.createURI("http://Bob"), vf.createURI("http://passedExam"), vf.createURI("http://BAR-Kansas")),
                vf.createStatement(vf.createURI("http://Charlie"), vf.createURI("http://hasDegreeIn"), vf.createURI("http://Law")));

        // Create the expected results of the SPARQL query once the PCJ has been computed.
        final Set<BindingSet> expectedResults = new HashSet<>();

        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("person", vf.createURI("http://Bob"));
        bs.addBinding("exam", vf.createURI("http://MBE"));
        expectedResults.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("person", vf.createURI("http://Bob"));
        bs.addBinding("exam", vf.createURI("http://BAR-Kansas"));
        expectedResults.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("person", vf.createURI("http://Charlie"));
        expectedResults.add(bs);

        // Verify the end results of the query match the expected results.
        runTest(sparql, statements, expectedResults, ExportStrategy.RYA);
    }

    /**
     * Tests when there are a bunch of variables across a bunch of joins.
     */
    @Test
    public void complexQuery() throws Exception {
        // A query that find people who live in the USA, have been recruited by Geek Squad,
        // and are skilled with computers. The resulting binding set includes everybody who
        // was involved in the recruitment process.
        final String sparql = "SELECT ?recruiter ?candidate ?leader " + "{ " + "?recruiter <http://recruiterFor> <http://GeekSquad>. "
                + "?recruiter <http://talksTo> ?candidate. " + "?candidate <http://skilledWith> <http://Computers>. " + "?candidate <http://livesIn> \"USA\". "
                + "?candidate <http://talksTo> ?leader." + "?leader <http://leaderOf> <http://GeekSquad>. }";

        // Create the Statements that will be loaded into Rya.
        final ValueFactory vf = new ValueFactoryImpl();
        final Collection<Statement> statements = Sets.newHashSet(
                // Leaders
                vf.createStatement(vf.createURI("http://Alice"), vf.createURI("http://leaderOf"), vf.createURI("http://GeekSquad")),
                vf.createStatement(vf.createURI("http://Bob"), vf.createURI("http://leaderOf"), vf.createURI("http://GeekSquad")),

        // Recruiters
                vf.createStatement(vf.createURI("http://Charlie"), vf.createURI("http://recruiterFor"), vf.createURI("http://GeekSquad")),
                vf.createStatement(vf.createURI("http://David"), vf.createURI("http://recruiterFor"), vf.createURI("http://GeekSquad")),

        // Candidates
                vf.createStatement(vf.createURI("http://Eve"), vf.createURI("http://skilledWith"), vf.createURI("http://Computers")),
                vf.createStatement(vf.createURI("http://Eve"), vf.createURI("http://livesIn"), vf.createLiteral("USA")),
                vf.createStatement(vf.createURI("http://Frank"), vf.createURI("http://skilledWith"), vf.createURI("http://Computers")),
                vf.createStatement(vf.createURI("http://Frank"), vf.createURI("http://livesIn"), vf.createLiteral("USA")),
                vf.createStatement(vf.createURI("http://George"), vf.createURI("http://skilledWith"), vf.createURI("http://Computers")),
                vf.createStatement(vf.createURI("http://George"), vf.createURI("http://livesIn"), vf.createLiteral("Germany")),
                vf.createStatement(vf.createURI("http://Harry"), vf.createURI("http://skilledWith"), vf.createURI("http://Negotiating")),
                vf.createStatement(vf.createURI("http://Harry"), vf.createURI("http://livesIn"), vf.createLiteral("USA")),
                vf.createStatement(vf.createURI("http://Ivan"), vf.createURI("http://skilledWith"), vf.createURI("http://Computers")),
                vf.createStatement(vf.createURI("http://Ivan"), vf.createURI("http://livesIn"), vf.createLiteral("USA")),

        // Candidates the recruiters talk to.
                vf.createStatement(vf.createURI("http://Charlie"), vf.createURI("http://talksTo"), vf.createURI("http://Eve")),
                vf.createStatement(vf.createURI("http://Charlie"), vf.createURI("http://talksTo"), vf.createURI("http://George")),
                vf.createStatement(vf.createURI("http://Charlie"), vf.createURI("http://talksTo"), vf.createURI("http://Harry")),
                vf.createStatement(vf.createURI("http://David"), vf.createURI("http://talksTo"), vf.createURI("http://Eve")),
                vf.createStatement(vf.createURI("http://David"), vf.createURI("http://talksTo"), vf.createURI("http://Frank")),
                vf.createStatement(vf.createURI("http://David"), vf.createURI("http://talksTo"), vf.createURI("http://Ivan")),

        // Recruits that talk to leaders.
                vf.createStatement(vf.createURI("http://Eve"), vf.createURI("http://talksTo"), vf.createURI("http://Alice")),
                vf.createStatement(vf.createURI("http://George"), vf.createURI("http://talksTo"), vf.createURI("http://Alice")),
                vf.createStatement(vf.createURI("http://Harry"), vf.createURI("http://talksTo"), vf.createURI("http://Bob")),
                vf.createStatement(vf.createURI("http://Ivan"), vf.createURI("http://talksTo"), vf.createURI("http://Bob")));

        // Create the expected results of the SPARQL query once the PCJ has been computed.
        final Set<BindingSet> expectedResults = new HashSet<>();

        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("recruiter", vf.createURI("http://Charlie"));
        bs.addBinding("candidate", vf.createURI("http://Eve"));
        bs.addBinding("leader", vf.createURI("http://Alice"));
        expectedResults.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("recruiter", vf.createURI("http://David"));
        bs.addBinding("candidate", vf.createURI("http://Eve"));
        bs.addBinding("leader", vf.createURI("http://Alice"));
        expectedResults.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("recruiter", vf.createURI("http://David"));
        bs.addBinding("candidate", vf.createURI("http://Ivan"));
        bs.addBinding("leader", vf.createURI("http://Bob"));
        expectedResults.add(bs);

        // Verify the end results of the query match the expected results.
        runTest(sparql, statements, expectedResults, ExportStrategy.RYA);
    }

    @Test
    public void withURIFilters() throws Exception {
        final String sparql = "SELECT ?customer ?worker ?city " + "{ " + "FILTER(?customer = <http://Alice>) "
                + "FILTER(?city = <http://London>) " + "?customer <http://talksTo> ?worker. " + "?worker <http://livesIn> ?city. "
                + "?worker <http://worksAt> <http://Chipotle>. " + "}";

        // Create the Statements that will be loaded into Rya.
        final ValueFactory vf = new ValueFactoryImpl();
        final Collection<Statement> statements = Sets.newHashSet(
                vf.createStatement(vf.createURI("http://Alice"), vf.createURI("http://talksTo"), vf.createURI("http://Bob")),
                vf.createStatement(vf.createURI("http://Bob"), vf.createURI("http://livesIn"), vf.createURI("http://London")),
                vf.createStatement(vf.createURI("http://Bob"), vf.createURI("http://worksAt"), vf.createURI("http://Chipotle")),

        vf.createStatement(vf.createURI("http://Alice"), vf.createURI("http://talksTo"), vf.createURI("http://Charlie")),
                vf.createStatement(vf.createURI("http://Charlie"), vf.createURI("http://livesIn"), vf.createURI("http://London")),
                vf.createStatement(vf.createURI("http://Charlie"), vf.createURI("http://worksAt"), vf.createURI("http://Chipotle")),

        vf.createStatement(vf.createURI("http://Alice"), vf.createURI("http://talksTo"), vf.createURI("http://David")),
                vf.createStatement(vf.createURI("http://David"), vf.createURI("http://livesIn"), vf.createURI("http://London")),
                vf.createStatement(vf.createURI("http://David"), vf.createURI("http://worksAt"), vf.createURI("http://Chipotle")),

        vf.createStatement(vf.createURI("http://Alice"), vf.createURI("http://talksTo"), vf.createURI("http://Eve")),
                vf.createStatement(vf.createURI("http://Eve"), vf.createURI("http://livesIn"), vf.createURI("http://Leeds")),
                vf.createStatement(vf.createURI("http://Eve"), vf.createURI("http://worksAt"), vf.createURI("http://Chipotle")),

        vf.createStatement(vf.createURI("http://Frank"), vf.createURI("http://talksTo"), vf.createURI("http://Alice")),
                vf.createStatement(vf.createURI("http://Frank"), vf.createURI("http://livesIn"), vf.createURI("http://London")),
                vf.createStatement(vf.createURI("http://Frank"), vf.createURI("http://worksAt"), vf.createURI("http://Chipotle")));

        // Create the expected results of the SPARQL query once the PCJ has been computed.
        final Set<BindingSet> expectedResults = new HashSet<>();

        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("customer", vf.createURI("http://Alice"));
        bs.addBinding("worker", vf.createURI("http://Bob"));
        bs.addBinding("city", vf.createURI("http://London"));
        expectedResults.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("customer", vf.createURI("http://Alice"));
        bs.addBinding("worker", vf.createURI("http://Charlie"));
        bs.addBinding("city", vf.createURI("http://London"));
        expectedResults.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("customer", vf.createURI("http://Alice"));
        bs.addBinding("worker", vf.createURI("http://David"));
        bs.addBinding("city", vf.createURI("http://London"));
        expectedResults.add(bs);

        // Verify the end results of the query match the expected results.
        runTest(sparql, statements, expectedResults, ExportStrategy.RYA);
    }

    @Test
    public void withNumericFilters() throws Exception {
        final String sparql = "SELECT ?name ?age " + "{" + "FILTER(?age < 30) ." + "?name <http://hasAge> ?age."
                + "?name <http://playsSport> \"Soccer\" " + "}";

        // Create the Statements that will be loaded into Rya.
        final ValueFactory vf = new ValueFactoryImpl();
        final Collection<Statement> statements = Sets.newHashSet(
                vf.createStatement(vf.createURI("http://Alice"), vf.createURI("http://hasAge"), vf.createLiteral(18)),
                vf.createStatement(vf.createURI("http://Bob"), vf.createURI("http://hasAge"), vf.createLiteral(30)),
                vf.createStatement(vf.createURI("http://Charlie"), vf.createURI("http://hasAge"), vf.createLiteral(14)),
                vf.createStatement(vf.createURI("http://David"), vf.createURI("http://hasAge"), vf.createLiteral(16)),
                vf.createStatement(vf.createURI("http://Eve"), vf.createURI("http://hasAge"), vf.createLiteral(35)),

        vf.createStatement(vf.createURI("http://Alice"), vf.createURI("http://playsSport"), vf.createLiteral("Soccer")),
                vf.createStatement(vf.createURI("http://Bob"), vf.createURI("http://playsSport"), vf.createLiteral("Soccer")),
                vf.createStatement(vf.createURI("http://Charlie"), vf.createURI("http://playsSport"), vf.createLiteral("Basketball")),
                vf.createStatement(vf.createURI("http://Charlie"), vf.createURI("http://playsSport"), vf.createLiteral("Soccer")),
                vf.createStatement(vf.createURI("http://David"), vf.createURI("http://playsSport"), vf.createLiteral("Basketball")));

        // Create the expected results of the SPARQL query once the PCJ has been computed.
        final Set<BindingSet> expectedResults = new HashSet<>();

        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("name", vf.createURI("http://Alice"));
        bs.addBinding("age", vf.createLiteral("18", XMLSchema.INTEGER));
        expectedResults.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("name", vf.createURI("http://Charlie"));
        bs.addBinding("age", vf.createLiteral("14", XMLSchema.INTEGER));
        expectedResults.add(bs);

        // Verify the end results of the query match the expected results.
        runTest(sparql, statements, expectedResults, ExportStrategy.RYA);
    }

    @Test
    public void withCustomFilters() throws Exception {
        final String sparql = "prefix ryafunc: <tag:rya.apache.org,2017:function#> " + "SELECT ?name ?age " + "{ "
                + "FILTER( ryafunc:isTeen(?age) ) . " + "?name <http://hasAge> ?age . " + "?name <http://playsSport> \"Soccer\" . " + "}";

        // Register a custom Filter.
        final Function fooFunction = new Function() {
            @Override
            public String getURI() {
                return "tag:rya.apache.org,2017:function#isTeen";
            }

            final static int TEEN_THRESHOLD = 20;

            @Override
            public Value evaluate(final ValueFactory valueFactory, final Value... args) throws ValueExprEvaluationException {
                if (args.length != 1) {
                    throw new ValueExprEvaluationException("isTeen() requires exactly 1 argument, got " + args.length);
                }

                if (args[0] instanceof Literal) {
                    final Literal literal = (Literal) args[0];
                    final URI datatype = literal.getDatatype();

                    // ABS function accepts only numeric literals
                    if (datatype != null && XMLDatatypeUtil.isNumericDatatype(datatype)) {
                        if (XMLDatatypeUtil.isDecimalDatatype(datatype)) {
                            final BigDecimal bigValue = literal.decimalValue();
                            return BooleanLiteralImpl.valueOf(bigValue.compareTo(new BigDecimal(TEEN_THRESHOLD)) < 0);
                        } else if (XMLDatatypeUtil.isFloatingPointDatatype(datatype)) {
                            final double doubleValue = literal.doubleValue();
                            return BooleanLiteralImpl.valueOf(doubleValue < TEEN_THRESHOLD);
                        } else {
                            throw new ValueExprEvaluationException(
                                    "unexpected datatype (expect decimal/int or floating) for function operand: " + args[0]);
                        }
                    } else {
                        throw new ValueExprEvaluationException(
                                "unexpected input value (expect non-null and numeric) for function: " + args[0]);
                    }
                } else {
                    throw new ValueExprEvaluationException("unexpected input value (expect literal) for function: " + args[0]);
                }
            }
        };

        // Add our new function to the registry
        FunctionRegistry.getInstance().add(fooFunction);

        // Create the Statements that will be loaded into Rya.
        final ValueFactory vf = new ValueFactoryImpl();
        final Collection<Statement> statements = Sets.newHashSet(
                vf.createStatement(vf.createURI("http://Alice"), vf.createURI("http://hasAge"), vf.createLiteral(18)),
                vf.createStatement(vf.createURI("http://Bob"), vf.createURI("http://hasAge"), vf.createLiteral(30)),
                vf.createStatement(vf.createURI("http://Charlie"), vf.createURI("http://hasAge"), vf.createLiteral(14)),
                vf.createStatement(vf.createURI("http://David"), vf.createURI("http://hasAge"), vf.createLiteral(16)),
                vf.createStatement(vf.createURI("http://Eve"), vf.createURI("http://hasAge"), vf.createLiteral(35)),

        vf.createStatement(vf.createURI("http://Alice"), vf.createURI("http://playsSport"), vf.createLiteral("Soccer")),
                vf.createStatement(vf.createURI("http://Bob"), vf.createURI("http://playsSport"), vf.createLiteral("Soccer")),
                vf.createStatement(vf.createURI("http://Charlie"), vf.createURI("http://playsSport"), vf.createLiteral("Basketball")),
                vf.createStatement(vf.createURI("http://Charlie"), vf.createURI("http://playsSport"), vf.createLiteral("Soccer")),
                vf.createStatement(vf.createURI("http://David"), vf.createURI("http://playsSport"), vf.createLiteral("Basketball")));

        // Create the expected results of the SPARQL query once the PCJ has been computed.
        final Set<BindingSet> expectedResults = new HashSet<>();

        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("name", vf.createURI("http://Alice"));
        bs.addBinding("age", vf.createLiteral("18", XMLSchema.INTEGER));
        expectedResults.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("name", vf.createURI("http://Charlie"));
        bs.addBinding("age", vf.createLiteral("14", XMLSchema.INTEGER));
        expectedResults.add(bs);

        // Verify the end results of the query match the expected results.
        runTest(sparql, statements, expectedResults, ExportStrategy.RYA);
    }

    @Test
    public void withTemporal() throws Exception {
        // A query that finds all stored data after 3 seconds.
        final String dtPredUri = "http://www.w3.org/2006/time#inXSDDateTime";
        final String dtPred = "<" + dtPredUri + ">";

        final String sparql = "PREFIX time: <http://www.w3.org/2006/time#> " + "PREFIX xml: <http://www.w3.org/2001/XMLSchema#> "
                + "PREFIX tempo: <tag:rya-rdf.org,2015:temporal#> " + "SELECT ?event ?time " + "WHERE { " + "?event " + dtPred + " ?time . "
                + "FILTER(?time > '2001-01-01T01:01:03-08:00'^^xml:dateTime) " + "}";

        // Create the Statements that will be loaded into Rya.
        final ValueFactory vf = new ValueFactoryImpl();
        final DatatypeFactory dtf = DatatypeFactory.newInstance();
        final Collection<Statement> statements = Sets.newHashSet(
                vf.createStatement(vf.createURI("http://eventz"), vf.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
                        vf.createURI("http://www.w3.org/2006/time#Instant")),
                vf.createStatement(vf.createURI("http://eventz"), vf.createURI(dtPredUri),
                        vf.createLiteral(dtf.newXMLGregorianCalendar("2001-01-01T01:01:01-08:00"))), // 1 second
                vf.createStatement(vf.createURI("http://eventz"), vf.createURI(dtPredUri),
                        vf.createLiteral(dtf.newXMLGregorianCalendar("2001-01-01T04:01:02.000-05:00"))), // 2 second
                vf.createStatement(vf.createURI("http://eventz"), vf.createURI(dtPredUri),
                        vf.createLiteral(dtf.newXMLGregorianCalendar("2001-01-01T01:01:03-08:00"))), // 3 seconds
                vf.createStatement(vf.createURI("http://eventz"), vf.createURI(dtPredUri),
                        vf.createLiteral(dtf.newXMLGregorianCalendar("2001-01-01T01:01:04-08:00"))), // 4 seconds
                vf.createStatement(vf.createURI("http://eventz"), vf.createURI(dtPredUri),
                        vf.createLiteral(dtf.newXMLGregorianCalendar("2001-01-01T09:01:05Z"))), // 5 seconds
                vf.createStatement(vf.createURI("http://eventz"), vf.createURI(dtPredUri),
                        vf.createLiteral(dtf.newXMLGregorianCalendar("2006-01-01T05:00:00.000Z"))),
                vf.createStatement(vf.createURI("http://eventz"), vf.createURI(dtPredUri),
                        vf.createLiteral(dtf.newXMLGregorianCalendar("2007-01-01T05:00:00.000Z"))),
                vf.createStatement(vf.createURI("http://eventz"), vf.createURI(dtPredUri),
                        vf.createLiteral(dtf.newXMLGregorianCalendar("2008-01-01T05:00:00.000Z"))));

        // Create the expected results of the SPARQL query once the PCJ has been computed.
        final Set<BindingSet> expectedResults = new HashSet<>();

        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("event", vf.createURI("http://eventz"));
        bs.addBinding("time", vf.createLiteral(dtf.newXMLGregorianCalendar("2001-01-01T09:01:04.000Z")));
        expectedResults.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("event", vf.createURI("http://eventz"));
        bs.addBinding("time", vf.createLiteral(dtf.newXMLGregorianCalendar("2001-01-01T09:01:05.000Z")));
        expectedResults.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("event", vf.createURI("http://eventz"));
        bs.addBinding("time", vf.createLiteral(dtf.newXMLGregorianCalendar("2006-01-01T05:00:00.000Z")));
        expectedResults.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("event", vf.createURI("http://eventz"));
        bs.addBinding("time", vf.createLiteral(dtf.newXMLGregorianCalendar("2007-01-01T05:00:00.000Z")));
        expectedResults.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("event", vf.createURI("http://eventz"));
        bs.addBinding("time", vf.createLiteral(dtf.newXMLGregorianCalendar("2008-01-01T05:00:00.000Z")));
        expectedResults.add(bs);

        // Verify the end results of the query match the expected results.
        runTest(sparql, statements, expectedResults, ExportStrategy.RYA);
    }


    @Test
    public void dateTimeWithin() throws Exception {

        final ValueFactory vf = new ValueFactoryImpl();
        final DatatypeFactory dtf = DatatypeFactory.newInstance();
        FunctionRegistry.getInstance().add(new DateTimeWithinPeriod());

        final String sparql = "PREFIX fn: <" + FN.NAMESPACE +">"
                + "SELECT ?event ?startTime ?endTime WHERE { ?event <uri:startTime> ?startTime; <uri:endTime> ?endTime. "
                + "FILTER(fn:dateTimeWithin(?startTime, ?endTime, 2,<" + OWLTime.HOURS_URI + "> ))}";

        final ZonedDateTime zTime = ZonedDateTime.now();
        final String time = zTime.format(DateTimeFormatter.ISO_INSTANT);

        final ZonedDateTime zTime1 = zTime.minusHours(1);
        final String time1 = zTime1.format(DateTimeFormatter.ISO_INSTANT);

        final ZonedDateTime zTime2 = zTime.minusHours(2);
        final String time2 = zTime2.format(DateTimeFormatter.ISO_INSTANT);

        final Literal lit = vf.createLiteral(dtf.newXMLGregorianCalendar(time));
        final Literal lit1 = vf.createLiteral(dtf.newXMLGregorianCalendar(time1));
        final Literal lit2 = vf.createLiteral(dtf.newXMLGregorianCalendar(time2));

        // Create the Statements that will be loaded into Rya.
        final Collection<Statement> statements = Sets.newHashSet(
                vf.createStatement(vf.createURI("uri:event1"), vf.createURI("uri:startTime"), lit),
                vf.createStatement(vf.createURI("uri:event1"), vf.createURI("uri:endTime"), lit1),
                vf.createStatement(vf.createURI("uri:event2"), vf.createURI("uri:startTime"), lit),
                vf.createStatement(vf.createURI("uri:event2"), vf.createURI("uri:endTime"), lit2)
               );

        // Create the expected results of the SPARQL query once the PCJ has been computed.
        final Set<BindingSet> expectedResults = new HashSet<>();

        final MapBindingSet bs = new MapBindingSet();
        bs.addBinding("event", vf.createURI("uri:event1"));
        bs.addBinding("startTime", lit);
        bs.addBinding("endTime", lit1);
        expectedResults.add(bs);

        // Verify the end results of the query match the expected results.
        runTest(sparql, statements, expectedResults, ExportStrategy.RYA);
    }

    @Test
    public void dateTimeWithinNow() throws Exception {

        final ValueFactory vf = new ValueFactoryImpl();
        final DatatypeFactory dtf = DatatypeFactory.newInstance();
        FunctionRegistry.getInstance().add(new DateTimeWithinPeriod());

        final String sparql = "PREFIX fn: <" + FN.NAMESPACE +">"
                + "SELECT ?event ?startTime WHERE { ?event <uri:startTime> ?startTime. "
                + "FILTER(fn:dateTimeWithin(?startTime, NOW(), 30, <" + OWLTime.SECONDS_URI + "> ))}";

        final ZonedDateTime zTime = ZonedDateTime.now();
        final String time = zTime.format(DateTimeFormatter.ISO_INSTANT);

        final ZonedDateTime zTime1 = zTime.minusSeconds(30);
        final String time1 = zTime1.format(DateTimeFormatter.ISO_INSTANT);

        final Literal lit = vf.createLiteral(dtf.newXMLGregorianCalendar(time));
        final Literal lit1 = vf.createLiteral(dtf.newXMLGregorianCalendar(time1));

        // Create the Statements that will be loaded into Rya.
        final Collection<Statement> statements = Sets.newHashSet(
                vf.createStatement(vf.createURI("uri:event1"), vf.createURI("uri:startTime"), lit),
                vf.createStatement(vf.createURI("uri:event2"), vf.createURI("uri:startTime"), lit1)
               );

        // Create the expected results of the SPARQL query once the PCJ has been computed.
        final Set<BindingSet> expectedResults = new HashSet<>();

        final MapBindingSet bs = new MapBindingSet();
        bs.addBinding("event", vf.createURI("uri:event1"));
        bs.addBinding("startTime", lit);
        expectedResults.add(bs);

        // Verify the end results of the query match the expected results.
        runTest(sparql, statements, expectedResults, ExportStrategy.RYA);
    }



    @Test
    public void periodicQueryTestWithoutAggregation() throws Exception {
        final String query = "prefix function: <http://org.apache.rya/function#> " // n
                + "prefix time: <http://www.w3.org/2006/time#> " // n
                + "select ?id where {" // n
                + "Filter(function:periodic(?time, 2, .5, time:hours)) " // n
                + "?obs <uri:hasTime> ?time. " // n
                + "?obs <uri:hasId> ?id }"; // n

        // Create the Statements that will be loaded into Rya.
        final ValueFactory vf = new ValueFactoryImpl();
        final DatatypeFactory dtf = DatatypeFactory.newInstance();
        final ZonedDateTime time = ZonedDateTime.now();
        final long currentTime = time.toInstant().toEpochMilli();

        final ZonedDateTime zTime1 = time.minusMinutes(30);
        final String time1 = zTime1.format(DateTimeFormatter.ISO_INSTANT);

        final ZonedDateTime zTime2 = zTime1.minusMinutes(30);
        final String time2 = zTime2.format(DateTimeFormatter.ISO_INSTANT);

        final ZonedDateTime zTime3 = zTime2.minusMinutes(30);
        final String time3 = zTime3.format(DateTimeFormatter.ISO_INSTANT);

        final ZonedDateTime zTime4 = zTime3.minusMinutes(30);
        final String time4 = zTime4.format(DateTimeFormatter.ISO_INSTANT);

        final Collection<Statement> statements = Sets.newHashSet(
                vf.createStatement(vf.createURI("urn:obs_1"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time1))),
                vf.createStatement(vf.createURI("urn:obs_1"), vf.createURI("uri:hasId"), vf.createLiteral("id_1")),
                vf.createStatement(vf.createURI("urn:obs_2"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time2))),
                vf.createStatement(vf.createURI("urn:obs_2"), vf.createURI("uri:hasId"), vf.createLiteral("id_2")),
                vf.createStatement(vf.createURI("urn:obs_3"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time3))),
                vf.createStatement(vf.createURI("urn:obs_3"), vf.createURI("uri:hasId"), vf.createLiteral("id_3")),
                vf.createStatement(vf.createURI("urn:obs_4"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time4))),
                vf.createStatement(vf.createURI("urn:obs_4"), vf.createURI("uri:hasId"), vf.createLiteral("id_4")));

        // Create the expected results of the SPARQL query once the PCJ has been computed.
        final Set<BindingSet> expectedResults = new HashSet<>();

        final long period = 1800000;
        final long binId = currentTime / period * period;

        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("id", vf.createLiteral("id_1", XMLSchema.STRING));
        bs.addBinding("periodicBinId", vf.createLiteral(binId));
        expectedResults.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("id", vf.createLiteral("id_1", XMLSchema.STRING));
        bs.addBinding("periodicBinId", vf.createLiteral(binId + period));
        expectedResults.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("id", vf.createLiteral("id_1", XMLSchema.STRING));
        bs.addBinding("periodicBinId", vf.createLiteral(binId + 2 * period));
        expectedResults.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("id", vf.createLiteral("id_1", XMLSchema.STRING));
        bs.addBinding("periodicBinId", vf.createLiteral(binId + 3 * period));
        expectedResults.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("id", vf.createLiteral("id_2", XMLSchema.STRING));
        bs.addBinding("periodicBinId", vf.createLiteral(binId));
        expectedResults.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("id", vf.createLiteral("id_2", XMLSchema.STRING));
        bs.addBinding("periodicBinId", vf.createLiteral(binId + period));
        expectedResults.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("id", vf.createLiteral("id_2", XMLSchema.STRING));
        bs.addBinding("periodicBinId", vf.createLiteral(binId + 2 * period));
        expectedResults.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("id", vf.createLiteral("id_3", XMLSchema.STRING));
        bs.addBinding("periodicBinId", vf.createLiteral(binId));
        expectedResults.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("id", vf.createLiteral("id_3", XMLSchema.STRING));
        bs.addBinding("periodicBinId", vf.createLiteral(binId + period));
        expectedResults.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("id", vf.createLiteral("id_4", XMLSchema.STRING));
        bs.addBinding("periodicBinId", vf.createLiteral(binId));
        expectedResults.add(bs);

        // Verify the end results of the query match the expected results.
        runTest(query, statements, expectedResults, ExportStrategy.PERIODIC);
    }

    @Test
    public void periodicQueryTestWithAggregation() throws Exception {
        final String query = "prefix function: <http://org.apache.rya/function#> " // n
                + "prefix time: <http://www.w3.org/2006/time#> " // n
                + "select (count(?obs) as ?total) where {" // n
                + "Filter(function:periodic(?time, 2, .5, time:hours)) " // n
                + "?obs <uri:hasTime> ?time. " // n
                + "?obs <uri:hasId> ?id }"; // n

        // Create the Statements that will be loaded into Rya.
        final ValueFactory vf = new ValueFactoryImpl();
        final DatatypeFactory dtf = DatatypeFactory.newInstance();
        final ZonedDateTime time = ZonedDateTime.now();
        final long currentTime = time.toInstant().toEpochMilli();

        final ZonedDateTime zTime1 = time.minusMinutes(30);
        final String time1 = zTime1.format(DateTimeFormatter.ISO_INSTANT);

        final ZonedDateTime zTime2 = zTime1.minusMinutes(30);
        final String time2 = zTime2.format(DateTimeFormatter.ISO_INSTANT);

        final ZonedDateTime zTime3 = zTime2.minusMinutes(30);
        final String time3 = zTime3.format(DateTimeFormatter.ISO_INSTANT);

        final ZonedDateTime zTime4 = zTime3.minusMinutes(30);
        final String time4 = zTime4.format(DateTimeFormatter.ISO_INSTANT);

        final Collection<Statement> statements = Sets.newHashSet(
                vf.createStatement(vf.createURI("urn:obs_1"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time1))),
                vf.createStatement(vf.createURI("urn:obs_1"), vf.createURI("uri:hasId"), vf.createLiteral("id_1")),
                vf.createStatement(vf.createURI("urn:obs_2"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time2))),
                vf.createStatement(vf.createURI("urn:obs_2"), vf.createURI("uri:hasId"), vf.createLiteral("id_2")),
                vf.createStatement(vf.createURI("urn:obs_3"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time3))),
                vf.createStatement(vf.createURI("urn:obs_3"), vf.createURI("uri:hasId"), vf.createLiteral("id_3")),
                vf.createStatement(vf.createURI("urn:obs_4"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time4))),
                vf.createStatement(vf.createURI("urn:obs_4"), vf.createURI("uri:hasId"), vf.createLiteral("id_4")));

        // Create the expected results of the SPARQL query once the PCJ has been computed.
        final Set<BindingSet> expectedResults = new HashSet<>();

        final long period = 1800000;
        final long binId = currentTime / period * period;

        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("total", vf.createLiteral("4", XMLSchema.INTEGER));
        bs.addBinding("periodicBinId", vf.createLiteral(binId));
        expectedResults.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("total", vf.createLiteral("3", XMLSchema.INTEGER));
        bs.addBinding("periodicBinId", vf.createLiteral(binId + period));
        expectedResults.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("total", vf.createLiteral("2", XMLSchema.INTEGER));
        bs.addBinding("periodicBinId", vf.createLiteral(binId + 2 * period));
        expectedResults.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("total", vf.createLiteral("1", XMLSchema.INTEGER));
        bs.addBinding("periodicBinId", vf.createLiteral(binId + 3 * period));
        expectedResults.add(bs);

        // Verify the end results of the query match the expected results.
        runTest(query, statements, expectedResults, ExportStrategy.PERIODIC);
    }

    @Test
    public void periodicQueryTestWithAggregationAndGroupBy() throws Exception {
        final String query = "prefix function: <http://org.apache.rya/function#> " // n
                + "prefix time: <http://www.w3.org/2006/time#> " // n
                + "select ?id (count(?obs) as ?total) where {" // n
                + "Filter(function:periodic(?time, 2, .5, time:hours)) " // n
                + "?obs <uri:hasTime> ?time. " // n
                + "?obs <uri:hasId> ?id } group by ?id"; // n

        // Create the Statements that will be loaded into Rya.
        final ValueFactory vf = new ValueFactoryImpl();
        final DatatypeFactory dtf = DatatypeFactory.newInstance();
        final ZonedDateTime time = ZonedDateTime.now();
        final long currentTime = time.toInstant().toEpochMilli();

        final ZonedDateTime zTime1 = time.minusMinutes(30);
        final String time1 = zTime1.format(DateTimeFormatter.ISO_INSTANT);

        final ZonedDateTime zTime2 = zTime1.minusMinutes(30);
        final String time2 = zTime2.format(DateTimeFormatter.ISO_INSTANT);

        final ZonedDateTime zTime3 = zTime2.minusMinutes(30);
        final String time3 = zTime3.format(DateTimeFormatter.ISO_INSTANT);

        final ZonedDateTime zTime4 = zTime3.minusMinutes(30);
        final String time4 = zTime4.format(DateTimeFormatter.ISO_INSTANT);

        final Collection<Statement> statements = Sets.newHashSet(
                vf.createStatement(vf.createURI("urn:obs_1"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time1))),
                vf.createStatement(vf.createURI("urn:obs_1"), vf.createURI("uri:hasId"), vf.createLiteral("id_1")),
                vf.createStatement(vf.createURI("urn:obs_2"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time2))),
                vf.createStatement(vf.createURI("urn:obs_2"), vf.createURI("uri:hasId"), vf.createLiteral("id_2")),
                vf.createStatement(vf.createURI("urn:obs_3"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time3))),
                vf.createStatement(vf.createURI("urn:obs_3"), vf.createURI("uri:hasId"), vf.createLiteral("id_3")),
                vf.createStatement(vf.createURI("urn:obs_4"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time4))),
                vf.createStatement(vf.createURI("urn:obs_4"), vf.createURI("uri:hasId"), vf.createLiteral("id_4")),
                vf.createStatement(vf.createURI("urn:obs_1"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time4))),
                vf.createStatement(vf.createURI("urn:obs_1"), vf.createURI("uri:hasId"), vf.createLiteral("id_1")),
                vf.createStatement(vf.createURI("urn:obs_2"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time3))),
                vf.createStatement(vf.createURI("urn:obs_2"), vf.createURI("uri:hasId"), vf.createLiteral("id_2")));

        // Create the expected results of the SPARQL query once the PCJ has been computed.
        final Set<BindingSet> expectedResults = new HashSet<>();

        final long period = 1800000;
        final long binId = currentTime / period * period;

        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("total", vf.createLiteral("2", XMLSchema.INTEGER));
        bs.addBinding("id", vf.createLiteral("id_1", XMLSchema.STRING));
        bs.addBinding("periodicBinId", vf.createLiteral(binId));
        expectedResults.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("total", vf.createLiteral("2", XMLSchema.INTEGER));
        bs.addBinding("id", vf.createLiteral("id_2", XMLSchema.STRING));
        bs.addBinding("periodicBinId", vf.createLiteral(binId));
        expectedResults.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("total", vf.createLiteral("1", XMLSchema.INTEGER));
        bs.addBinding("id", vf.createLiteral("id_3", XMLSchema.STRING));
        bs.addBinding("periodicBinId", vf.createLiteral(binId));
        expectedResults.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("total", vf.createLiteral("1", XMLSchema.INTEGER));
        bs.addBinding("id", vf.createLiteral("id_4", XMLSchema.STRING));
        bs.addBinding("periodicBinId", vf.createLiteral(binId));
        expectedResults.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("total", vf.createLiteral("1", XMLSchema.INTEGER));
        bs.addBinding("id", vf.createLiteral("id_1", XMLSchema.STRING));
        bs.addBinding("periodicBinId", vf.createLiteral(binId + period));
        expectedResults.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("total", vf.createLiteral("2", XMLSchema.INTEGER));
        bs.addBinding("id", vf.createLiteral("id_2", XMLSchema.STRING));
        bs.addBinding("periodicBinId", vf.createLiteral(binId + period));
        expectedResults.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("total", vf.createLiteral("1", XMLSchema.INTEGER));
        bs.addBinding("id", vf.createLiteral("id_3", XMLSchema.STRING));
        bs.addBinding("periodicBinId", vf.createLiteral(binId + period));
        expectedResults.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("total", vf.createLiteral("1", XMLSchema.INTEGER));
        bs.addBinding("id", vf.createLiteral("id_1", XMLSchema.STRING));
        bs.addBinding("periodicBinId", vf.createLiteral(binId + 2 * period));
        expectedResults.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("total", vf.createLiteral("1", XMLSchema.INTEGER));
        bs.addBinding("id", vf.createLiteral("id_2", XMLSchema.STRING));
        bs.addBinding("periodicBinId", vf.createLiteral(binId + 2 * period));
        expectedResults.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("total", vf.createLiteral("1", XMLSchema.INTEGER));
        bs.addBinding("id", vf.createLiteral("id_1", XMLSchema.STRING));
        bs.addBinding("periodicBinId", vf.createLiteral(binId + 3 * period));
        expectedResults.add(bs);

        // Verify the end results of the query match the expected results.
        runTest(query, statements, expectedResults, ExportStrategy.PERIODIC);
    }


    @Test
    public void nestedPeriodicQueryTestWithAggregationAndGroupBy() throws Exception {
        final String query = "prefix function: <http://org.apache.rya/function#> " // n
                + "prefix time: <http://www.w3.org/2006/time#> " // n
                + "select ?location ?total "
                + "where { Filter(?total > 1) {"
                + "select ?location (count(?obs) as ?total) where {" // n
                + "Filter(function:periodic(?time, 2, .5, time:hours)) " // n
                + "?obs <uri:hasTime> ?time. " // n
                + "?obs <uri:hasLoc> ?location } group by ?location }}"; // n

        // Create the Statements that will be loaded into Rya.
        final ValueFactory vf = new ValueFactoryImpl();
        final DatatypeFactory dtf = DatatypeFactory.newInstance();
        final ZonedDateTime time = ZonedDateTime.now();
        final long currentTime = time.toInstant().toEpochMilli();

        final ZonedDateTime zTime1 = time.minusMinutes(30);
        final String time1 = zTime1.format(DateTimeFormatter.ISO_INSTANT);

        final ZonedDateTime zTime2 = zTime1.minusMinutes(30);
        final String time2 = zTime2.format(DateTimeFormatter.ISO_INSTANT);

        final ZonedDateTime zTime3 = zTime2.minusMinutes(30);
        final String time3 = zTime3.format(DateTimeFormatter.ISO_INSTANT);

        final ZonedDateTime zTime4 = zTime3.minusMinutes(30);
        final String time4 = zTime4.format(DateTimeFormatter.ISO_INSTANT);

        final Collection<Statement> statements = Sets.newHashSet(
                vf.createStatement(vf.createURI("urn:obs_1"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time1))),
                vf.createStatement(vf.createURI("urn:obs_1"), vf.createURI("uri:hasLoc"), vf.createLiteral("loc_1")),
                vf.createStatement(vf.createURI("urn:obs_2"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time2))),
                vf.createStatement(vf.createURI("urn:obs_2"), vf.createURI("uri:hasLoc"), vf.createLiteral("loc_2")),
                vf.createStatement(vf.createURI("urn:obs_3"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time3))),
                vf.createStatement(vf.createURI("urn:obs_3"), vf.createURI("uri:hasLoc"), vf.createLiteral("loc_3")),
                vf.createStatement(vf.createURI("urn:obs_4"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time4))),
                vf.createStatement(vf.createURI("urn:obs_4"), vf.createURI("uri:hasLoc"), vf.createLiteral("loc_4")),
                vf.createStatement(vf.createURI("urn:obs_5"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time4))),
                vf.createStatement(vf.createURI("urn:obs_5"), vf.createURI("uri:hasLoc"), vf.createLiteral("loc_1")),
                vf.createStatement(vf.createURI("urn:obs_6"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time3))),
                vf.createStatement(vf.createURI("urn:obs_6"), vf.createURI("uri:hasLoc"), vf.createLiteral("loc_2")));

        // Create the expected results of the SPARQL query once the PCJ has been computed.
        final Set<BindingSet> expectedResults = new HashSet<>();

        final long period = 1800000;
        final long binId = currentTime / period * period;

        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("total", vf.createLiteral("2", XMLSchema.INTEGER));
        bs.addBinding("location", vf.createLiteral("loc_1", XMLSchema.STRING));
        bs.addBinding("periodicBinId", vf.createLiteral(binId));
        expectedResults.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("total", vf.createLiteral("2", XMLSchema.INTEGER));
        bs.addBinding("location", vf.createLiteral("loc_2", XMLSchema.STRING));
        bs.addBinding("periodicBinId", vf.createLiteral(binId));
        expectedResults.add(bs);


        bs = new MapBindingSet();
        bs.addBinding("total", vf.createLiteral("2", XMLSchema.INTEGER));
        bs.addBinding("location", vf.createLiteral("loc_2", XMLSchema.STRING));
        bs.addBinding("periodicBinId", vf.createLiteral(binId + period));
        expectedResults.add(bs);

        // Verify the end results of the query match the expected results.
        runTest(query, statements, expectedResults, ExportStrategy.PERIODIC);
    }

    @Test
    public void nestedJoinPeriodicQueryWithAggregationAndGroupBy() throws Exception {
        final String query = "prefix function: <http://org.apache.rya/function#> " // n
                + "prefix time: <http://www.w3.org/2006/time#> " // n
                + "select ?location ?total ?population "
                + "where { Filter(?total > 1)"
                + "?location <uri:hasPopulation> ?population . {"
                + "select ?location (count(?obs) as ?total) where {" // n
                + "Filter(function:periodic(?time, 2, .5, time:hours)) " // n
                + "?obs <uri:hasTime> ?time. " // n
                + "?obs <uri:hasLoc> ?location } group by ?location }}"; // n

        // Create the Statements that will be loaded into Rya.
        final ValueFactory vf = new ValueFactoryImpl();
        final DatatypeFactory dtf = DatatypeFactory.newInstance();
        final ZonedDateTime time = ZonedDateTime.now();
        final long currentTime = time.toInstant().toEpochMilli();

        final ZonedDateTime zTime1 = time.minusMinutes(30);
        final String time1 = zTime1.format(DateTimeFormatter.ISO_INSTANT);

        final ZonedDateTime zTime2 = zTime1.minusMinutes(30);
        final String time2 = zTime2.format(DateTimeFormatter.ISO_INSTANT);

        final ZonedDateTime zTime3 = zTime2.minusMinutes(30);
        final String time3 = zTime3.format(DateTimeFormatter.ISO_INSTANT);

        final ZonedDateTime zTime4 = zTime3.minusMinutes(30);
        final String time4 = zTime4.format(DateTimeFormatter.ISO_INSTANT);

        final Collection<Statement> statements = Sets.newHashSet(
                vf.createStatement(vf.createURI("urn:obs_1"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time1))),
                vf.createStatement(vf.createURI("urn:obs_1"), vf.createURI("uri:hasLoc"), vf.createURI("uri:loc_1")),
                vf.createStatement(vf.createURI("uri:loc_1"), vf.createURI("uri:hasPopulation"), vf.createLiteral(3500)),
                vf.createStatement(vf.createURI("uri:loc_2"), vf.createURI("uri:hasPopulation"), vf.createLiteral(8000)),
                vf.createStatement(vf.createURI("urn:obs_2"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time2))),
                vf.createStatement(vf.createURI("urn:obs_2"), vf.createURI("uri:hasLoc"), vf.createURI("uri:loc_2")),
                vf.createStatement(vf.createURI("urn:obs_3"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time3))),
                vf.createStatement(vf.createURI("urn:obs_3"), vf.createURI("uri:hasLoc"), vf.createURI("uri:loc_3")),
                vf.createStatement(vf.createURI("urn:obs_4"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time4))),
                vf.createStatement(vf.createURI("urn:obs_4"), vf.createURI("uri:hasLoc"), vf.createURI("uri:loc_4")),
                vf.createStatement(vf.createURI("urn:obs_5"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time4))),
                vf.createStatement(vf.createURI("urn:obs_5"), vf.createURI("uri:hasLoc"), vf.createURI("uri:loc_1")),
                vf.createStatement(vf.createURI("urn:obs_6"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time3))),
                vf.createStatement(vf.createURI("urn:obs_6"), vf.createURI("uri:hasLoc"), vf.createURI("uri:loc_2")));

        // Create the expected results of the SPARQL query once the PCJ has been computed.
        final Set<BindingSet> expectedResults = new HashSet<>();

        final long period = 1800000;
        final long binId = currentTime / period * period;

        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("total", vf.createLiteral("2", XMLSchema.INTEGER));
        bs.addBinding("location", vf.createURI("uri:loc_1"));
        bs.addBinding("population", vf.createLiteral("3500", XMLSchema.INTEGER));
        bs.addBinding("periodicBinId", vf.createLiteral(binId));
        expectedResults.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("total", vf.createLiteral("2", XMLSchema.INTEGER));
        bs.addBinding("location", vf.createURI("uri:loc_2"));
        bs.addBinding("population", vf.createLiteral("8000", XMLSchema.INTEGER));
        bs.addBinding("periodicBinId", vf.createLiteral(binId));
        expectedResults.add(bs);


        bs = new MapBindingSet();
        bs.addBinding("total", vf.createLiteral("2", XMLSchema.INTEGER));
        bs.addBinding("location", vf.createURI("uri:loc_2"));
        bs.addBinding("population", vf.createLiteral("8000", XMLSchema.INTEGER));
        bs.addBinding("periodicBinId", vf.createLiteral(binId + period));
        expectedResults.add(bs);

        // Verify the end results of the query match the expected results.
        runTest(query, statements, expectedResults, ExportStrategy.PERIODIC);
    }

    @Test(expected= UnsupportedQueryException.class)
    public void nestedConstructPeriodicQueryWithAggregationAndGroupBy() throws Exception {
        final String query = "prefix function: <http://org.apache.rya/function#> " // n
                + "prefix time: <http://www.w3.org/2006/time#> " // n
                + "construct{?location a <uri:highObservationArea> } "
                + "where { Filter(?total > 1)"
                + "?location <uri:hasPopulation> ?population . {"
                + "select ?location (count(?obs) as ?total) where {" // n
                + "Filter(function:periodic(?time, 2, .5, time:hours)) " // n
                + "?obs <uri:hasTime> ?time. " // n
                + "?obs <uri:hasLoc> ?location } group by ?location }}"; // n


        final Collection<Statement> statements = Sets.newHashSet();
        final Set<BindingSet> expectedResults = new HashSet<>();

        // Verify the end results of the query match the expected results.
        runTest(query, statements, expectedResults, ExportStrategy.PERIODIC);
    }

    public void runTest(final String sparql, final Collection<Statement> statements, final Collection<BindingSet> expectedResults,
            final ExportStrategy strategy) throws Exception {
        requireNonNull(sparql);
        requireNonNull(statements);
        requireNonNull(expectedResults);

        // Register the PCJ with Rya.
        final Connector accumuloConn = super.getAccumuloConnector();

        final RyaClient ryaClient = AccumuloRyaClientFactory.build(createConnectionDetails(), accumuloConn);

        switch (strategy) {
        case RYA:
            ryaClient.getCreatePCJ().createPCJ(getRyaInstanceName(), sparql);
            addStatementsAndWait(statements);
            // Fetch the value that is stored within the PCJ table.
            try (final PrecomputedJoinStorage pcjStorage = new AccumuloPcjStorage(accumuloConn, getRyaInstanceName())) {
                final String pcjId = pcjStorage.listPcjs().get(0);
                final Set<BindingSet> results = Sets.newHashSet(pcjStorage.listResults(pcjId));
                // Ensure the result of the query matches the expected result.
                assertEquals(expectedResults, results);
            }

            break;
        case PERIODIC:
            final PeriodicQueryResultStorage periodicStorage = new AccumuloPeriodicQueryResultStorage(accumuloConn, getRyaInstanceName());
            final String periodicId = periodicStorage.createPeriodicQuery(sparql);
            try (FluoClient fluo = new FluoClientImpl(super.getFluoConfiguration())) {
                new CreateFluoPcj().createPcj(periodicId, sparql, Sets.newHashSet(ExportStrategy.PERIODIC), fluo);
            }
            addStatementsAndWait(statements);

            final Set<BindingSet> results = Sets.newHashSet();
            try (CloseableIterator<BindingSet> resultIter = periodicStorage.listResults(periodicId, Optional.empty())) {
                while (resultIter.hasNext()) {
                    results.add(resultIter.next());
                }
            }
            assertEquals(expectedResults, results);
            break;
        default:
            throw new RuntimeException("Invalid export option");
        }
    }

    private void addStatementsAndWait(final Collection<Statement> statements) throws RepositoryException, Exception {
        // Write the data to Rya.
        final SailRepositoryConnection ryaConn = super.getRyaSailRepository().getConnection();
        ryaConn.begin();
        ryaConn.add(statements);
        ryaConn.commit();
        ryaConn.close();

        // Wait for the Fluo application to finish computing the end result.
        super.getMiniFluo().waitForObservers();
    }
}