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

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.xml.datatype.DatatypeFactory;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;
import org.apache.rya.pcj.fluo.test.base.KafkaExportITBase;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.BindingSet;
import org.openrdf.query.impl.MapBindingSet;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Performs integration tests over the Fluo application geared towards Kafka PCJ exporting.
 * <p>
 * These tests might be ignored so that they will not run as unit tests while building the application.
 * Run this test from Maven command line:
 * $ cd rya/extras/rya.pcj.fluo/pcj.fluo.integration
 * $ mvn surefire:test -Dtest=KafkaExportIT
 */
public class KafkaExportIT extends KafkaExportITBase {

    @Test
    public void newResultsExportedTest() throws Exception {
        final String sparql =
                "SELECT ?customer ?worker ?city { " +
                    "FILTER(?customer = <http://Alice>) " +
                    "FILTER(?city = <http://London>) " +
                    "?customer <http://talksTo> ?worker. " +
                    "?worker <http://livesIn> ?city. " +
                    "?worker <http://worksAt> <http://Chipotle>. " +
                "}";

        // Triples that will be streamed into Fluo after the PCJ has been created.
        final ValueFactory vf = new ValueFactoryImpl();
        final Collection<Statement> statements =
                Sets.newHashSet(
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

        // Create the PCJ in Fluo and load the statements into Rya.
        final String pcjId = loadDataAndCreateQuery(sparql, statements);

        // The expected results of the SPARQL query once the PCJ has been computed.
        final Set<BindingSet> expectedResult = new HashSet<>();

        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("customer", vf.createURI("http://Alice"));
        bs.addBinding("worker", vf.createURI("http://Bob"));
        bs.addBinding("city", vf.createURI("http://London"));
        expectedResult.add( new VisibilityBindingSet(bs) );

        bs = new MapBindingSet();
        bs.addBinding("customer", vf.createURI("http://Alice"));
        bs.addBinding("worker", vf.createURI("http://Charlie"));
        bs.addBinding("city", vf.createURI("http://London"));
        expectedResult.add( new VisibilityBindingSet(bs) );

        bs = new MapBindingSet();
        bs.addBinding("customer", vf.createURI("http://Alice"));
        bs.addBinding("worker", vf.createURI("http://David"));
        bs.addBinding("city", vf.createURI("http://London"));
        expectedResult.add( new VisibilityBindingSet(bs) );

        // Ensure the last result matches the expected result.
        final Set<VisibilityBindingSet> result = readAllResults(pcjId);
        assertEquals(expectedResult, result);
    }

    @Test
    public void min() throws Exception {
        // A query that finds the minimum price for an item within the inventory.
        final String sparql =
                "SELECT (min(?price) as ?minPrice) { " +
                    "?item <urn:price> ?price . " +
                "}";

        // Create the Statements that will be loaded into Rya.
        final ValueFactory vf = new ValueFactoryImpl();
        final Collection<Statement> statements = Sets.newHashSet(
                vf.createStatement(vf.createURI("urn:apple"), vf.createURI("urn:price"), vf.createLiteral(2.50)),
                vf.createStatement(vf.createURI("urn:gum"), vf.createURI("urn:price"), vf.createLiteral(0.99)),
                vf.createStatement(vf.createURI("urn:sandwich"), vf.createURI("urn:price"), vf.createLiteral(4.99)));

        // Create the PCJ in Fluo and load the statements into Rya.
        final String pcjId = loadDataAndCreateQuery(sparql, statements);

        // Create the expected results of the SPARQL query once the PCJ has been computed.
        final MapBindingSet expectedResult = new MapBindingSet();
        expectedResult.addBinding("minPrice", vf.createLiteral(0.99));

        // Ensure the last result matches the expected result.
        final VisibilityBindingSet result = readLastResult(pcjId);
        assertEquals(expectedResult, result);
    }

    @Test
    public void max() throws Exception {
        // A query that finds the maximum price for an item within the inventory.
        final String sparql =
                "SELECT (max(?price) as ?maxPrice) { " +
                    "?item <urn:price> ?price . " +
                "}";

        // Create the Statements that will be loaded into Rya.
        final ValueFactory vf = new ValueFactoryImpl();
        final Collection<Statement> statements = Sets.newHashSet(
                vf.createStatement(vf.createURI("urn:apple"), vf.createURI("urn:price"), vf.createLiteral(2.50)),
                vf.createStatement(vf.createURI("urn:gum"), vf.createURI("urn:price"), vf.createLiteral(0.99)),
                vf.createStatement(vf.createURI("urn:sandwich"), vf.createURI("urn:price"), vf.createLiteral(4.99)));

        // Create the PCJ in Fluo and load the statements into Rya.
        final String pcjId = loadDataAndCreateQuery(sparql, statements);

        // Create the expected results of the SPARQL query once the PCJ has been computed.
        final MapBindingSet expectedResult = new MapBindingSet();
        expectedResult.addBinding("maxPrice", vf.createLiteral(4.99));

        // Ensure the last result matches the expected result.
        final VisibilityBindingSet result = readLastResult(pcjId);
        assertEquals(expectedResult, result);
    }

    @Test
    public void count() throws Exception {
        // A query that counts the number of unique items that are in the inventory.
        final String sparql =
                "SELECT (count(?item) as ?itemCount) { " +
                    "?item <urn:id> ?id . " +
                "}";

        // Create the Statements that will be loaded into Rya.
        final ValueFactory vf = new ValueFactoryImpl();
        final Collection<Statement> statements = Sets.newHashSet(
                // Three that are part of the count.
                vf.createStatement(vf.createURI("urn:apple"), vf.createURI("urn:id"), vf.createLiteral(UUID.randomUUID().toString())),
                vf.createStatement(vf.createURI("urn:gum"), vf.createURI("urn:id"), vf.createLiteral(UUID.randomUUID().toString())),
                vf.createStatement(vf.createURI("urn:sandwich"), vf.createURI("urn:id"), vf.createLiteral(UUID.randomUUID().toString())),

                // One that is not.
                vf.createStatement(vf.createURI("urn:sandwich"), vf.createURI("urn:price"), vf.createLiteral(3.99)));

        // Create the PCJ in Fluo and load the statements into Rya.
        final String pcjId = loadDataAndCreateQuery(sparql, statements);

        // Create the expected results of the SPARQL query once the PCJ has been computed.
        final MapBindingSet expectedResult = new MapBindingSet();
        expectedResult.addBinding("itemCount", vf.createLiteral("3", XMLSchema.INTEGER));

        // Ensure the last result matches the expected result.
        final VisibilityBindingSet result = readLastResult(pcjId);
        assertEquals(expectedResult, result);
    }

    @Test
    public void sum() throws Exception {
        // A query that sums the counts of all of the items that are in the inventory.
        final String sparql =
                "SELECT (sum(?count) as ?itemSum) { " +
                    "?item <urn:count> ?count . " +
                "}";

        // Create the Statements that will be loaded into Rya.
        final ValueFactory vf = new ValueFactoryImpl();
        final Collection<Statement> statements = Sets.newHashSet(
                vf.createStatement(vf.createURI("urn:apple"), vf.createURI("urn:count"), vf.createLiteral(5)),
                vf.createStatement(vf.createURI("urn:gum"), vf.createURI("urn:count"), vf.createLiteral(7)),
                vf.createStatement(vf.createURI("urn:sandwich"), vf.createURI("urn:count"), vf.createLiteral(2)));

        // Create the PCJ in Fluo and load the statements into Rya.
        final String pcjId = loadDataAndCreateQuery(sparql, statements);

        // Create the expected results of the SPARQL query once the PCJ has been computed.
        final MapBindingSet expectedResult = new MapBindingSet();
        expectedResult.addBinding("itemSum", vf.createLiteral("14", XMLSchema.INTEGER));

        // Ensure the last result matches the expected result.
        final VisibilityBindingSet result = readLastResult(pcjId);
        assertEquals(expectedResult, result);
    }

    @Test
    public void average() throws Exception  {
        // A query that finds the average price for an item that is in the inventory.
        final String sparql =
                "SELECT (avg(?price) as ?averagePrice) { " +
                    "?item <urn:price> ?price . " +
                "}";

        // Create the Statements that will be loaded into Rya.
        final ValueFactory vf = new ValueFactoryImpl();
        final Collection<Statement> statements = Sets.newHashSet(
                vf.createStatement(vf.createURI("urn:apple"), vf.createURI("urn:price"), vf.createLiteral(3)),
                vf.createStatement(vf.createURI("urn:gum"), vf.createURI("urn:price"), vf.createLiteral(4)),
                vf.createStatement(vf.createURI("urn:sandwich"), vf.createURI("urn:price"), vf.createLiteral(8)));

        // Create the PCJ in Fluo and load the statements into Rya.
        final String pcjId = loadDataAndCreateQuery(sparql, statements);
        
        // Create the expected results of the SPARQL query once the PCJ has been computed.
        final MapBindingSet expectedResult = new MapBindingSet();
        expectedResult.addBinding("averagePrice", vf.createLiteral("5", XMLSchema.DECIMAL));

        // Ensure the last result matches the expected result.
        final VisibilityBindingSet result = readLastResult(pcjId);
        assertEquals(expectedResult, result);
    }

    @Test
    public void aggregateWithFilter() throws Exception {
        // A query that filters results from a statement pattern before applying the aggregation function.
        final String sparql =
                "SELECT (min(?price) as ?minPrice) { " +
                    "FILTER(?price > 1.00) " +
                    "?item <urn:price> ?price . " +
                "}";

        // Create the Statements that will be loaded into Rya.
        final ValueFactory vf = new ValueFactoryImpl();
        final Collection<Statement> statements = Sets.newHashSet(
                vf.createStatement(vf.createURI("urn:apple"), vf.createURI("urn:price"), vf.createLiteral(2.50)),
                vf.createStatement(vf.createURI("urn:gum"), vf.createURI("urn:price"), vf.createLiteral(0.99)),
                vf.createStatement(vf.createURI("urn:sandwich"), vf.createURI("urn:price"), vf.createLiteral(4.99)));

        // Create the PCJ in Fluo and load the statements into Rya.
        final String pcjId = loadDataAndCreateQuery(sparql, statements);

        // Create the expected results of the SPARQL query once the PCJ has been computed.
        final MapBindingSet expectedResult = new MapBindingSet();
        expectedResult.addBinding("minPrice", vf.createLiteral(2.50));

        // Ensure the last result matches the expected result.
        final VisibilityBindingSet result = readLastResult(pcjId);
        assertEquals(expectedResult, result);
    }

    @Test
    public void multipleAggregations() throws Exception {
        // A query that both counts the number of items being averaged and finds the average price.
        final String sparql =
                "SELECT (count(?item) as ?itemCount) (avg(?price) as ?averagePrice) {" +
                    "?item <urn:price> ?price . " +
                "}";

        // Create the Statements that will be loaded into Rya.
        final ValueFactory vf = new ValueFactoryImpl();
        final Collection<Statement> statements = Sets.newHashSet(
                vf.createStatement(vf.createURI("urn:apple"), vf.createURI("urn:price"), vf.createLiteral(5.25)),
                vf.createStatement(vf.createURI("urn:gum"), vf.createURI("urn:price"), vf.createLiteral(7)),
                vf.createStatement(vf.createURI("urn:sandwich"), vf.createURI("urn:price"), vf.createLiteral(2.75)));

        // Create the PCJ in Fluo and load the statements into Rya.
        final String pcjId = loadDataAndCreateQuery(sparql, statements);

        // Create the expected results of the SPARQL query once the PCJ has been computed.
        final MapBindingSet expectedResult = new MapBindingSet();
        expectedResult.addBinding("itemCount", vf.createLiteral("3", XMLSchema.INTEGER));
        expectedResult.addBinding("averagePrice", vf.createLiteral("5.0", XMLSchema.DECIMAL));

        // Ensure the last result matches the expected result.
        final VisibilityBindingSet result = readLastResult(pcjId);
        assertEquals(expectedResult, result);
    }

    @Test
    public void groupBySingleBinding() throws Exception {
        // A query that groups what is aggregated by one of the keys.
        final String sparql =
                "SELECT ?item (avg(?price) as ?averagePrice) {" +
                    "?item <urn:price> ?price . " +
                "} " +
                "GROUP BY ?item";

        // Create the Statements that will be loaded into Rya.
        final ValueFactory vf = new ValueFactoryImpl();
        final Collection<Statement> statements = Sets.newHashSet(
                vf.createStatement(vf.createURI("urn:apple"), vf.createURI("urn:price"), vf.createLiteral(5.25)),
                vf.createStatement(vf.createURI("urn:apple"), vf.createURI("urn:price"), vf.createLiteral(7)),
                vf.createStatement(vf.createURI("urn:apple"), vf.createURI("urn:price"), vf.createLiteral(2.75)),
                vf.createStatement(vf.createURI("urn:banana"), vf.createURI("urn:price"), vf.createLiteral(2.75)),
                vf.createStatement(vf.createURI("urn:banana"), vf.createURI("urn:price"), vf.createLiteral(1.99)));

        // Create the PCJ in Fluo and load the statements into Rya.
        final String pcjId = loadDataAndCreateQuery(sparql, statements);

        // Create the expected results of the SPARQL query once the PCJ has been computed.
        final Set<VisibilityBindingSet> expectedResults = new HashSet<>();

        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("item", vf.createURI("urn:apple"));
        bs.addBinding("averagePrice", vf.createLiteral("5.0", XMLSchema.DECIMAL));
        expectedResults.add( new VisibilityBindingSet(bs) );

        bs = new MapBindingSet();
        bs.addBinding("item", vf.createURI("urn:banana"));
        bs.addBinding("averagePrice", vf.createLiteral("2.37", XMLSchema.DECIMAL));
        expectedResults.add( new VisibilityBindingSet(bs) );

        // Verify the end results of the query match the expected results.
        final Set<VisibilityBindingSet> results = readGroupedResults(pcjId, new VariableOrder("item"));
        assertEquals(expectedResults, results);
    }

    @Test
    public void groupByManyBindings_averages() throws Exception {
        // A query that groups what is aggregated by two of the keys.
        final String sparql =
                "SELECT ?type ?location (avg(?price) as ?averagePrice) {" +
                    "?id <urn:type> ?type . " +
                    "?id <urn:location> ?location ." +
                    "?id <urn:price> ?price ." +
                "} " +
                "GROUP BY ?type ?location";

        // Create the Statements that will be loaded into Rya.
        final ValueFactory vf = new ValueFactoryImpl();
        final Collection<Statement> statements = Sets.newHashSet(
                // American items that will be averaged.
                vf.createStatement(vf.createURI("urn:1"), vf.createURI("urn:type"), vf.createLiteral("apple")),
                vf.createStatement(vf.createURI("urn:1"), vf.createURI("urn:location"), vf.createLiteral("USA")),
                vf.createStatement(vf.createURI("urn:1"), vf.createURI("urn:price"), vf.createLiteral(2.50)),

                vf.createStatement(vf.createURI("urn:2"), vf.createURI("urn:type"), vf.createLiteral("cheese")),
                vf.createStatement(vf.createURI("urn:2"), vf.createURI("urn:location"), vf.createLiteral("USA")),
                vf.createStatement(vf.createURI("urn:2"), vf.createURI("urn:price"), vf.createLiteral(.99)),

                vf.createStatement(vf.createURI("urn:3"), vf.createURI("urn:type"), vf.createLiteral("cheese")),
                vf.createStatement(vf.createURI("urn:3"), vf.createURI("urn:location"), vf.createLiteral("USA")),
                vf.createStatement(vf.createURI("urn:3"), vf.createURI("urn:price"), vf.createLiteral(5.25)),

                // French items that will be averaged.
                vf.createStatement(vf.createURI("urn:4"), vf.createURI("urn:type"), vf.createLiteral("cheese")),
                vf.createStatement(vf.createURI("urn:4"), vf.createURI("urn:location"), vf.createLiteral("France")),
                vf.createStatement(vf.createURI("urn:4"), vf.createURI("urn:price"), vf.createLiteral(8.5)),

                vf.createStatement(vf.createURI("urn:5"), vf.createURI("urn:type"), vf.createLiteral("cigarettes")),
                vf.createStatement(vf.createURI("urn:5"), vf.createURI("urn:location"), vf.createLiteral("France")),
                vf.createStatement(vf.createURI("urn:5"), vf.createURI("urn:price"), vf.createLiteral(3.99)),

                vf.createStatement(vf.createURI("urn:6"), vf.createURI("urn:type"), vf.createLiteral("cigarettes")),
                vf.createStatement(vf.createURI("urn:6"), vf.createURI("urn:location"), vf.createLiteral("France")),
                vf.createStatement(vf.createURI("urn:6"), vf.createURI("urn:price"), vf.createLiteral(4.99)));

        // Create the PCJ in Fluo and load the statements into Rya.
        final String pcjId = loadDataAndCreateQuery(sparql, statements);

        // Create the expected results of the SPARQL query once the PCJ has been computed.
        final Set<VisibilityBindingSet> expectedResults = new HashSet<>();

        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("type", vf.createLiteral("apple", XMLSchema.STRING));
        bs.addBinding("location", vf.createLiteral("USA", XMLSchema.STRING));
        bs.addBinding("averagePrice", vf.createLiteral("2.5", XMLSchema.DECIMAL));
        expectedResults.add( new VisibilityBindingSet(bs) );

        bs = new MapBindingSet();
        bs.addBinding("type", vf.createLiteral("cheese", XMLSchema.STRING));
        bs.addBinding("location", vf.createLiteral("USA", XMLSchema.STRING));
        bs.addBinding("averagePrice", vf.createLiteral("3.12", XMLSchema.DECIMAL));
        expectedResults.add( new VisibilityBindingSet(bs) );

        bs = new MapBindingSet();
        bs.addBinding("type", vf.createLiteral("cheese", XMLSchema.STRING));
        bs.addBinding("location", vf.createLiteral("France", XMLSchema.STRING));
        bs.addBinding("averagePrice", vf.createLiteral("8.5", XMLSchema.DECIMAL));
        expectedResults.add( new VisibilityBindingSet(bs));

        bs = new MapBindingSet();
        bs.addBinding("type", vf.createLiteral("cigarettes", XMLSchema.STRING));
        bs.addBinding("location", vf.createLiteral("France", XMLSchema.STRING));
        bs.addBinding("averagePrice", vf.createLiteral("4.49", XMLSchema.DECIMAL));
        expectedResults.add( new VisibilityBindingSet(bs) );

        // Verify the end results of the query match the expected results.
        final Set<VisibilityBindingSet> results = readGroupedResults(pcjId, new VariableOrder("type", "location"));
        assertEquals(expectedResults, results);
    }

    
    @Test
    public void nestedGroupByManyBindings_averages() throws Exception {
        // A query that groups what is aggregated by two of the keys.
        final String sparql =
                "SELECT ?type ?location ?averagePrice {" +
                "FILTER(?averagePrice > 4) " +
                "{SELECT ?type ?location (avg(?price) as ?averagePrice) {" +
                    "?id <urn:type> ?type . " +
                    "?id <urn:location> ?location ." +
                    "?id <urn:price> ?price ." +
                "} " +
                "GROUP BY ?type ?location }}";

        // Create the Statements that will be loaded into Rya.
        final ValueFactory vf = new ValueFactoryImpl();
        final Collection<Statement> statements = Sets.newHashSet(
                // American items that will be averaged.
                vf.createStatement(vf.createURI("urn:1"), vf.createURI("urn:type"), vf.createLiteral("apple")),
                vf.createStatement(vf.createURI("urn:1"), vf.createURI("urn:location"), vf.createLiteral("USA")),
                vf.createStatement(vf.createURI("urn:1"), vf.createURI("urn:price"), vf.createLiteral(2.50)),

                vf.createStatement(vf.createURI("urn:2"), vf.createURI("urn:type"), vf.createLiteral("cheese")),
                vf.createStatement(vf.createURI("urn:2"), vf.createURI("urn:location"), vf.createLiteral("USA")),
                vf.createStatement(vf.createURI("urn:2"), vf.createURI("urn:price"), vf.createLiteral(4.25)),

                vf.createStatement(vf.createURI("urn:3"), vf.createURI("urn:type"), vf.createLiteral("cheese")),
                vf.createStatement(vf.createURI("urn:3"), vf.createURI("urn:location"), vf.createLiteral("USA")),
                vf.createStatement(vf.createURI("urn:3"), vf.createURI("urn:price"), vf.createLiteral(5.25)),

                // French items that will be averaged.
                vf.createStatement(vf.createURI("urn:4"), vf.createURI("urn:type"), vf.createLiteral("cheese")),
                vf.createStatement(vf.createURI("urn:4"), vf.createURI("urn:location"), vf.createLiteral("France")),
                vf.createStatement(vf.createURI("urn:4"), vf.createURI("urn:price"), vf.createLiteral(8.5)),

                vf.createStatement(vf.createURI("urn:5"), vf.createURI("urn:type"), vf.createLiteral("cigarettes")),
                vf.createStatement(vf.createURI("urn:5"), vf.createURI("urn:location"), vf.createLiteral("France")),
                vf.createStatement(vf.createURI("urn:5"), vf.createURI("urn:price"), vf.createLiteral(3.99)),

                vf.createStatement(vf.createURI("urn:6"), vf.createURI("urn:type"), vf.createLiteral("cigarettes")),
                vf.createStatement(vf.createURI("urn:6"), vf.createURI("urn:location"), vf.createLiteral("France")),
                vf.createStatement(vf.createURI("urn:6"), vf.createURI("urn:price"), vf.createLiteral(4.99)));

        // Create the PCJ in Fluo and load the statements into Rya.
        final String pcjId = loadDataAndCreateQuery(sparql, statements);

        // Create the expected results of the SPARQL query once the PCJ has been computed.
        final Set<VisibilityBindingSet> expectedResults = new HashSet<>();

        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("type", vf.createLiteral("cheese", XMLSchema.STRING));
        bs.addBinding("location", vf.createLiteral("France", XMLSchema.STRING));
        bs.addBinding("averagePrice", vf.createLiteral("8.5", XMLSchema.DECIMAL));
        expectedResults.add( new VisibilityBindingSet(bs));

        bs = new MapBindingSet();
        bs.addBinding("type", vf.createLiteral("cigarettes", XMLSchema.STRING));
        bs.addBinding("location", vf.createLiteral("France", XMLSchema.STRING));
        bs.addBinding("averagePrice", vf.createLiteral("4.49", XMLSchema.DECIMAL));
        expectedResults.add( new VisibilityBindingSet(bs) );
        
        bs = new MapBindingSet();
        bs.addBinding("type", vf.createLiteral("cheese", XMLSchema.STRING));
        bs.addBinding("location", vf.createLiteral("USA", XMLSchema.STRING));
        bs.addBinding("averagePrice", vf.createLiteral("4.75", XMLSchema.DECIMAL));
        expectedResults.add( new VisibilityBindingSet(bs) );

        // Verify the end results of the query match the expected results.
        final Set<VisibilityBindingSet> results = readGroupedResults(pcjId, new VariableOrder("type", "location"));
        assertEquals(expectedResults, results);
    }
    
    
    @Test
    public void nestedWithJoinGroupByManyBindings_averages() throws Exception {
       
        // A query that groups what is aggregated by two of the keys.
        final String sparql =
                "SELECT ?type ?location ?averagePrice ?milkType {" +
                "FILTER(?averagePrice > 4) " +
                "?type <urn:hasMilkType> ?milkType ." +
                "{SELECT ?type ?location (avg(?price) as ?averagePrice) {" +
                    "?id <urn:type> ?type . " +
                    "?id <urn:location> ?location ." +
                    "?id <urn:price> ?price ." +
                "} " +
                "GROUP BY ?type ?location }}";

        // Create the Statements that will be loaded into Rya.
        final ValueFactory vf = new ValueFactoryImpl();
        final Collection<Statement> statements = Sets.newHashSet(
               
                vf.createStatement(vf.createURI("urn:1"), vf.createURI("urn:type"), vf.createURI("urn:blue")),
                vf.createStatement(vf.createURI("urn:1"), vf.createURI("urn:location"), vf.createLiteral("France")),
                vf.createStatement(vf.createURI("urn:1"), vf.createURI("urn:price"), vf.createLiteral(8.5)),
                vf.createStatement(vf.createURI("urn:blue"), vf.createURI("urn:hasMilkType"), vf.createLiteral("cow", XMLSchema.STRING)),

                vf.createStatement(vf.createURI("urn:2"), vf.createURI("urn:type"), vf.createURI("urn:american")),
                vf.createStatement(vf.createURI("urn:2"), vf.createURI("urn:location"), vf.createLiteral("USA")),
                vf.createStatement(vf.createURI("urn:2"), vf.createURI("urn:price"), vf.createLiteral(.99)),

                vf.createStatement(vf.createURI("urn:3"), vf.createURI("urn:type"), vf.createURI("urn:cheddar")),
                vf.createStatement(vf.createURI("urn:3"), vf.createURI("urn:location"), vf.createLiteral("USA")),
                vf.createStatement(vf.createURI("urn:3"), vf.createURI("urn:price"), vf.createLiteral(5.25)),

                // French items that will be averaged.
                vf.createStatement(vf.createURI("urn:4"), vf.createURI("urn:type"), vf.createURI("urn:goat")),
                vf.createStatement(vf.createURI("urn:4"), vf.createURI("urn:location"), vf.createLiteral("France")),
                vf.createStatement(vf.createURI("urn:4"), vf.createURI("urn:price"), vf.createLiteral(6.5)),
                vf.createStatement(vf.createURI("urn:goat"), vf.createURI("urn:hasMilkType"), vf.createLiteral("goat", XMLSchema.STRING)),
                
                vf.createStatement(vf.createURI("urn:5"), vf.createURI("urn:type"), vf.createURI("urn:fontina")),
                vf.createStatement(vf.createURI("urn:5"), vf.createURI("urn:location"), vf.createLiteral("Italy")),
                vf.createStatement(vf.createURI("urn:5"), vf.createURI("urn:price"), vf.createLiteral(3.99)),
                vf.createStatement(vf.createURI("urn:fontina"), vf.createURI("urn:hasMilkType"), vf.createLiteral("cow", XMLSchema.STRING)),

                vf.createStatement(vf.createURI("urn:6"), vf.createURI("urn:type"), vf.createURI("urn:fontina")),
                vf.createStatement(vf.createURI("urn:6"), vf.createURI("urn:location"), vf.createLiteral("Italy")),
                vf.createStatement(vf.createURI("urn:6"), vf.createURI("urn:price"), vf.createLiteral(4.99)));

        // Create the PCJ in Fluo and load the statements into Rya.
        final String pcjId = loadDataAndCreateQuery(sparql, statements);

        // Create the expected results of the SPARQL query once the PCJ has been computed.
        final Set<VisibilityBindingSet> expectedResults = new HashSet<>();

        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("type", vf.createURI("urn:blue"));
        bs.addBinding("location", vf.createLiteral("France", XMLSchema.STRING));
        bs.addBinding("averagePrice", vf.createLiteral("8.5", XMLSchema.DECIMAL));
        bs.addBinding("milkType", vf.createLiteral("cow", XMLSchema.STRING));
        expectedResults.add( new VisibilityBindingSet(bs));

        bs = new MapBindingSet();
        bs.addBinding("type", vf.createURI("urn:goat"));
        bs.addBinding("location", vf.createLiteral("France", XMLSchema.STRING));
        bs.addBinding("averagePrice", vf.createLiteral("6.5", XMLSchema.DECIMAL));
        bs.addBinding("milkType", vf.createLiteral("goat", XMLSchema.STRING));
        expectedResults.add( new VisibilityBindingSet(bs) );
        
        bs = new MapBindingSet();
        bs.addBinding("type", vf.createURI("urn:fontina"));
        bs.addBinding("location", vf.createLiteral("Italy", XMLSchema.STRING));
        bs.addBinding("averagePrice", vf.createLiteral("4.49", XMLSchema.DECIMAL));
        bs.addBinding("milkType", vf.createLiteral("cow", XMLSchema.STRING));
        expectedResults.add( new VisibilityBindingSet(bs) );

        // Verify the end results of the query match the expected results.
        final Set<VisibilityBindingSet> results = readGroupedResults(pcjId, new VariableOrder("type", "location"));
        assertEquals(expectedResults, results);
    }
    
    
    @Test
    public void nestedWithJoinGroupByManyBindings_average_oneResult() throws Exception {
       
        // A query that groups what is aggregated by two of the keys.
        final String sparql =
                "SELECT ?id ?price ?location ?averagePrice {" +
                 "?id <urn:location> ?location ." +
                "FILTER(abs(?averagePrice - ?price) > 1) " +
                "?id <urn:price> ?price ." +
                "{SELECT ?type (avg(?price) as ?averagePrice) {" +
                    "?id <urn:type> ?type . " +
                    "?id <urn:price> ?price ." +
                "} " +
                "GROUP BY ?type }}";

        // Create the Statements that will be loaded into Rya.
        // Shift result above four and then below.  Make sure no results are emitted
        // when milkType is joined.
        final ValueFactory vf = new ValueFactoryImpl();
        final Collection<Statement> statements = Lists.newArrayList(
               
                vf.createStatement(vf.createURI("urn:1"), vf.createURI("urn:type"), vf.createURI("urn:blue")),
                vf.createStatement(vf.createURI("urn:1"), vf.createURI("urn:location"), vf.createLiteral("loc_1")),
                vf.createStatement(vf.createURI("urn:1"), vf.createURI("urn:price"), vf.createLiteral(6)),
                
                vf.createStatement(vf.createURI("urn:2"), vf.createURI("urn:type"), vf.createURI("urn:blue")),
                vf.createStatement(vf.createURI("urn:2"), vf.createURI("urn:location"), vf.createLiteral("loc_2")),
                vf.createStatement(vf.createURI("urn:2"), vf.createURI("urn:price"), vf.createLiteral(3)),
                
                vf.createStatement(vf.createURI("urn:3"), vf.createURI("urn:type"), vf.createURI("urn:blue")),
                vf.createStatement(vf.createURI("urn:3"), vf.createURI("urn:location"), vf.createLiteral("loc_3")),
                vf.createStatement(vf.createURI("urn:3"), vf.createURI("urn:price"), vf.createLiteral(3))
              );

        // Create the PCJ in Fluo and load the statements into Rya.
        final String pcjId = loadDataAndCreateQuery(sparql, statements);

        // Create the expected results of the SPARQL query once the PCJ has been computed.
        MapBindingSet expected = new MapBindingSet();
        expected.addBinding("id", vf.createURI("urn:1"));
        expected.addBinding("price", vf.createLiteral("6", XMLSchema.INTEGER));
        expected.addBinding("location", vf.createLiteral("loc_1", XMLSchema.STRING));
        expected.addBinding("averagePrice", vf.createLiteral("4", XMLSchema.DECIMAL));
        VisibilityBindingSet expectedBs = new VisibilityBindingSet(expected);
        
        // Verify the end results of the query match the expected results.
        final VisibilityBindingSet results = readLastResult(pcjId);
        assertEquals(expectedBs, results);
    }
    
    @Test
    public void nestedWithJoinGroupByManyBindings_average_lateJoin() throws Exception {
       
        // A query that groups what is aggregated by two of the keys.
        final String sparql =
                "SELECT ?id ?price ?location ?averagePrice {" +
                 "?id <urn:location> ?location ." +
                "FILTER(abs(?averagePrice - ?price) > 1) " +
                "?id <urn:price> ?price ." +
                "{SELECT ?type (avg(?price) as ?averagePrice) {" +
                    "?id <urn:type> ?type . " +
                    "?id <urn:price> ?price ." +
                "} " +
                "GROUP BY ?type }}";

        // Create the Statements that will be loaded into Rya.
        // Shift result above four and then below.  Make sure no results are emitted
        // when milkType is joined.
        final ValueFactory vf = new ValueFactoryImpl();
        final Collection<Statement> statements = Lists.newArrayList(
               
                vf.createStatement(vf.createURI("urn:1"), vf.createURI("urn:type"), vf.createURI("urn:blue")),
                vf.createStatement(vf.createURI("urn:1"), vf.createURI("urn:price"), vf.createLiteral(6)),
                
                vf.createStatement(vf.createURI("urn:2"), vf.createURI("urn:type"), vf.createURI("urn:blue")),
                vf.createStatement(vf.createURI("urn:2"), vf.createURI("urn:price"), vf.createLiteral(3)),
                
                vf.createStatement(vf.createURI("urn:3"), vf.createURI("urn:type"), vf.createURI("urn:blue")),
                vf.createStatement(vf.createURI("urn:3"), vf.createURI("urn:price"), vf.createLiteral(3))
              );

        // Create the PCJ in Fluo and load the statements into Rya.
        final String pcjId = loadDataAndCreateQuery(sparql, statements);

        loadData(Sets.newHashSet(
                vf.createStatement(vf.createURI("urn:1"), vf.createURI("urn:location"), vf.createLiteral("loc_1")),
                vf.createStatement(vf.createURI("urn:2"), vf.createURI("urn:location"), vf.createLiteral("loc_2")),
                vf.createStatement(vf.createURI("urn:3"), vf.createURI("urn:location"), vf.createLiteral("loc_3"))
                ));
        
        // Create the expected results of the SPARQL query once the PCJ has been computed.
        MapBindingSet expected = new MapBindingSet();
        expected.addBinding("id", vf.createURI("urn:1"));
        expected.addBinding("price", vf.createLiteral("6", XMLSchema.INTEGER));
        expected.addBinding("location", vf.createLiteral("loc_1", XMLSchema.STRING));
        expected.addBinding("averagePrice", vf.createLiteral("4", XMLSchema.DECIMAL));
        VisibilityBindingSet expectedBs = new VisibilityBindingSet(expected);
        
        // Verify the end results of the query match the expected results.
        final VisibilityBindingSet results = readLastResult(pcjId);
        assertEquals(expectedBs, results);
    }
    
    @Test
    public void nestedWithJoinGroupByManyBindings_incDecAverage() throws Exception {
       
        // A query that groups what is aggregated by two of the keys.
        final String sparql =
                "SELECT ?id ?price ?location ?averagePrice {" +
                 "?id <urn:location> ?location ." +
                "FILTER(abs(?averagePrice - ?price) > 1) " +
                "?id <urn:price> ?price ." +
                "{SELECT ?type (avg(?price) as ?averagePrice) {" +
                    "?id <urn:type> ?type . " +
                    "?id <urn:price> ?price ." +
                "} " +
                "GROUP BY ?type }}";

        // Create the Statements that will be loaded into Rya.
        // Shift result above four and then below.  Make sure no results are emitted
        // when milkType is joined.
        final ValueFactory vf = new ValueFactoryImpl();
        final Collection<Statement> statements = Lists.newArrayList(
               
                vf.createStatement(vf.createURI("urn:1"), vf.createURI("urn:type"), vf.createURI("urn:blue")),
                vf.createStatement(vf.createURI("urn:1"), vf.createURI("urn:price"), vf.createLiteral(5)),
                
                vf.createStatement(vf.createURI("urn:2"), vf.createURI("urn:type"), vf.createURI("urn:blue")),
                vf.createStatement(vf.createURI("urn:2"), vf.createURI("urn:price"), vf.createLiteral(2)),
                
                vf.createStatement(vf.createURI("urn:3"), vf.createURI("urn:type"), vf.createURI("urn:blue")),
                vf.createStatement(vf.createURI("urn:3"), vf.createURI("urn:price"), vf.createLiteral(5)),
                
                vf.createStatement(vf.createURI("urn:1"), vf.createURI("urn:location"), vf.createLiteral("loc_1")),
                vf.createStatement(vf.createURI("urn:2"), vf.createURI("urn:location"), vf.createLiteral("loc_2")),
                vf.createStatement(vf.createURI("urn:3"), vf.createURI("urn:location"), vf.createLiteral("loc_3"))
              );

        // Create the PCJ in Fluo and load the statements into Rya.
        final String pcjId = loadDataAndCreateQuery(sparql, statements);
        
        // Create the expected results of the SPARQL query once the PCJ has been computed.
        MapBindingSet expected = new MapBindingSet();
        expected.addBinding("id", vf.createURI("urn:2"));
        expected.addBinding("price", vf.createLiteral("2", XMLSchema.INTEGER));
        expected.addBinding("location", vf.createLiteral("loc_2", XMLSchema.STRING));
        expected.addBinding("averagePrice", vf.createLiteral("4", XMLSchema.DECIMAL));
        VisibilityBindingSet expectedBs = new VisibilityBindingSet(expected);
        
        // Verify the end results of the query match the expected results.
        final VisibilityBindingSet results = readLastResult(pcjId);
        assertEquals(expectedBs, results);
    }
    
    
    @Test
    public void nestedJoinOnAggregationResult() throws Exception {
        String sparql = "select ?location ?lastObserved where {{" //n
                + "select (MAX(?time) as ?lastObserved) where {" // n
                + "?obs <uri:hasTime> ?time }} . " //n
                + "?obs <uri:hasTime> ?lastObserved . " //n
                + "?obs <uri:hasLoc> ?location . }"; // n

        // Create the Statements that will be loaded into Rya.
        final ValueFactory vf = new ValueFactoryImpl();
        final DatatypeFactory dtf = DatatypeFactory.newInstance();
        ZonedDateTime time = ZonedDateTime.now();

        ZonedDateTime zTime1 = time.minusMinutes(30);
        String time1 = zTime1.format(DateTimeFormatter.ISO_INSTANT);

        ZonedDateTime zTime2 = zTime1.minusMinutes(30);
        String time2 = zTime2.format(DateTimeFormatter.ISO_INSTANT);

        ZonedDateTime zTime3 = zTime2.minusMinutes(30);
        String time3 = zTime3.format(DateTimeFormatter.ISO_INSTANT);

        ZonedDateTime zTime4 = zTime3.minusMinutes(30);
        String time4 = zTime4.format(DateTimeFormatter.ISO_INSTANT);

        final Collection<Statement> statements = Sets.newHashSet(
                vf.createStatement(vf.createURI("urn:obs_1"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time1))),
                vf.createStatement(vf.createURI("urn:obs_1"), vf.createURI("uri:hasLoc"), vf.createURI("uri:loc_1")),
                vf.createStatement(vf.createURI("urn:obs_2"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time2))),
                vf.createStatement(vf.createURI("urn:obs_2"), vf.createURI("uri:hasLoc"), vf.createURI("uri:loc_2")),
                vf.createStatement(vf.createURI("urn:obs_3"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time3))),
                vf.createStatement(vf.createURI("urn:obs_3"), vf.createURI("uri:hasLoc"), vf.createURI("uri:loc_3")),
                vf.createStatement(vf.createURI("urn:obs_4"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time4))),
                vf.createStatement(vf.createURI("urn:obs_4"), vf.createURI("uri:hasLoc"), vf.createURI("uri:loc_4")));

        String pcjId = loadDataAndCreateQuery(sparql, statements);
        
        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("lastObserved", vf.createLiteral(dtf.newXMLGregorianCalendar(time1)));
        bs.addBinding("location", vf.createURI("uri:loc_1"));
        VisibilityBindingSet expected = new VisibilityBindingSet(bs);

        VisibilityBindingSet results = readLastResult(pcjId);
        assertEquals(expected, results);
    }
    
    
    @Test
    public void nestedJoinOnAggregationResultReverseOrder() throws Exception {
        String sparql = "select ?location ?lastObserved where {" //n
                + "?obs <uri:hasTime> ?lastObserved . " //n
                + "?obs <uri:hasLoc> ?location . " //n
                + "{ select (MAX(?time) as ?lastObserved) where {" // n
                + "?obs <uri:hasTime> ?time }}}";

        // Create the Statements that will be loaded into Rya.
        final ValueFactory vf = new ValueFactoryImpl();
        final DatatypeFactory dtf = DatatypeFactory.newInstance();
        ZonedDateTime time = ZonedDateTime.now();

        ZonedDateTime zTime1 = time.minusMinutes(30);
        String time1 = zTime1.format(DateTimeFormatter.ISO_INSTANT);

        ZonedDateTime zTime2 = zTime1.minusMinutes(30);
        String time2 = zTime2.format(DateTimeFormatter.ISO_INSTANT);

        ZonedDateTime zTime3 = zTime2.minusMinutes(30);
        String time3 = zTime3.format(DateTimeFormatter.ISO_INSTANT);

        ZonedDateTime zTime4 = zTime3.minusMinutes(30);
        String time4 = zTime4.format(DateTimeFormatter.ISO_INSTANT);

        final Collection<Statement> statements = Sets.newHashSet(
                vf.createStatement(vf.createURI("urn:obs_1"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time1))),
                vf.createStatement(vf.createURI("urn:obs_1"), vf.createURI("uri:hasLoc"), vf.createURI("uri:loc_1")),
                vf.createStatement(vf.createURI("urn:obs_2"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time2))),
                vf.createStatement(vf.createURI("urn:obs_2"), vf.createURI("uri:hasLoc"), vf.createURI("uri:loc_2")),
                vf.createStatement(vf.createURI("urn:obs_3"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time3))),
                vf.createStatement(vf.createURI("urn:obs_3"), vf.createURI("uri:hasLoc"), vf.createURI("uri:loc_3")),
                vf.createStatement(vf.createURI("urn:obs_4"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time4))),
                vf.createStatement(vf.createURI("urn:obs_4"), vf.createURI("uri:hasLoc"), vf.createURI("uri:loc_4")));

        String pcjId = loadDataAndCreateQuery(sparql, statements);
        
        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("lastObserved", vf.createLiteral(dtf.newXMLGregorianCalendar(time1)));
        bs.addBinding("location", vf.createURI("uri:loc_1"));
        VisibilityBindingSet expected = new VisibilityBindingSet(bs);

        VisibilityBindingSet results = readLastResult(pcjId);
        assertEquals(expected, results);
    }
    
    @Test
    public void nestedJoinOnAggregationLateJoin() throws Exception {
        //this is meant to verify that the location statements (which are
        //entered after the initial observation statements) are not joined
        //with old, out of date aggregation state observations
        String sparql = "select ?location ?lastObserved where {{" //n
                + "select (MAX(?time) as ?lastObserved) where {" // n
                + "?obs <uri:hasTime> ?time }} . " //n
                + "?obs <uri:hasTime> ?lastObserved . " //n
                + "?obs <uri:hasLoc> ?location . }"; // n

        // Create the Statements that will be loaded into Rya.
        final ValueFactory vf = new ValueFactoryImpl();
        final DatatypeFactory dtf = DatatypeFactory.newInstance();
        ZonedDateTime time = ZonedDateTime.now();

        ZonedDateTime zTime1 = time.minusMinutes(30);
        String time1 = zTime1.format(DateTimeFormatter.ISO_INSTANT);

        ZonedDateTime zTime2 = zTime1.minusMinutes(30);
        String time2 = zTime2.format(DateTimeFormatter.ISO_INSTANT);

        ZonedDateTime zTime3 = zTime2.minusMinutes(30);
        String time3 = zTime3.format(DateTimeFormatter.ISO_INSTANT);

        ZonedDateTime zTime4 = zTime3.minusMinutes(30);
        String time4 = zTime4.format(DateTimeFormatter.ISO_INSTANT);

        final Collection<Statement> statements = Sets.newHashSet(
                vf.createStatement(vf.createURI("urn:obs_1"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time1))),
                vf.createStatement(vf.createURI("urn:obs_2"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time2))),
                vf.createStatement(vf.createURI("urn:obs_3"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time3))),
                vf.createStatement(vf.createURI("urn:obs_4"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time4)))
               );

        String pcjId = loadDataAndCreateQuery(sparql, statements);
        
        loadData(Sets.newHashSet(
                vf.createStatement(vf.createURI("urn:obs_1"), vf.createURI("uri:hasLoc"), vf.createURI("uri:loc_1")),
                vf.createStatement(vf.createURI("urn:obs_2"), vf.createURI("uri:hasLoc"), vf.createURI("uri:loc_2")),
                vf.createStatement(vf.createURI("urn:obs_3"), vf.createURI("uri:hasLoc"), vf.createURI("uri:loc_3")),
                vf.createStatement(vf.createURI("urn:obs_4"), vf.createURI("uri:hasLoc"), vf.createURI("uri:loc_4"))
                ));
        
        Set<VisibilityBindingSet> expected = new HashSet<>();
        
        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("lastObserved", vf.createLiteral(dtf.newXMLGregorianCalendar(time1)));
        bs.addBinding("location", vf.createURI("uri:loc_1"));
        VisibilityBindingSet expectedBs = new VisibilityBindingSet(bs);
        expected.add(expectedBs);
        
        Set<VisibilityBindingSet> results = readAllResults(pcjId);
        assertEquals(Sets.newHashSet(expected), results);
    }
    
    @Test
    public void nestedJoinOnAggregationWithGroupBy() throws Exception {
        String sparql = "select ?subject ?location ?lastObserved where {" //n
                + "?obs <uri:hasTime> ?lastObserved . " //n
                + "?obs <uri:hasLoc> ?location . " //n
                + "{ select ?subject (MAX(?time) as ?lastObserved) where {" // n
                + "?obs <uri:hasTime> ?time. "
                + "?obs <uri:hasSubject> ?subject } group by ?subject}}";

        // Create the Statements that will be loaded into Rya.
        final ValueFactory vf = new ValueFactoryImpl();
        final DatatypeFactory dtf = DatatypeFactory.newInstance();
        ZonedDateTime time = ZonedDateTime.now();

        ZonedDateTime zTime1 = time.minusMinutes(30);
        String time1 = zTime1.format(DateTimeFormatter.ISO_INSTANT);

        ZonedDateTime zTime2 = zTime1.minusMinutes(30);
        String time2 = zTime2.format(DateTimeFormatter.ISO_INSTANT);

        ZonedDateTime zTime3 = zTime2.minusMinutes(30);
        String time3 = zTime3.format(DateTimeFormatter.ISO_INSTANT);

        ZonedDateTime zTime4 = zTime3.minusMinutes(30);
        String time4 = zTime4.format(DateTimeFormatter.ISO_INSTANT);

        final Collection<Statement> statements = Sets.newHashSet(
                vf.createStatement(vf.createURI("urn:obs_1"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time1))),
                vf.createStatement(vf.createURI("urn:obs_1"), vf.createURI("uri:hasLoc"), vf.createURI("uri:loc_1")),
                vf.createStatement(vf.createURI("urn:obs_1"), vf.createURI("uri:hasSubject"), vf.createURI("uri:subject_1")),
                vf.createStatement(vf.createURI("urn:obs_2"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time2))),
                vf.createStatement(vf.createURI("urn:obs_2"), vf.createURI("uri:hasLoc"), vf.createURI("uri:loc_2")),
                vf.createStatement(vf.createURI("urn:obs_2"), vf.createURI("uri:hasSubject"), vf.createURI("uri:subject_1")),
                vf.createStatement(vf.createURI("urn:obs_3"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time3))),
                vf.createStatement(vf.createURI("urn:obs_3"), vf.createURI("uri:hasLoc"), vf.createURI("uri:loc_3")),
                vf.createStatement(vf.createURI("urn:obs_3"), vf.createURI("uri:hasSubject"), vf.createURI("uri:subject_1")),
                vf.createStatement(vf.createURI("urn:obs_4"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time4))),
                vf.createStatement(vf.createURI("urn:obs_4"), vf.createURI("uri:hasLoc"), vf.createURI("uri:loc_4")),
                vf.createStatement(vf.createURI("urn:obs_4"), vf.createURI("uri:hasSubject"), vf.createURI("uri:subject_1"))
                );

        String pcjId = loadDataAndCreateQuery(sparql, statements);
        
        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("lastObserved", vf.createLiteral(dtf.newXMLGregorianCalendar(time1)));
        bs.addBinding("location", vf.createURI("uri:loc_1"));
        bs.addBinding("subject", vf.createURI("uri:subject_1"));
        VisibilityBindingSet expected = new VisibilityBindingSet(bs);

        VisibilityBindingSet results = readLastResult(pcjId);
        assertEquals(expected, results);
    }
    
    
    @Test
    public void nestedFilterInPlaceOfJoinOnAggregationResult() throws Exception {
        String sparql = "select ?location ?lastObserved where {{" //n
                + "select (MAX(?time) as ?lastObserved) where {" // n
                + "?obs <uri:hasTime> ?time }} . " //n
                + "?obs <uri:hasTime> ?time . " //n
                + "Filter(?time = ?lastObserved)"
                + "?obs <uri:hasLoc> ?location . }"; // n

        // Create the Statements that will be loaded into Rya.
        final ValueFactory vf = new ValueFactoryImpl();
        final DatatypeFactory dtf = DatatypeFactory.newInstance();
        ZonedDateTime time = ZonedDateTime.now();

        ZonedDateTime zTime1 = time.minusMinutes(30);
        String time1 = zTime1.format(DateTimeFormatter.ISO_INSTANT);

        ZonedDateTime zTime2 = zTime1.minusMinutes(30);
        String time2 = zTime2.format(DateTimeFormatter.ISO_INSTANT);

        ZonedDateTime zTime3 = zTime2.minusMinutes(30);
        String time3 = zTime3.format(DateTimeFormatter.ISO_INSTANT);

        ZonedDateTime zTime4 = zTime3.minusMinutes(30);
        String time4 = zTime4.format(DateTimeFormatter.ISO_INSTANT);

        final Collection<Statement> statements = Sets.newHashSet(
                vf.createStatement(vf.createURI("urn:obs_1"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time1))),
                vf.createStatement(vf.createURI("urn:obs_1"), vf.createURI("uri:hasLoc"), vf.createURI("uri:loc_1")),
                vf.createStatement(vf.createURI("urn:obs_2"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time2))),
                vf.createStatement(vf.createURI("urn:obs_2"), vf.createURI("uri:hasLoc"), vf.createURI("uri:loc_2")),
                vf.createStatement(vf.createURI("urn:obs_3"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time3))),
                vf.createStatement(vf.createURI("urn:obs_3"), vf.createURI("uri:hasLoc"), vf.createURI("uri:loc_3")),
                vf.createStatement(vf.createURI("urn:obs_4"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time4))),
                vf.createStatement(vf.createURI("urn:obs_4"), vf.createURI("uri:hasLoc"), vf.createURI("uri:loc_4")));

        String pcjId = loadDataAndCreateQuery(sparql, statements);
        
        // Create the expected results of the SPARQL query once the PCJ has been computed.
        final Set<BindingSet> expectedResults = new HashSet<>();

        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("lastObserved", vf.createLiteral(dtf.newXMLGregorianCalendar(time1)));
        bs.addBinding("location", vf.createURI("uri:loc_1"));
        expectedResults.add(new VisibilityBindingSet(bs));

        final Set<VisibilityBindingSet> results = readGroupedResults(pcjId, new VariableOrder("location"));
        assertEquals(expectedResults, results);
    }



    private Set<VisibilityBindingSet> readAllResults(final String pcjId) throws Exception {
        requireNonNull(pcjId);

        // Read all of the results from the Kafka topic.
        final Set<VisibilityBindingSet> results = new HashSet<>();

        try(final KafkaConsumer<String, VisibilityBindingSet> consumer = makeConsumer(pcjId)) {
            final ConsumerRecords<String, VisibilityBindingSet> records = consumer.poll(5000);
            final Iterator<ConsumerRecord<String, VisibilityBindingSet>> recordIterator = records.iterator();
            while (recordIterator.hasNext()) {
                results.add( recordIterator.next().value() );
            }
        }

        return results;
    }

    private VisibilityBindingSet readLastResult(final String pcjId) throws Exception {
        requireNonNull(pcjId);

        // Read the results from the Kafka topic. The last one has the final aggregation result.
        VisibilityBindingSet result = null;

        try(final KafkaConsumer<String, VisibilityBindingSet> consumer = makeConsumer(pcjId)) {
            final ConsumerRecords<String, VisibilityBindingSet> records = consumer.poll(5000);
            final Iterator<ConsumerRecord<String, VisibilityBindingSet>> recordIterator = records.iterator();
            while (recordIterator.hasNext()) {
                result = recordIterator.next().value();
            }
        }

        return result;
    }

    private Set<VisibilityBindingSet> readGroupedResults(final String pcjId, final VariableOrder groupByVars) {
        requireNonNull(pcjId);

        // Read the results from the Kafka topic. The last one for each set of Group By values is an aggregation result.
        // The key in this map is a Binding Set containing only the group by variables.
        final Map<BindingSet, VisibilityBindingSet> results = new HashMap<>();

        try(final KafkaConsumer<String, VisibilityBindingSet> consumer = makeConsumer(pcjId)) {
            final ConsumerRecords<String, VisibilityBindingSet> records = consumer.poll(5000);
            final Iterator<ConsumerRecord<String, VisibilityBindingSet>> recordIterator = records.iterator();
            while (recordIterator.hasNext()) {
                final VisibilityBindingSet visBindingSet = recordIterator.next().value();

                final MapBindingSet key = new MapBindingSet();
                for(final String groupByBar : groupByVars) {
                    key.addBinding( visBindingSet.getBinding(groupByBar) );
                }

                results.put(key, visBindingSet);
            }
        }

        return Sets.newHashSet( results.values() );
    }
}