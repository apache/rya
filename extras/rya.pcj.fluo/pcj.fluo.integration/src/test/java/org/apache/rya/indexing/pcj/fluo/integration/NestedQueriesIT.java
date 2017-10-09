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
public class NestedQueriesIT extends KafkaExportITBase {

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