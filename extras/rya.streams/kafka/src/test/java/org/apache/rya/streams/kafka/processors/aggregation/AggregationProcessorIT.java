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
package org.apache.rya.streams.kafka.processors.aggregation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.rya.api.function.projection.RandomUUIDFactory;
import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.api.model.VisibilityStatement;
import org.apache.rya.streams.kafka.KafkaTopics;
import org.apache.rya.streams.kafka.RyaStreamsTestUtil;
import org.apache.rya.streams.kafka.processors.aggregation.AggregationProcessorSupplier.AggregationProcessor;
import org.apache.rya.streams.kafka.serialization.VisibilityBindingSetDeserializer;
import org.apache.rya.streams.kafka.topology.TopologyFactory;
import org.apache.rya.test.kafka.KafkaTestInstanceRule;
import org.junit.Rule;
import org.junit.Test;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.impl.MapBindingSet;

/**
 * Integration tests {@link AggregationProcessor}.
 */
public class AggregationProcessorIT {

    @Rule
    public KafkaTestInstanceRule kafka = new KafkaTestInstanceRule(false);

    @Test
    public void count() throws Exception {
        // A query that figures out how many books each person has.
        final String sparql =
                "SELECT ?person (count(?book) as ?bookCount) " +
                "WHERE { " +
                    "?person <urn:hasBook> ?book " +
                "} GROUP BY ?person";

        // Create the statements that will be input into the query..
        final ValueFactory vf = new ValueFactoryImpl();
        final List<VisibilityStatement> statements = new ArrayList<>();
        statements.add(new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:Alice"), vf.createURI("urn:hasBook"), vf.createLiteral("Book 1")), "a"));
        statements.add(new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:Bob"), vf.createURI("urn:hasBook"), vf.createLiteral("Book 1")), ""));
        statements.add(new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:Alice"), vf.createURI("urn:hasBook"), vf.createLiteral("Book 2")), "b"));

        // Make the expected results.
        final Set<VisibilityBindingSet> expected = new HashSet<>();
        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("person", vf.createURI("urn:Alice"));
        bs.addBinding("bookCount", vf.createLiteral("1", XMLSchema.INTEGER));
        expected.add(new VisibilityBindingSet(bs, "a"));

        bs = new MapBindingSet();
        bs.addBinding("person", vf.createURI("urn:Bob"));
        bs.addBinding("bookCount", vf.createLiteral("1", XMLSchema.INTEGER));
        expected.add(new VisibilityBindingSet(bs, ""));

        bs = new MapBindingSet();
        bs.addBinding("person", vf.createURI("urn:Alice"));
        bs.addBinding("bookCount", vf.createLiteral("2", XMLSchema.INTEGER));
        expected.add(new VisibilityBindingSet(bs, "a&b"));

        // Enumerate some topics that will be re-used
        final String ryaInstance = UUID.randomUUID().toString();
        final UUID queryId = UUID.randomUUID();
        final String statementsTopic = KafkaTopics.statementsTopic(ryaInstance);
        final String resultsTopic = KafkaTopics.queryResultsTopic(queryId);

        // Setup a topology.
        final TopologyBuilder builder = new TopologyFactory().build(sparql, statementsTopic, resultsTopic, new RandomUUIDFactory());

        // Run the test.
        RyaStreamsTestUtil.runStreamProcessingTest(kafka, statementsTopic, resultsTopic, builder, 2000, statements, expected, VisibilityBindingSetDeserializer.class);
    }

    @Test
    public void sum() throws Exception {
        // A query that figures out how much food each person has.
        final String sparql =
                "SELECT ?person (sum(?foodCount) as ?totalFood) " +
                "WHERE { " +
                    "?person <urn:hasFoodType> ?food . " +
                    "?food <urn:count> ?foodCount . " +
                "} GROUP BY ?person";

        // Create the statements that will be input into the query..
        final ValueFactory vf = new ValueFactoryImpl();
        final List<VisibilityStatement> statements = new ArrayList<>();
        statements.add(new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:Alice"), vf.createURI("urn:hasFoodType"), vf.createURI("urn:corn")), ""));
        statements.add(new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:Alice"), vf.createURI("urn:hasFoodType"), vf.createURI("urn:apple")), ""));
        statements.add(new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:corn"), vf.createURI("urn:count"), vf.createLiteral(4)), ""));
        statements.add(new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:apple"), vf.createURI("urn:count"), vf.createLiteral(3)), ""));

        // Make the expected results.
        final Set<VisibilityBindingSet> expected = new HashSet<>();
        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("person", vf.createURI("urn:Alice"));
        bs.addBinding("totalFood", vf.createLiteral("4", XMLSchema.INTEGER));
        expected.add(new VisibilityBindingSet(bs, ""));

        bs = new MapBindingSet();
        bs.addBinding("person", vf.createURI("urn:Alice"));
        bs.addBinding("totalFood", vf.createLiteral("7", XMLSchema.INTEGER));
        expected.add(new VisibilityBindingSet(bs, ""));

        // Enumerate some topics that will be re-used
        final String ryaInstance = UUID.randomUUID().toString();
        final UUID queryId = UUID.randomUUID();
        final String statementsTopic = KafkaTopics.statementsTopic(ryaInstance);
        final String resultsTopic = KafkaTopics.queryResultsTopic(queryId);

        // Setup a topology.
        final TopologyBuilder builder = new TopologyFactory().build(sparql, statementsTopic, resultsTopic, new RandomUUIDFactory());

        // Run the test.
        RyaStreamsTestUtil.runStreamProcessingTest(kafka, statementsTopic, resultsTopic, builder, 2000, statements, expected, VisibilityBindingSetDeserializer.class);
    }

    @Test
    public void average() throws Exception {
        // A query that figures out the average age across all people.
        final String sparql =
                "SELECT (avg(?age) as ?avgAge) " +
                "WHERE { " +
                    "?person <urn:age> ?age " +
                "}";

        // Create the statements that will be input into the query..
        final ValueFactory vf = new ValueFactoryImpl();
        final List<VisibilityStatement> statements = new ArrayList<>();
        statements.add(new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:Alice"), vf.createURI("urn:age"), vf.createLiteral(3)), ""));
        statements.add(new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:Bob"), vf.createURI("urn:age"), vf.createLiteral(7)), ""));
        statements.add(new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:Charlie"), vf.createURI("urn:age"), vf.createLiteral(2)), ""));

        // Make the expected results.
        final Set<VisibilityBindingSet> expected = new HashSet<>();
        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("avgAge", vf.createLiteral("3", XMLSchema.DECIMAL));
        expected.add(new VisibilityBindingSet(bs, ""));

        bs = new MapBindingSet();
        bs.addBinding("avgAge", vf.createLiteral("5", XMLSchema.DECIMAL));
        expected.add(new VisibilityBindingSet(bs, ""));

        bs = new MapBindingSet();
        bs.addBinding("avgAge", vf.createLiteral("4", XMLSchema.DECIMAL));
        expected.add(new VisibilityBindingSet(bs, ""));

        // Enumerate some topics that will be re-used
        final String ryaInstance = UUID.randomUUID().toString();
        final UUID queryId = UUID.randomUUID();
        final String statementsTopic = KafkaTopics.statementsTopic(ryaInstance);
        final String resultsTopic = KafkaTopics.queryResultsTopic(queryId);

        // Setup a topology.
        final TopologyBuilder builder = new TopologyFactory().build(sparql, statementsTopic, resultsTopic, new RandomUUIDFactory());

        // Run the test.
        RyaStreamsTestUtil.runStreamProcessingTest(kafka, statementsTopic, resultsTopic, builder, 2000, statements, expected, VisibilityBindingSetDeserializer.class);
    }

    @Test
    public void min() throws Exception {
        // A query that figures out what the youngest age is across all people.
        final String sparql =
                "SELECT (min(?age) as ?youngest) " +
                "WHERE { " +
                    "?person <urn:age> ?age " +
                "}";

        // Create the statements that will be input into the query..
        final ValueFactory vf = new ValueFactoryImpl();
        final List<VisibilityStatement> statements = new ArrayList<>();
        statements.add(new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:Alice"), vf.createURI("urn:age"), vf.createLiteral(13)), ""));
        statements.add(new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:Bob"), vf.createURI("urn:age"), vf.createLiteral(14)), ""));
        statements.add(new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:Charlie"), vf.createURI("urn:age"), vf.createLiteral(7)), ""));
        statements.add(new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:David"), vf.createURI("urn:age"), vf.createLiteral(5)), ""));
        statements.add(new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:Eve"), vf.createURI("urn:age"), vf.createLiteral(25)), ""));

        // Make the expected results.
        final Set<VisibilityBindingSet> expected = new HashSet<>();
        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("youngest", vf.createLiteral(13));
        expected.add(new VisibilityBindingSet(bs, ""));

        bs = new MapBindingSet();
        bs.addBinding("youngest", vf.createLiteral(7));
        expected.add(new VisibilityBindingSet(bs, ""));

        bs = new MapBindingSet();
        bs.addBinding("youngest", vf.createLiteral(5));
        expected.add(new VisibilityBindingSet(bs, ""));

        // Enumerate some topics that will be re-used
        final String ryaInstance = UUID.randomUUID().toString();
        final UUID queryId = UUID.randomUUID();
        final String statementsTopic = KafkaTopics.statementsTopic(ryaInstance);
        final String resultsTopic = KafkaTopics.queryResultsTopic(queryId);

        // Setup a topology.
        final TopologyBuilder builder = new TopologyFactory().build(sparql, statementsTopic, resultsTopic, new RandomUUIDFactory());

        // Run the test.
        RyaStreamsTestUtil.runStreamProcessingTest(kafka, statementsTopic, resultsTopic, builder, 2000, statements, expected, VisibilityBindingSetDeserializer.class);
    }

    @Test
    public void max() throws Exception {
        // A query that figures out what the oldest age is across all people.
        final String sparql =
                "SELECT (max(?age) as ?oldest) " +
                "WHERE { " +
                    "?person <urn:age> ?age " +
                "}";

        // Create the statements that will be input into the query..
        final ValueFactory vf = new ValueFactoryImpl();
        final List<VisibilityStatement> statements = new ArrayList<>();
        statements.add(new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:Alice"), vf.createURI("urn:age"), vf.createLiteral(13)), ""));
        statements.add(new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:Bob"), vf.createURI("urn:age"), vf.createLiteral(14)), ""));
        statements.add(new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:Charlie"), vf.createURI("urn:age"), vf.createLiteral(7)), ""));
        statements.add(new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:David"), vf.createURI("urn:age"), vf.createLiteral(5)), ""));
        statements.add(new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:Eve"), vf.createURI("urn:age"), vf.createLiteral(25)), ""));

        // Make the expected results.
        final Set<VisibilityBindingSet> expected = new HashSet<>();
        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("oldest", vf.createLiteral(13));
        expected.add(new VisibilityBindingSet(bs, ""));

        bs = new MapBindingSet();
        bs.addBinding("oldest", vf.createLiteral(14));
        expected.add(new VisibilityBindingSet(bs, ""));

        bs = new MapBindingSet();
        bs.addBinding("oldest", vf.createLiteral(25));
        expected.add(new VisibilityBindingSet(bs, ""));

        // Enumerate some topics that will be re-used
        final String ryaInstance = UUID.randomUUID().toString();
        final UUID queryId = UUID.randomUUID();
        final String statementsTopic = KafkaTopics.statementsTopic(ryaInstance);
        final String resultsTopic = KafkaTopics.queryResultsTopic(queryId);

        // Setup a topology.
        final TopologyBuilder builder = new TopologyFactory().build(sparql, statementsTopic, resultsTopic, new RandomUUIDFactory());

        // Run the test.
        RyaStreamsTestUtil.runStreamProcessingTest(kafka, statementsTopic, resultsTopic, builder, 2000, statements, expected, VisibilityBindingSetDeserializer.class);
    }

    @Test
    public void multipleGroupByVars() throws Exception {
        // A query that contains more than one group by variable.
        final String sparql =
                "SELECT ?business ?employee (sum(?hours) AS ?totalHours) " +
                "WHERE {" +
                    "?employee <urn:worksAt> ?business . " +
                    "?business <urn:hasTimecardId> ?timecardId . " +
                    "?employee <urn:hasTimecardId> ?timecardId . " +
                    "?timecardId <urn:hours> ?hours . " +
                "} GROUP BY ?business ?employee";

        // Create the statements that will be input into the query.
        final ValueFactory vf = new ValueFactoryImpl();
        final List<VisibilityStatement> statements = new ArrayList<>();
        statements.add(new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:Alice"), vf.createURI("urn:worksAt"), vf.createURI("urn:TacoJoint")), ""));
        statements.add(new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:TacoJoint"), vf.createURI("urn:hasTimecardId"), vf.createURI("urn:timecard1")), ""));
        statements.add(new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:Alice"), vf.createURI("urn:hasTimecardId"), vf.createURI("urn:timecard1")), ""));
        statements.add(new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:timecard1"), vf.createURI("urn:hours"), vf.createLiteral(40)), ""));

        statements.add(new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:TacoJoint"), vf.createURI("urn:hasTimecardId"), vf.createURI("urn:timecard2")), ""));
        statements.add(new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:Alice"), vf.createURI("urn:hasTimecardId"), vf.createURI("urn:timecard2")), ""));
        statements.add(new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:timecard2"), vf.createURI("urn:hours"), vf.createLiteral(25)), ""));

        statements.add(new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:Bob"), vf.createURI("urn:worksAt"), vf.createURI("urn:TacoJoint")), ""));
        statements.add(new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:TacoJoint"), vf.createURI("urn:hasTimecardId"), vf.createURI("urn:timecard3")), ""));
        statements.add(new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:Bob"), vf.createURI("urn:hasTimecardId"), vf.createURI("urn:timecard3")), ""));
        statements.add(new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:timecard3"), vf.createURI("urn:hours"), vf.createLiteral(28)), ""));

        statements.add(new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:TacoJoint"), vf.createURI("urn:hasTimecardId"), vf.createURI("urn:timecard4")), ""));
        statements.add(new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:Bob"), vf.createURI("urn:hasTimecardId"), vf.createURI("urn:timecard4")), ""));
        statements.add(new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:timecard4"), vf.createURI("urn:hours"), vf.createLiteral(28)), ""));

        statements.add(new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:Alice"), vf.createURI("urn:worksAt"), vf.createURI("urn:CoffeeShop")), ""));
        statements.add(new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:CoffeeShop"), vf.createURI("urn:hasTimecardId"), vf.createURI("urn:timecard5")), ""));
        statements.add(new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:Alice"), vf.createURI("urn:hasTimecardId"), vf.createURI("urn:timecard5")), ""));
        statements.add(new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:timecard5"), vf.createURI("urn:hours"), vf.createLiteral(12)), ""));

        // Make the expected results.
        final Set<VisibilityBindingSet> expected = new HashSet<>();
        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("business", vf.createURI("urn:TacoJoint"));
        bs.addBinding("employee", vf.createURI("urn:Alice"));
        bs.addBinding("totalHours", vf.createLiteral("40", XMLSchema.INTEGER));
        expected.add(new VisibilityBindingSet(bs, ""));

        bs = new MapBindingSet();
        bs.addBinding("business", vf.createURI("urn:TacoJoint"));
        bs.addBinding("employee", vf.createURI("urn:Alice"));
        bs.addBinding("totalHours", vf.createLiteral("65", XMLSchema.INTEGER));
        expected.add(new VisibilityBindingSet(bs, ""));

        bs = new MapBindingSet();
        bs.addBinding("business", vf.createURI("urn:TacoJoint"));
        bs.addBinding("employee", vf.createURI("urn:Bob"));
        bs.addBinding("totalHours", vf.createLiteral("28", XMLSchema.INTEGER));
        expected.add(new VisibilityBindingSet(bs, ""));

        bs = new MapBindingSet();
        bs.addBinding("business", vf.createURI("urn:TacoJoint"));
        bs.addBinding("employee", vf.createURI("urn:Bob"));
        bs.addBinding("totalHours", vf.createLiteral("56", XMLSchema.INTEGER));
        expected.add(new VisibilityBindingSet(bs, ""));

        bs = new MapBindingSet();
        bs.addBinding("business", vf.createURI("urn:CoffeeShop"));
        bs.addBinding("employee", vf.createURI("urn:Alice"));
        bs.addBinding("totalHours", vf.createLiteral("12", XMLSchema.INTEGER));
        expected.add(new VisibilityBindingSet(bs, ""));

        // Enumerate some topics that will be re-used
        final String ryaInstance = UUID.randomUUID().toString();
        final UUID queryId = UUID.randomUUID();
        final String statementsTopic = KafkaTopics.statementsTopic(ryaInstance);
        final String resultsTopic = KafkaTopics.queryResultsTopic(queryId);

        // Setup a topology.
        final TopologyBuilder builder = new TopologyFactory().build(sparql, statementsTopic, resultsTopic, new RandomUUIDFactory());

        // Run the test.
        RyaStreamsTestUtil.runStreamProcessingTest(kafka, statementsTopic, resultsTopic, builder, 4000, statements, expected, VisibilityBindingSetDeserializer.class);
    }

    @Test
    public void multipleAggregations() throws Exception {
        // A query that figures out what the youngest and oldest ages are across all people.
        final String sparql =
                "SELECT (min(?age) as ?youngest) (max(?age) as ?oldest) " +
                "WHERE { " +
                    "?person <urn:age> ?age " +
                "}";

        // Create the statements that will be input into the query..
        final ValueFactory vf = new ValueFactoryImpl();
        final List<VisibilityStatement> statements = new ArrayList<>();
        statements.add(new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:Alice"), vf.createURI("urn:age"), vf.createLiteral(13)), ""));
        statements.add(new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:Bob"), vf.createURI("urn:age"), vf.createLiteral(14)), ""));
        statements.add(new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:Charlie"), vf.createURI("urn:age"), vf.createLiteral(7)), ""));
        statements.add(new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:David"), vf.createURI("urn:age"), vf.createLiteral(5)), ""));
        statements.add(new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:Eve"), vf.createURI("urn:age"), vf.createLiteral(25)), ""));

        // Make the expected results.
        final Set<VisibilityBindingSet> expected = new HashSet<>();
        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("youngest", vf.createLiteral(13));
        bs.addBinding("oldest", vf.createLiteral(13));
        expected.add(new VisibilityBindingSet(bs, ""));

        bs = new MapBindingSet();
        bs.addBinding("youngest", vf.createLiteral(13));
        bs.addBinding("oldest", vf.createLiteral(14));
        expected.add(new VisibilityBindingSet(bs, ""));

        bs = new MapBindingSet();
        bs.addBinding("youngest", vf.createLiteral(7));
        bs.addBinding("oldest", vf.createLiteral(14));
        expected.add(new VisibilityBindingSet(bs, ""));

        bs = new MapBindingSet();
        bs.addBinding("youngest", vf.createLiteral(5));
        bs.addBinding("oldest", vf.createLiteral(14));
        expected.add(new VisibilityBindingSet(bs, ""));

        bs = new MapBindingSet();
        bs.addBinding("youngest", vf.createLiteral(5));
        bs.addBinding("oldest", vf.createLiteral(25));
        expected.add(new VisibilityBindingSet(bs, ""));

        // Enumerate some topics that will be re-used
        final String ryaInstance = UUID.randomUUID().toString();
        final UUID queryId = UUID.randomUUID();
        final String statementsTopic = KafkaTopics.statementsTopic(ryaInstance);
        final String resultsTopic = KafkaTopics.queryResultsTopic(queryId);

        // Setup a topology.
        final TopologyBuilder builder = new TopologyFactory().build(sparql, statementsTopic, resultsTopic, new RandomUUIDFactory());

        // Run the test.
        RyaStreamsTestUtil.runStreamProcessingTest(kafka, statementsTopic, resultsTopic, builder, 2000, statements, expected, VisibilityBindingSetDeserializer.class);
    }
}