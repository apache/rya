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
package org.apache.rya.streams.kafka.processors.join;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.rya.api.function.join.NaturalJoin;
import org.apache.rya.api.function.projection.RandomUUIDFactory;
import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.api.model.VisibilityStatement;
import org.apache.rya.streams.kafka.KafkaTestUtil;
import org.apache.rya.streams.kafka.KafkaTopics;
import org.apache.rya.streams.kafka.processors.ProcessorResult;
import org.apache.rya.streams.kafka.processors.ProcessorResult.UnaryResult;
import org.apache.rya.streams.kafka.processors.join.JoinProcessorSupplier.JoinProcessor;
import org.apache.rya.streams.kafka.serialization.VisibilityBindingSetDeserializer;
import org.apache.rya.streams.kafka.topology.TopologyFactory;
import org.apache.rya.test.kafka.KafkaTestInstanceRule;
import org.junit.Rule;
import org.junit.Test;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.impl.MapBindingSet;

import com.google.common.collect.Lists;

/**
 * Integration tests the methods of {@link JoinProcessor}.
 */
public class JoinProcessorIT {

    @Rule
    public KafkaTestInstanceRule kafka = new KafkaTestInstanceRule(true);

    @Test(expected = IllegalArgumentException.class)
    public void badAllVars() throws IllegalArgumentException {
        new JoinProcessorSupplier(
                "NATURAL_JOIN",
                new NaturalJoin(),
                Lists.newArrayList("employee"),
                Lists.newArrayList("person", "employee", "business"),
                result -> ProcessorResult.make( new UnaryResult(result) ));
    }

    @Test
    public void newLeftResult() throws Exception {
        // Enumerate some topics that will be re-used
        final String ryaInstance = UUID.randomUUID().toString();
        final UUID queryId = UUID.randomUUID();
        final String statementsTopic = KafkaTopics.statementsTopic(ryaInstance);
        final String resultsTopic = KafkaTopics.queryResultsTopic(queryId);

        // Setup a topology.
        final String query =
                "SELECT * WHERE { " +
                    "?person <urn:talksTo> ?employee ." +
                    "?employee <urn:worksAt> ?business" +
                " }";
        final TopologyFactory factory = new TopologyFactory();
        final TopologyBuilder builder = factory.build(query, statementsTopic, resultsTopic, new RandomUUIDFactory());

        // Create some statements that generate a bunch of right SP results.
        final ValueFactory vf = new ValueFactoryImpl();
        final List<VisibilityStatement> statements = new ArrayList<>();
        statements.add( new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:Bob"), vf.createURI("urn:worksAt"), vf.createURI("urn:TacoPlace")), "a&b") );
        statements.add( new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:Charlie"), vf.createURI("urn:worksAt"), vf.createURI("urn:BurgerJoint")), "a") );
        statements.add( new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:Eve"), vf.createURI("urn:worksAt"), vf.createURI("urn:CoffeeShop")), "b") );
        statements.add( new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:Bob"), vf.createURI("urn:worksAt"), vf.createURI("urn:BurgerJoint")), "b|c") );

        // Add a statement that will generate a left result that joins with some of those right results.
        statements.add( new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:Alice"), vf.createURI("urn:talksTo"), vf.createURI("urn:Bob")), "c") );

        // Make the expected results.
        final Set<VisibilityBindingSet> expected = new HashSet<>();
        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("person", vf.createURI("urn:Alice"));
        bs.addBinding("employee", vf.createURI("urn:Bob"));
        bs.addBinding("business", vf.createURI("urn:TacoPlace"));
        expected.add( new VisibilityBindingSet(bs, "a&b&c") );

        bs = new MapBindingSet();
        bs.addBinding("person", vf.createURI("urn:Alice"));
        bs.addBinding("employee", vf.createURI("urn:Bob"));
        bs.addBinding("business", vf.createURI("urn:BurgerJoint"));
        expected.add( new VisibilityBindingSet(bs, "c&(b|c)") );

        // Run the test.
        KafkaTestUtil.runStreamProcessingTest(kafka, statementsTopic, resultsTopic, builder, 2000, statements, expected, VisibilityBindingSetDeserializer.class);
    }

    @Test
    public void newRightResult() throws Exception {
        // Enumerate some topics that will be re-used
        final String ryaInstance = UUID.randomUUID().toString();
        final UUID queryId = UUID.randomUUID();
        final String statementsTopic = KafkaTopics.statementsTopic(ryaInstance);
        final String resultsTopic = KafkaTopics.queryResultsTopic(queryId);

        // Setup a topology.
        final String query =
                "SELECT * WHERE { " +
                    "?person <urn:talksTo> ?employee ." +
                    "?employee <urn:worksAt> ?business" +
                " }";
        final TopologyFactory factory = new TopologyFactory();
        final TopologyBuilder builder = factory.build(query, statementsTopic, resultsTopic, new RandomUUIDFactory());

        // Create some statements that generate a bunch of right SP results.
        final ValueFactory vf = new ValueFactoryImpl();
        final List<VisibilityStatement> statements = new ArrayList<>();
        statements.add( new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:Bob"), vf.createURI("urn:worksAt"), vf.createURI("urn:TacoPlace")), "a&b") );
        statements.add( new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:Charlie"), vf.createURI("urn:worksAt"), vf.createURI("urn:BurgerJoint")), "a") );
        statements.add( new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:Eve"), vf.createURI("urn:worksAt"), vf.createURI("urn:CoffeeShop")), "b") );
        statements.add( new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:Bob"), vf.createURI("urn:worksAt"), vf.createURI("urn:BurgerJoint")), "b|c") );

        // Add a statement that will generate a left result that joins with some of those right results.
        statements.add( new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:Alice"), vf.createURI("urn:talksTo"), vf.createURI("urn:Bob")), "c") );

        // Make the expected results.
        final Set<VisibilityBindingSet> expected = new HashSet<>();
        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("person", vf.createURI("urn:Alice"));
        bs.addBinding("employee", vf.createURI("urn:Bob"));
        bs.addBinding("business", vf.createURI("urn:TacoPlace"));
        expected.add( new VisibilityBindingSet(bs, "a&b&c") );

        bs = new MapBindingSet();
        bs.addBinding("person", vf.createURI("urn:Alice"));
        bs.addBinding("employee", vf.createURI("urn:Bob"));
        bs.addBinding("business", vf.createURI("urn:BurgerJoint"));
        expected.add( new VisibilityBindingSet(bs, "c&(b|c)") );

        // Run the test.
        KafkaTestUtil.runStreamProcessingTest(kafka, statementsTopic, resultsTopic, builder, 2000, statements, expected, VisibilityBindingSetDeserializer.class);
    }

    @Test
    public void newResultsBothSides() throws Exception {
        // Enumerate some topics that will be re-used
        final String ryaInstance = UUID.randomUUID().toString();
        final UUID queryId = UUID.randomUUID();
        final String statementsTopic = KafkaTopics.statementsTopic(ryaInstance);
        final String resultsTopic = KafkaTopics.queryResultsTopic(queryId);

        // Setup a topology.
        final String query =
                "SELECT * WHERE { " +
                    "?person <urn:talksTo> ?employee ." +
                    "?employee <urn:worksAt> ?business" +
                " }";
        final TopologyFactory factory = new TopologyFactory();
        final TopologyBuilder builder = factory.build(query, statementsTopic, resultsTopic, new RandomUUIDFactory());

        // Create some statements that generate a bunch of right SP results.
        final ValueFactory vf = new ValueFactoryImpl();
        final List<VisibilityStatement> statements = new ArrayList<>();
        statements.add( new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:Bob"), vf.createURI("urn:worksAt"), vf.createURI("urn:TacoPlace")), "a&b") );
        statements.add( new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:Alice"), vf.createURI("urn:talksTo"), vf.createURI("urn:Bob")), "c") );
        statements.add( new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:Charlie"), vf.createURI("urn:worksAt"), vf.createURI("urn:BurgerJoint")), "a") );
        statements.add( new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:Eve"), vf.createURI("urn:worksAt"), vf.createURI("urn:CoffeeShop")), "b") );
        statements.add( new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:Bob"), vf.createURI("urn:worksAt"), vf.createURI("urn:BurgerJoint")), "b|c") );
        statements.add( new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:Bob"), vf.createURI("urn:talksTo"), vf.createURI("urn:Charlie")), "c") );

        // Make the expected results.
        final Set<VisibilityBindingSet> expected = new HashSet<>();
        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("person", vf.createURI("urn:Alice"));
        bs.addBinding("employee", vf.createURI("urn:Bob"));
        bs.addBinding("business", vf.createURI("urn:TacoPlace"));
        expected.add( new VisibilityBindingSet(bs, "a&b&c") );

        bs = new MapBindingSet();
        bs.addBinding("person", vf.createURI("urn:Alice"));
        bs.addBinding("employee", vf.createURI("urn:Bob"));
        bs.addBinding("business", vf.createURI("urn:BurgerJoint"));
        expected.add( new VisibilityBindingSet(bs, "c&(b|c)") );

        bs = new MapBindingSet();
        bs.addBinding("person", vf.createURI("urn:Bob"));
        bs.addBinding("employee", vf.createURI("urn:Charlie"));
        bs.addBinding("business", vf.createURI("urn:BurgerJoint"));
        expected.add( new VisibilityBindingSet(bs, "a&c") );

        // Run the test.
        KafkaTestUtil.runStreamProcessingTest(kafka, statementsTopic, resultsTopic, builder, 2000, statements, expected, VisibilityBindingSetDeserializer.class);
    }

    @Test
    public void manyJoins() throws Exception {
        // Enumerate some topics that will be re-used
        final String ryaInstance = UUID.randomUUID().toString();
        final UUID queryId = UUID.randomUUID();
        final String statementsTopic = KafkaTopics.statementsTopic(ryaInstance);
        final String resultsTopic = KafkaTopics.queryResultsTopic(queryId);

        // Setup a topology.
        final String query =
                "SELECT * WHERE { " +
                    "?person <urn:talksTo> ?employee ." +
                    "?employee <urn:worksAt> ?business ." +
                    "?employee <urn:hourlyWage> ?wage ." +
                " }";
        final TopologyFactory factory = new TopologyFactory();
        final TopologyBuilder builder = factory.build(query, statementsTopic, resultsTopic, new RandomUUIDFactory());

        // Create some statements that generate a bunch of right SP results.
        final ValueFactory vf = new ValueFactoryImpl();
        final List<VisibilityStatement> statements = new ArrayList<>();
        statements.add( new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:Alice"), vf.createURI("urn:talksTo"), vf.createURI("urn:Bob")), "a") );
        statements.add( new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:Bob"), vf.createURI("urn:worksAt"), vf.createURI("urn:BurgerJoint")), "a") );
        statements.add( new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:Bob"), vf.createURI("urn:hourlyWage"), vf.createLiteral(7.25)), "a") );

        // Make the expected results.
        final Set<VisibilityBindingSet> expected = new HashSet<>();
        final MapBindingSet bs = new MapBindingSet();
        bs.addBinding("person", vf.createURI("urn:Alice"));
        bs.addBinding("employee", vf.createURI("urn:Bob"));
        bs.addBinding("business", vf.createURI("urn:BurgerJoint"));
        bs.addBinding("wage", vf.createLiteral(7.25));
        expected.add( new VisibilityBindingSet(bs, "a") );

        // Run the test.
        KafkaTestUtil.runStreamProcessingTest(kafka, statementsTopic, resultsTopic, builder, 3000, statements, expected, VisibilityBindingSetDeserializer.class);
    }

    @Test
    public void leftJoin() throws Exception {
        // Enumerate some topics that will be re-used
        final String ryaInstance = UUID.randomUUID().toString();
        final UUID queryId = UUID.randomUUID();
        final String statementsTopic = KafkaTopics.statementsTopic(ryaInstance);
        final String resultsTopic = KafkaTopics.queryResultsTopic(queryId);

        // Setup a topology.
        final String query =
                "SELECT * WHERE { " +
                    "?person <urn:talksTo> ?employee ." +
                    "OPTIONAL{ ?employee <urn:worksAt> ?business } " +
                " }";
        final TopologyFactory factory = new TopologyFactory();
        final TopologyBuilder builder = factory.build(query, statementsTopic, resultsTopic, new RandomUUIDFactory());

        // Create some statements that generate a result that includes the optional value as well as one that does not.
        final ValueFactory vf = new ValueFactoryImpl();
        final List<VisibilityStatement> statements = new ArrayList<>();
        statements.add( new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:Alice"), vf.createURI("urn:talksTo"), vf.createURI("urn:Bob")), "a") );
        statements.add( new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:Bob"), vf.createURI("urn:worksAt"), vf.createURI("urn:TacoPlace")), "b") );
        statements.add( new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:Bob"), vf.createURI("urn:talksTo"), vf.createURI("urn:Charlie")), "c") );
        statements.add( new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:David"), vf.createURI("urn:worksAt"), vf.createURI("urn:BurgerJoint")), "d") );

        // Make the expected results.
        final Set<VisibilityBindingSet> expected = new HashSet<>();
        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("person", vf.createURI("urn:Alice"));
        bs.addBinding("employee", vf.createURI("urn:Bob"));
        expected.add( new VisibilityBindingSet(bs, "a") );

        bs = new MapBindingSet();
        bs.addBinding("person", vf.createURI("urn:Alice"));
        bs.addBinding("employee", vf.createURI("urn:Bob"));
        bs.addBinding("business", vf.createURI("urn:TacoPlace"));
        expected.add( new VisibilityBindingSet(bs, "a&b") );

        bs = new MapBindingSet();
        bs.addBinding("person", vf.createURI("urn:Bob"));
        bs.addBinding("employee", vf.createURI("urn:Charlie"));
        expected.add( new VisibilityBindingSet(bs, "c") );

        // Run the test.
        KafkaTestUtil.runStreamProcessingTest(kafka, statementsTopic, resultsTopic, builder, 2000, statements, expected, VisibilityBindingSetDeserializer.class);
    }
}