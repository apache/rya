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
package org.apache.rya.streams.kafka.processors.sp;

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
import org.apache.rya.streams.kafka.processors.StatementPatternProcessorSupplier;
import org.apache.rya.streams.kafka.processors.StatementPatternProcessorSupplier.StatementPatternProcessor;
import org.apache.rya.streams.kafka.serialization.VisibilityBindingSetDeserializer;
import org.apache.rya.streams.kafka.topology.TopologyFactory;
import org.apache.rya.test.kafka.KafkaTestInstanceRule;
import org.junit.Rule;
import org.junit.Test;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;

/**
 * Integration tests the methods of {@link StatementPatternProcessor}.
 */
public class StatementPatternProcessorIT {

    @Rule
    public KafkaTestInstanceRule kafka = new KafkaTestInstanceRule(true);

    @Test
    public void singlePattern_singleStatement() throws Exception {
        // Enumerate some topics that will be re-used
        final String ryaInstance = UUID.randomUUID().toString();
        final UUID queryId = UUID.randomUUID();
        final String statementsTopic = KafkaTopics.statementsTopic(ryaInstance);
        final String resultsTopic = KafkaTopics.queryResultsTopic(queryId);

        // Setup a topology.
        final String query = "SELECT * WHERE { ?person <urn:talksTo> ?otherPerson }";
        final TopologyFactory factory = new TopologyFactory();
        final TopologyBuilder builder = factory.build(query, statementsTopic, resultsTopic, new RandomUUIDFactory());

        // Create a statement that generate an SP result.
        final ValueFactory vf = new ValueFactoryImpl();
        final List<VisibilityStatement> statements = new ArrayList<>();
        statements.add( new VisibilityStatement(vf.createStatement(vf.createURI("urn:Alice"), vf.createURI("urn:talksTo"), vf.createURI("urn:Bob")), "a") );

        // Show the correct binding set results from the job.
        final Set<VisibilityBindingSet> expected = new HashSet<>();

        final QueryBindingSet bs = new QueryBindingSet();
        bs.addBinding("person", vf.createURI("urn:Alice"));
        bs.addBinding("otherPerson", vf.createURI("urn:Bob"));
        expected.add( new VisibilityBindingSet(bs, "a") );

        // Run the test.
        RyaStreamsTestUtil.runStreamProcessingTest(kafka, statementsTopic, resultsTopic, builder, statements, expected, VisibilityBindingSetDeserializer.class);
    }

    @Test
    public void singlePattern_manyStatements() throws Exception {
        // Enumerate some topics that will be re-used
        final String ryaInstance = UUID.randomUUID().toString();
        final UUID queryId = UUID.randomUUID();
        final String statementsTopic = KafkaTopics.statementsTopic(ryaInstance);
        final String resultsTopic = KafkaTopics.queryResultsTopic(queryId);

        // Setup a topology.
        final String query = "SELECT * WHERE { ?person <urn:talksTo> ?otherPerson }";
        final TopologyFactory factory = new TopologyFactory();
        final TopologyBuilder builder = factory.build(query, statementsTopic, resultsTopic, new RandomUUIDFactory());

        // Create some statements where some generates SP results and others do not.
        final ValueFactory vf = new ValueFactoryImpl();
        final List<VisibilityStatement> statements = new ArrayList<>();
        statements.add( new VisibilityStatement(vf.createStatement(vf.createURI("urn:Alice"), vf.createURI("urn:talksTo"), vf.createURI("urn:Bob")), "a") );
        statements.add( new VisibilityStatement(vf.createStatement(vf.createURI("urn:Alice"), vf.createURI("urn:worksAt"), vf.createURI("urn:TacoJoin")), "b") );
        statements.add( new VisibilityStatement(vf.createStatement(vf.createURI("urn:Bob"), vf.createURI("urn:talksTo"), vf.createURI("urn:Alice")), "a|b") );
        statements.add( new VisibilityStatement(vf.createStatement(vf.createURI("urn:Bob"), vf.createURI("urn:worksAt"), vf.createURI("urn:BurgerJoint")), "c") );

        // Show the correct binding set results from the job.
        final Set<VisibilityBindingSet> expected = new HashSet<>();

        QueryBindingSet bs = new QueryBindingSet();
        bs.addBinding("person", vf.createURI("urn:Alice"));
        bs.addBinding("otherPerson", vf.createURI("urn:Bob"));
        expected.add( new VisibilityBindingSet(bs, "a") );

        bs = new QueryBindingSet();
        bs.addBinding("person", vf.createURI("urn:Bob"));
        bs.addBinding("otherPerson", vf.createURI("urn:Alice"));
        expected.add( new VisibilityBindingSet(bs, "a|b") );

        // Run the test.
        RyaStreamsTestUtil.runStreamProcessingTest(kafka, statementsTopic, resultsTopic, builder, statements, expected, VisibilityBindingSetDeserializer.class);
    }

    @Test
    public void multiplePatterns_singleStatement() throws Exception {
        // Enumerate some topics that will be re-used
        final String ryaInstance = UUID.randomUUID().toString();
        final UUID queryId = UUID.randomUUID();
        final String statementsTopic = KafkaTopics.statementsTopic(ryaInstance);
        final String resultsTopic = KafkaTopics.queryResultsTopic(queryId);

        // Setup a topology.
        final String query = "SELECT * WHERE { "
                + "?person <urn:talksTo> ?otherPerson . "
                + "?person ?action <urn:Bob>"
                + "}";
        final TopologyFactory factory = new TopologyFactory();
        final TopologyBuilder builder = factory.build(query, statementsTopic, resultsTopic, new RandomUUIDFactory());

        // Create some statements where some generates SP results and others do not.
        final ValueFactory vf = new ValueFactoryImpl();
        final List<VisibilityStatement> statements = new ArrayList<>();
        statements.add( new VisibilityStatement(vf.createStatement(vf.createURI("urn:Alice"), vf.createURI("urn:talksTo"), vf.createURI("urn:Bob")), "a") );

        // Show the correct binding set results from the job.
        final Set<VisibilityBindingSet> expected = new HashSet<>();

        final QueryBindingSet bs = new QueryBindingSet();
        bs.addBinding("person", vf.createURI("urn:Alice"));
        bs.addBinding("action", vf.createURI("urn:talksTo"));
        bs.addBinding("otherPerson", vf.createURI("urn:Bob"));
        expected.add( new VisibilityBindingSet(bs, "a") );

        // Run the test.
        RyaStreamsTestUtil.runStreamProcessingTest(kafka, statementsTopic, resultsTopic, builder, statements, expected, VisibilityBindingSetDeserializer.class);
    }

    @Test
    public void multiplePatterns_multipleStatements() throws Exception {
        // Enumerate some topics that will be re-used
        final String ryaInstance = UUID.randomUUID().toString();
        final UUID queryId = UUID.randomUUID();
        final String statementsTopic = KafkaTopics.statementsTopic(ryaInstance);
        final String resultsTopic = KafkaTopics.queryResultsTopic(queryId);

        // Setup a topology.
        final String query = "SELECT * WHERE { "
                + "?person <urn:talksTo> ?otherPerson ."
                + "?person ?action <urn:Bob>"
                + "}";
        final TopologyFactory factory = new TopologyFactory();
        final TopologyBuilder builder = factory.build(query, statementsTopic, resultsTopic, new RandomUUIDFactory());

        // Create some statements where some generates SP results and others do not.
        final ValueFactory vf = new ValueFactoryImpl();
        final List<VisibilityStatement> statements = new ArrayList<>();
        statements.add( new VisibilityStatement(vf.createStatement(vf.createURI("urn:Alice"), vf.createURI("urn:talksTo"), vf.createURI("urn:Bob")), "a") );
        statements.add( new VisibilityStatement(vf.createStatement(vf.createURI("urn:Alice"), vf.createURI("urn:talksTo"), vf.createURI("urn:Charlie")), "a|b") );
        statements.add( new VisibilityStatement(vf.createStatement(vf.createURI("urn:Charlie"), vf.createURI("urn:walksWith"), vf.createURI("urn:Bob")), "b") );

        // Show the correct binding set results from the job.
        final Set<VisibilityBindingSet> expected = new HashSet<>();

        QueryBindingSet bs = new QueryBindingSet();
        bs = new QueryBindingSet();
        bs.addBinding("person", vf.createURI("urn:Alice"));
        bs.addBinding("action", vf.createURI("urn:talksTo"));
        bs.addBinding("otherPerson", vf.createURI("urn:Charlie"));
        expected.add(new VisibilityBindingSet(bs, "a&(a|b)"));

        bs = new QueryBindingSet();
        bs.addBinding("person", vf.createURI("urn:Alice"));
        bs.addBinding("action", vf.createURI("urn:talksTo"));
        bs.addBinding("otherPerson", vf.createURI("urn:Bob"));
        expected.add(new VisibilityBindingSet(bs, "a"));

        // Run the test.
        RyaStreamsTestUtil.runStreamProcessingTest(kafka, statementsTopic, resultsTopic, builder, statements, expected, VisibilityBindingSetDeserializer.class);
    }
}