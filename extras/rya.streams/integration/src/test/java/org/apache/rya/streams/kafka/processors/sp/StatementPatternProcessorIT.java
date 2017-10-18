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
import org.apache.rya.streams.kafka.processors.StatementPatternProcessorSupplier.StatementPatternProcessor;
import org.apache.rya.streams.kafka.serialization.VisibilityBindingSetDeserializer;
import org.apache.rya.streams.kafka.topology.TopologyFactory;
import org.apache.rya.test.kafka.KafkaTestInstanceRule;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.junit.Rule;
import org.junit.Test;

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
        final String resultsTopic = KafkaTopics.queryResultsTopic(ryaInstance, queryId);

        // Setup a topology.
        final String query = "SELECT * WHERE { ?person <urn:talksTo> ?otherPerson }";
        final TopologyFactory factory = new TopologyFactory();
        final TopologyBuilder builder = factory.build(query, statementsTopic, resultsTopic, new RandomUUIDFactory());

        // Create a statement that generate an SP result.
        final ValueFactory vf = SimpleValueFactory.getInstance();
        final List<VisibilityStatement> statements = new ArrayList<>();
        statements.add( new VisibilityStatement(vf.createStatement(vf.createIRI("urn:Alice"), vf.createIRI("urn:talksTo"), vf.createIRI("urn:Bob")), "a") );

        // Show the correct binding set results from the job.
        final Set<VisibilityBindingSet> expected = new HashSet<>();

        final QueryBindingSet bs = new QueryBindingSet();
        bs.addBinding("person", vf.createIRI("urn:Alice"));
        bs.addBinding("otherPerson", vf.createIRI("urn:Bob"));
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
        final String resultsTopic = KafkaTopics.queryResultsTopic(ryaInstance, queryId);

        // Setup a topology.
        final String query = "SELECT * WHERE { ?person <urn:talksTo> ?otherPerson }";
        final TopologyFactory factory = new TopologyFactory();
        final TopologyBuilder builder = factory.build(query, statementsTopic, resultsTopic, new RandomUUIDFactory());

        // Create some statements where some generates SP results and others do not.
        final ValueFactory vf = SimpleValueFactory.getInstance();
        final List<VisibilityStatement> statements = new ArrayList<>();
        statements.add( new VisibilityStatement(vf.createStatement(vf.createIRI("urn:Alice"), vf.createIRI("urn:talksTo"), vf.createIRI("urn:Bob")), "a") );
        statements.add( new VisibilityStatement(vf.createStatement(vf.createIRI("urn:Alice"), vf.createIRI("urn:worksAt"), vf.createIRI("urn:TacoJoin")), "b") );
        statements.add( new VisibilityStatement(vf.createStatement(vf.createIRI("urn:Bob"), vf.createIRI("urn:talksTo"), vf.createIRI("urn:Alice")), "a|b") );
        statements.add( new VisibilityStatement(vf.createStatement(vf.createIRI("urn:Bob"), vf.createIRI("urn:worksAt"), vf.createIRI("urn:BurgerJoint")), "c") );

        // Show the correct binding set results from the job.
        final Set<VisibilityBindingSet> expected = new HashSet<>();

        QueryBindingSet bs = new QueryBindingSet();
        bs.addBinding("person", vf.createIRI("urn:Alice"));
        bs.addBinding("otherPerson", vf.createIRI("urn:Bob"));
        expected.add( new VisibilityBindingSet(bs, "a") );

        bs = new QueryBindingSet();
        bs.addBinding("person", vf.createIRI("urn:Bob"));
        bs.addBinding("otherPerson", vf.createIRI("urn:Alice"));
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
        final String resultsTopic = KafkaTopics.queryResultsTopic(ryaInstance, queryId);

        // Setup a topology.
        final String query = "SELECT * WHERE { "
                + "?person <urn:talksTo> ?otherPerson . "
                + "?person ?action <urn:Bob>"
                + "}";
        final TopologyFactory factory = new TopologyFactory();
        final TopologyBuilder builder = factory.build(query, statementsTopic, resultsTopic, new RandomUUIDFactory());

        // Create some statements where some generates SP results and others do not.
        final ValueFactory vf = SimpleValueFactory.getInstance();
        final List<VisibilityStatement> statements = new ArrayList<>();
        statements.add( new VisibilityStatement(vf.createStatement(vf.createIRI("urn:Alice"), vf.createIRI("urn:talksTo"), vf.createIRI("urn:Bob")), "a") );

        // Show the correct binding set results from the job.
        final Set<VisibilityBindingSet> expected = new HashSet<>();

        final QueryBindingSet bs = new QueryBindingSet();
        bs.addBinding("person", vf.createIRI("urn:Alice"));
        bs.addBinding("action", vf.createIRI("urn:talksTo"));
        bs.addBinding("otherPerson", vf.createIRI("urn:Bob"));
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
        final String resultsTopic = KafkaTopics.queryResultsTopic(ryaInstance, queryId);

        // Setup a topology.
        final String query = "SELECT * WHERE { "
                + "?person <urn:talksTo> ?otherPerson ."
                + "?person ?action <urn:Bob>"
                + "}";
        final TopologyFactory factory = new TopologyFactory();
        final TopologyBuilder builder = factory.build(query, statementsTopic, resultsTopic, new RandomUUIDFactory());

        // Create some statements where some generates SP results and others do not.
        final ValueFactory vf = SimpleValueFactory.getInstance();
        final List<VisibilityStatement> statements = new ArrayList<>();
        statements.add( new VisibilityStatement(vf.createStatement(vf.createIRI("urn:Alice"), vf.createIRI("urn:talksTo"), vf.createIRI("urn:Bob")), "a") );
        statements.add( new VisibilityStatement(vf.createStatement(vf.createIRI("urn:Alice"), vf.createIRI("urn:talksTo"), vf.createIRI("urn:Charlie")), "a|b") );
        statements.add( new VisibilityStatement(vf.createStatement(vf.createIRI("urn:Charlie"), vf.createIRI("urn:walksWith"), vf.createIRI("urn:Bob")), "b") );

        // Show the correct binding set results from the job.
        final Set<VisibilityBindingSet> expected = new HashSet<>();

        QueryBindingSet bs = new QueryBindingSet();
        bs = new QueryBindingSet();
        bs.addBinding("person", vf.createIRI("urn:Alice"));
        bs.addBinding("action", vf.createIRI("urn:talksTo"));
        bs.addBinding("otherPerson", vf.createIRI("urn:Charlie"));
        expected.add(new VisibilityBindingSet(bs, "a&(a|b)"));

        bs = new QueryBindingSet();
        bs.addBinding("person", vf.createIRI("urn:Alice"));
        bs.addBinding("action", vf.createIRI("urn:talksTo"));
        bs.addBinding("otherPerson", vf.createIRI("urn:Bob"));
        expected.add(new VisibilityBindingSet(bs, "a"));

        // Run the test.
        RyaStreamsTestUtil.runStreamProcessingTest(kafka, statementsTopic, resultsTopic, builder, statements, expected, VisibilityBindingSetDeserializer.class);
    }
}