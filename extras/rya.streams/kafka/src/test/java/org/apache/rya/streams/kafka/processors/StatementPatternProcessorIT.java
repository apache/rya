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
package org.apache.rya.streams.kafka.processors;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.api.model.VisibilityStatement;
import org.apache.rya.streams.kafka.KafkaTestUtil;
import org.apache.rya.streams.kafka.KafkaTopics;
import org.apache.rya.streams.kafka.RdfTestUtil;
import org.apache.rya.streams.kafka.processors.ProcessorResult.UnaryResult;
import org.apache.rya.streams.kafka.processors.StatementPatternProcessorSupplier.StatementPatternProcessor;
import org.apache.rya.streams.kafka.processors.output.BindingSetOutputFormatterSupplier.BindingSetOutputFormatter;
import org.apache.rya.streams.kafka.serialization.VisibilityBindingSetSerializer;
import org.apache.rya.streams.kafka.serialization.VisibilityStatementDeserializer;
import org.apache.rya.test.kafka.KafkaTestInstanceRule;
import org.junit.Rule;
import org.junit.Test;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.algebra.StatementPattern;
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

        // Get the StatementPattern object that will be evaluated.
        final StatementPattern sp = RdfTestUtil.getSp("SELECT * WHERE { ?person <urn:talksTo> ?otherPerson }");

        // Setup a topology.
        final TopologyBuilder builder = new TopologyBuilder();

        // The topic that Statements are written to is used as a source.
        builder.addSource("STATEMENTS", new StringDeserializer(), new VisibilityStatementDeserializer(), statementsTopic);

        // Add a processor that handles the first statement pattern.
        builder.addProcessor("SP1", new StatementPatternProcessorSupplier(sp, result -> ProcessorResult.make( new UnaryResult(result) )), "STATEMENTS");

        // Add a processor that formats the VisibilityBindingSet for output.
        builder.addProcessor("SINK_FORMATTER", BindingSetOutputFormatter::new, "SP1");

        // Add a sink that writes the data out to a new Kafka topic.
        builder.addSink("QUERY_RESULTS", resultsTopic, new StringSerializer(), new VisibilityBindingSetSerializer(), "SINK_FORMATTER");

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
        KafkaTestUtil.runStreamProcessingTest(kafka, statementsTopic, resultsTopic, builder, 2000, statements, expected);
    }

    @Test
    public void singlePattern_manyStatements() throws Exception {
        // Enumerate some topics that will be re-used
        final String ryaInstance = UUID.randomUUID().toString();
        final UUID queryId = UUID.randomUUID();
        final String statementsTopic = KafkaTopics.statementsTopic(ryaInstance);
        final String resultsTopic = KafkaTopics.queryResultsTopic(queryId);

        // Get the StatementPattern object that will be evaluated.
        final StatementPattern sp = RdfTestUtil.getSp("SELECT * WHERE { ?person <urn:talksTo> ?otherPerson }");

        // Setup a topology.
        final TopologyBuilder builder = new TopologyBuilder();

        // The topic that Statements are written to is used as a source.
        builder.addSource("STATEMENTS", new StringDeserializer(), new VisibilityStatementDeserializer(), statementsTopic);

        // Add a processor that handles the first statement pattern.
        builder.addProcessor("SP1", new StatementPatternProcessorSupplier(sp, result -> ProcessorResult.make( new UnaryResult(result) )), "STATEMENTS");

        // Add a processor that formats the VisibilityBindingSet for output.
        builder.addProcessor("SINK_FORMATTER", BindingSetOutputFormatter::new, "SP1");

        // Add a sink that writes the data out to a new Kafka topic.
        builder.addSink("QUERY_RESULTS", resultsTopic, new StringSerializer(), new VisibilityBindingSetSerializer(), "SINK_FORMATTER");

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
        KafkaTestUtil.runStreamProcessingTest(kafka, statementsTopic, resultsTopic, builder, 2000, statements, expected);
    }

    @Test
    public void multiplePatterns_singleStatement() throws Exception {
        // Enumerate some topics that will be re-used
        final String ryaInstance = UUID.randomUUID().toString();
        final UUID queryId = UUID.randomUUID();
        final String statementsTopic = KafkaTopics.statementsTopic(ryaInstance);
        final String resultsTopic = KafkaTopics.queryResultsTopic(queryId);

        // Get the StatementPattern object that will be evaluated.
        final StatementPattern sp1 = RdfTestUtil.getSp("SELECT * WHERE { ?person <urn:talksTo> ?otherPerson }");
        final StatementPattern sp2 = RdfTestUtil.getSp("SELECT * WHERE { ?person ?action <urn:Bob> }");

        // Setup a topology.
        final TopologyBuilder builder = new TopologyBuilder();

        // The topic that Statements are written to is used as a source.
        builder.addSource("STATEMENTS", new StringDeserializer(), new VisibilityStatementDeserializer(), statementsTopic);

        // Add a processor that handles the first statement pattern.
        builder.addProcessor("SP1", new StatementPatternProcessorSupplier(sp1, result -> ProcessorResult.make( new UnaryResult(result) )), "STATEMENTS");

        // Add a processor that handles the second statement pattern.
        builder.addProcessor("SP2", new StatementPatternProcessorSupplier(sp2, result -> ProcessorResult.make( new UnaryResult(result) )), "STATEMENTS");

        // Add a processor that formats the VisibilityBindingSet for output.
        builder.addProcessor("SINK_FORMATTER", BindingSetOutputFormatter::new, "SP1", "SP2");

        // Add a sink that writes the data out to a new Kafka topic.
        builder.addSink("QUERY_RESULTS", resultsTopic, new StringSerializer(), new VisibilityBindingSetSerializer(), "SINK_FORMATTER");

        // Create some statements where some generates SP results and others do not.
        final ValueFactory vf = new ValueFactoryImpl();
        final List<VisibilityStatement> statements = new ArrayList<>();
        statements.add( new VisibilityStatement(vf.createStatement(vf.createURI("urn:Alice"), vf.createURI("urn:talksTo"), vf.createURI("urn:Bob")), "a") );

        // Show the correct binding set results from the job.
        final Set<VisibilityBindingSet> expected = new HashSet<>();

        QueryBindingSet bs = new QueryBindingSet();
        bs.addBinding("person", vf.createURI("urn:Alice"));
        bs.addBinding("otherPerson", vf.createURI("urn:Bob"));
        expected.add( new VisibilityBindingSet(bs, "a") );

        bs = new QueryBindingSet();
        bs.addBinding("person", vf.createURI("urn:Alice"));
        bs.addBinding("action", vf.createURI("urn:talksTo"));
        expected.add( new VisibilityBindingSet(bs, "a") );

        // Run the test.
        KafkaTestUtil.runStreamProcessingTest(kafka, statementsTopic, resultsTopic, builder, 2000, statements, expected);
    }

    @Test
    public void multiplePatterns_multipleStatements() throws Exception {
        // Enumerate some topics that will be re-used
        final String ryaInstance = UUID.randomUUID().toString();
        final UUID queryId = UUID.randomUUID();
        final String statementsTopic = KafkaTopics.statementsTopic(ryaInstance);
        final String resultsTopic = KafkaTopics.queryResultsTopic(queryId);

        // Get the StatementPattern object that will be evaluated.
        final StatementPattern sp1 = RdfTestUtil.getSp("SELECT * WHERE { ?person <urn:talksTo> ?otherPerson }");
        final StatementPattern sp2 = RdfTestUtil.getSp("SELECT * WHERE { ?person ?action <urn:Bob> }");

        // Setup a topology.
        final TopologyBuilder builder = new TopologyBuilder();

        // The topic that Statements are written to is used as a source.
        builder.addSource("STATEMENTS", new StringDeserializer(), new VisibilityStatementDeserializer(), statementsTopic);

        // Add a processor that handles the first statement pattern.
        builder.addProcessor("SP1", new StatementPatternProcessorSupplier(sp1, result -> ProcessorResult.make( new UnaryResult(result) )), "STATEMENTS");

        // Add a processor that handles the second statement pattern.
        builder.addProcessor("SP2", new StatementPatternProcessorSupplier(sp2, result -> ProcessorResult.make( new UnaryResult(result) )), "STATEMENTS");

        // Add a processor that formats the VisibilityBindingSet for output.
        builder.addProcessor("SINK_FORMATTER", BindingSetOutputFormatter::new, "SP1", "SP2");

        // Add a sink that writes the data out to a new Kafka topic.
        builder.addSink("QUERY_RESULTS", resultsTopic, new StringSerializer(), new VisibilityBindingSetSerializer(), "SINK_FORMATTER");

        // Create some statements where some generates SP results and others do not.
        final ValueFactory vf = new ValueFactoryImpl();
        final List<VisibilityStatement> statements = new ArrayList<>();
        statements.add( new VisibilityStatement(vf.createStatement(vf.createURI("urn:Alice"), vf.createURI("urn:talksTo"), vf.createURI("urn:Bob")), "a") );
        statements.add( new VisibilityStatement(vf.createStatement(vf.createURI("urn:Alice"), vf.createURI("urn:talksTo"), vf.createURI("urn:Charlie")), "a|b") );
        statements.add( new VisibilityStatement(vf.createStatement(vf.createURI("urn:Charlie"), vf.createURI("urn:walksWith"), vf.createURI("urn:Bob")), "b") );

        // Show the correct binding set results from the job.
        final Set<VisibilityBindingSet> expected = new HashSet<>();

        QueryBindingSet bs = new QueryBindingSet();
        bs.addBinding("person", vf.createURI("urn:Alice"));
        bs.addBinding("otherPerson", vf.createURI("urn:Bob"));
        expected.add( new VisibilityBindingSet(bs, "a") );

        bs = new QueryBindingSet();
        bs.addBinding("person", vf.createURI("urn:Alice"));
        bs.addBinding("action", vf.createURI("urn:talksTo"));
        expected.add( new VisibilityBindingSet(bs, "a") );

        bs = new QueryBindingSet();
        bs.addBinding("person", vf.createURI("urn:Alice"));
        bs.addBinding("otherPerson", vf.createURI("urn:Charlie"));
        expected.add( new VisibilityBindingSet(bs, "a|b") );

        bs = new QueryBindingSet();
        bs.addBinding("person", vf.createURI("urn:Charlie"));
        bs.addBinding("action", vf.createURI("urn:walksWith"));
        expected.add( new VisibilityBindingSet(bs, "b") );

        // Run the test.
        KafkaTestUtil.runStreamProcessingTest(kafka, statementsTopic, resultsTopic, builder, 2000, statements, expected);
    }
}