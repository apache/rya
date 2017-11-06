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

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.api.model.VisibilityStatement;
import org.apache.rya.streams.kafka.KafkaTestUtil;
import org.apache.rya.streams.kafka.KafkaTopics;
import org.apache.rya.streams.kafka.RdfTestUtil;
import org.apache.rya.streams.kafka.interactor.KafkaLoadStatements;
import org.apache.rya.streams.kafka.processors.ProcessorResult.UnaryResult;
import org.apache.rya.streams.kafka.processors.RyaStreamsSinkFormatterSupplier.RyaStreamsSinkFormatter;
import org.apache.rya.streams.kafka.processors.StatementPatternProcessorSupplier.StatementPatternProcessor;
import org.apache.rya.streams.kafka.serialization.VisibilityBindingSetDeserializer;
import org.apache.rya.streams.kafka.serialization.VisibilityBindingSetSerializer;
import org.apache.rya.streams.kafka.serialization.VisibilityStatementDeserializer;
import org.apache.rya.streams.kafka.serialization.VisibilityStatementSerializer;
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
    public void statementPatternMatches() throws Exception {
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
        builder.addProcessor("SINK_FORMATTER", RyaStreamsSinkFormatter::new, "SP1");

        // Add a sink that writes the data out to a new Kafka topic.
        builder.addSink("QUERY_RESULTS", resultsTopic, new StringSerializer(), new VisibilityBindingSetSerializer(), "SINK_FORMATTER");

        // Start the streams program.
        final Properties props = kafka.createBootstrapServerConfig();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "StatementPatternProcessorIT");

        final KafkaStreams streams = new KafkaStreams(builder, new StreamsConfig(props));
        streams.cleanUp();
        try {
            streams.start();

            // Wait for the streams application to start. Streams only see data after their consumers are connected.
            Thread.sleep(2000);

            // Load some data into the input topic.
            final ValueFactory vf = new ValueFactoryImpl();
            final List<VisibilityStatement> statements = new ArrayList<>();
            statements.add( new VisibilityStatement(vf.createStatement(vf.createURI("urn:Alice"), vf.createURI("urn:talksTo"), vf.createURI("urn:Bob")), "a") );

            try(Producer<String, VisibilityStatement> producer = KafkaTestUtil.makeProducer(
                    kafka, StringSerializer.class, VisibilityStatementSerializer.class)) {
                new KafkaLoadStatements(statementsTopic, producer).fromCollection(statements);
            }

            // Wait for the final results to appear in the output topic and verify the expected Binding Set was found.
            try(Consumer<String, VisibilityBindingSet> consumer = KafkaTestUtil.fromStartConsumer(
                    kafka, StringDeserializer.class, VisibilityBindingSetDeserializer.class)) {
                // Register the topic.
                consumer.subscribe(Arrays.asList(resultsTopic));

                // Poll for the result.
                final List<VisibilityBindingSet> results = KafkaTestUtil.pollForResults(500, 6, 1, consumer);

                // Show the correct binding set results from the job.
                final QueryBindingSet bs = new QueryBindingSet();
                bs.addBinding("person", vf.createURI("urn:Alice"));
                bs.addBinding("otherPerson", vf.createURI("urn:Bob"));
                final VisibilityBindingSet expected = new VisibilityBindingSet(bs, "a");

                final VisibilityBindingSet result = results.iterator().next();
                assertEquals(expected, result);
            }
        } finally {
            streams.close();
        }
    }
}