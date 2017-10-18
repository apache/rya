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
package org.apache.rya.streams.client.command;

import static org.junit.Assert.assertEquals;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.rya.api.model.VisibilityStatement;
import org.apache.rya.streams.kafka.KafkaTopics;
import org.apache.rya.streams.kafka.serialization.VisibilityStatementDeserializer;
import org.apache.rya.test.kafka.KafkaTestInstanceRule;
import org.apache.rya.test.kafka.KafkaTestUtil;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.Rule;
import org.junit.Test;

/**
 * Integration tests the methods of {@link LoadStatementsCommand}.
 */
public class LoadStatementsCommandIT {
    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    private static final Path TURTLE_FILE = Paths.get("src/test/resources/statements.ttl");

    private final String ryaInstance = UUID.randomUUID().toString();

    @Rule
    public KafkaTestInstanceRule kafka = new KafkaTestInstanceRule(true);

    @Test
    public void shortParams() throws Exception {
        // Load a file of statements into Kafka.
        final String visibilities = "a|b|c";
        final String[] args = new String[] {
                "-r", "" + ryaInstance,
                "-i", kafka.getKafkaHostname(),
                "-p", kafka.getKafkaPort(),
                "-f", TURTLE_FILE.toString(),
                "-v", visibilities
        };

        // Load the file of statements into the Statements topic.
        new LoadStatementsCommand().execute(args);

        // Show that the statements were loaded into the topic.
        final List<VisibilityStatement> read = new ArrayList<>();

        try(final Consumer<String, VisibilityStatement> consumer =
                KafkaTestUtil.fromStartConsumer(kafka, StringDeserializer.class, VisibilityStatementDeserializer.class)) {
            // Subscribe for messages.
            consumer.subscribe( Arrays.asList(KafkaTopics.statementsTopic(ryaInstance)) );

            // Read the messages and extract their values.
            final Iterator<ConsumerRecord<String, VisibilityStatement>> iter = consumer.poll(3000).iterator();
            while(iter.hasNext()) {
                read.add( iter.next().value() );
            }
        }

        final List<VisibilityStatement> expected = new ArrayList<>();
        expected.add(new VisibilityStatement(
                VF.createStatement(VF.createIRI("http://example#alice"), VF.createIRI("http://example#talksTo"), VF.createIRI("http://example#bob")),
                visibilities));
        expected.add(new VisibilityStatement(
                VF.createStatement(VF.createIRI("http://example#bob"), VF.createIRI("http://example#talksTo"), VF.createIRI("http://example#charlie")),
                visibilities));
        expected.add(new VisibilityStatement(
                VF.createStatement(VF.createIRI("http://example#charlie"), VF.createIRI("http://example#likes"), VF.createIRI("http://example#icecream")),
                visibilities));

        // Show the written statements matches the read ones.
        assertEquals(expected, read);
    }

    @Test
    public void longParams() throws Exception {
        // Load a file of statements into Kafka.
        final String visibilities = "a|b|c";
        final String[] args = new String[] {
                "--ryaInstance", "" + ryaInstance,
                "--kafkaHostname", kafka.getKafkaHostname(),
                "--kafkaPort", kafka.getKafkaPort(),
                "--statementsFile", TURTLE_FILE.toString(),
                "--visibilities", visibilities
        };

        // Load the file of statements into the Statements topic.
        new LoadStatementsCommand().execute(args);

        // Show that the statements were loaded into the topic.
        final List<VisibilityStatement> read = new ArrayList<>();

        try(final Consumer<String, VisibilityStatement> consumer =
                KafkaTestUtil.fromStartConsumer(kafka, StringDeserializer.class, VisibilityStatementDeserializer.class)) {
            // Subscribe for messages.
            consumer.subscribe( Arrays.asList(KafkaTopics.statementsTopic(ryaInstance)) );

            // Read the messages and extract their values.
            final Iterator<ConsumerRecord<String, VisibilityStatement>> iter = consumer.poll(3000).iterator();
            while(iter.hasNext()) {
                read.add( iter.next().value() );
            }
        }

        final List<VisibilityStatement> expected = new ArrayList<>();
        expected.add(new VisibilityStatement(
                VF.createStatement(VF.createIRI("http://example#alice"), VF.createIRI("http://example#talksTo"), VF.createIRI("http://example#bob")),
                visibilities));
        expected.add(new VisibilityStatement(
                VF.createStatement(VF.createIRI("http://example#bob"), VF.createIRI("http://example#talksTo"), VF.createIRI("http://example#charlie")),
                visibilities));
        expected.add(new VisibilityStatement(
                VF.createStatement(VF.createIRI("http://example#charlie"), VF.createIRI("http://example#likes"), VF.createIRI("http://example#icecream")),
                visibilities));

        // Show the written statements matches the read ones.
        assertEquals(expected, read);
    }
}