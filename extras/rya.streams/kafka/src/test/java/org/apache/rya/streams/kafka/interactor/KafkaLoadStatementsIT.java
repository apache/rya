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
package org.apache.rya.streams.kafka.interactor;

import static org.junit.Assert.assertEquals;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.rya.api.model.VisibilityStatement;
import org.apache.rya.streams.kafka.serialization.VisibilityStatementDeserializer;
import org.apache.rya.streams.kafka.serialization.VisibilityStatementSerializer;
import org.apache.rya.test.kafka.KafkaTestInstanceRule;
import org.apache.rya.test.kafka.KafkaTestUtil;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.UnsupportedRDFormatException;
import org.junit.Rule;
import org.junit.Test;

/**
 * Integration tests the {@link KafkaLoadStatements} command
 */
public class KafkaLoadStatementsIT {
    private static final Path TURTLE_FILE = Paths.get("src/test/resources/statements.ttl");

    private static final Path INVALID = Paths.get("src/test/resources/invalid.INVALID");

    @Rule
    public KafkaTestInstanceRule rule = new KafkaTestInstanceRule(true);

    @Test(expected = UnsupportedRDFormatException.class)
    public void test_invalidFile() throws Exception {
        try(final Producer<?, VisibilityStatement> producer =
                KafkaTestUtil.makeProducer(rule, StringSerializer.class, VisibilityStatementSerializer.class)) {
            final KafkaLoadStatements command = new KafkaLoadStatements(rule.getKafkaTopicName(), producer);
            command.fromFile(INVALID, "a|b|c");
        }
    }

    @Test
    public void testTurtle() throws Exception {
        final String visibilities = "a|b|c";

        // Load the statements into the kafka topic.
        try(final Producer<?, VisibilityStatement> producer =
                KafkaTestUtil.makeProducer(rule, StringSerializer.class, VisibilityStatementSerializer.class)) {
            final KafkaLoadStatements command = new KafkaLoadStatements(rule.getKafkaTopicName(), producer);
            command.fromFile(TURTLE_FILE, visibilities);
        }

        // Read a VisibilityBindingSets from the test topic.
        final List<VisibilityStatement> read;// = new ArrayList<>();
        try(Consumer<String, VisibilityStatement> consumer =
                KafkaTestUtil.fromStartConsumer(rule, StringDeserializer.class, VisibilityStatementDeserializer.class)) {
            consumer.subscribe(Arrays.asList(rule.getKafkaTopicName()));
            read = KafkaTestUtil.pollForResults(500, 6, 3, consumer);
        }

        final List<VisibilityStatement> original = new ArrayList<>();
        final ValueFactory vf = SimpleValueFactory.getInstance();

        original.add(new VisibilityStatement(
                vf.createStatement(vf.createIRI("http://example#alice"), vf.createIRI("http://example#talksTo"), vf.createIRI("http://example#bob")),
                visibilities));
        original.add(new VisibilityStatement(
                vf.createStatement(vf.createIRI("http://example#bob"), vf.createIRI("http://example#talksTo"), vf.createIRI("http://example#charlie")),
                visibilities));
        original.add(new VisibilityStatement(
                vf.createStatement(vf.createIRI("http://example#charlie"), vf.createIRI("http://example#likes"), vf.createIRI("http://example#icecream")),
                visibilities));
        // Show the written statement matches the read one.
        assertEquals(original, read);
    }
}