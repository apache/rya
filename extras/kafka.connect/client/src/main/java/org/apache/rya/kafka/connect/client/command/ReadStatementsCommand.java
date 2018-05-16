/**
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
package org.apache.rya.kafka.connect.client.command;

import static java.util.Objects.requireNonNull;

import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.rya.kafka.connect.api.StatementsDeserializer;
import org.apache.rya.kafka.connect.client.RyaKafkaClientCommand;
import org.eclipse.rdf4j.model.Statement;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Reads {@link Statement}s from a Kafka topic using the Rya Kafka Connect Sink format.
 */
@DefaultAnnotation(NonNull.class)
public class ReadStatementsCommand implements RyaKafkaClientCommand {

    @Override
    public String getCommand() {
        return "read";
    }

    @Override
    public String getDescription() {
        return "Reads Statements from the specified Kafka topic.";
    }

    @Override
    public boolean validArguments(final String[] args) {
        boolean valid = true;
        try {
            new JCommander(new KafkaParameters(), args);
        } catch(final ParameterException e) {
            valid = false;
        }
        return valid;
    }

    @Override
    public void execute(final String[] args) throws ArgumentsException, ExecutionException {
        requireNonNull(args);

        // Parse the command line arguments.
        final KafkaParameters params = new KafkaParameters();
        try {
            new JCommander(params, args);
        } catch(final ParameterException e) {
            throw new ArgumentsException("Could not read the Statements from the topic because of invalid command line parameters.", e);
        }

        // Set up the consumer.
        try(KafkaConsumer<String, Set<Statement>> consumer = makeConsumer(params)) {
            // Subscribe to the configured topic.
            consumer.subscribe(Collections.singleton(params.topic));

            // Read the statements and write them to output.
            for(final ConsumerRecord<String, Set<Statement>> record : consumer.poll(500)) {
                for(final Statement stmt: record.value()) {
                    System.out.println( stmt );
                }
            }
        }
    }

    private KafkaConsumer<String, Set<Statement>> makeConsumer(final KafkaParameters params) {
        requireNonNull(params);

        // Configure which instance of Kafka to connect to.
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, params.bootstrapServers);

        // Nothing meaningful is in the key and the values is a Set<BindingSet> object.
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StatementsDeserializer.class);

        // Use a UUID for the Group Id so that we never register as part of the same group as another consumer.
        final String groupId = UUID.randomUUID().toString();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // Set a client id so that server side logging can be traced.
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "Kafka-Connect-Client-" + groupId);

        // These consumers always start at the beginning and move forwards until the caller is finished with
        // the returned stream, so never commit the consumer's progress.
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        return new KafkaConsumer<>(props);
    }
}