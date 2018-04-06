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
import static org.junit.Assert.fail;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.rya.api.function.projection.RandomUUIDFactory;
import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.streams.api.entity.StreamsQuery;
import org.apache.rya.streams.api.queries.InMemoryQueryRepository;
import org.apache.rya.streams.api.queries.QueryChange;
import org.apache.rya.streams.api.queries.QueryChangeLog;
import org.apache.rya.streams.api.queries.QueryRepository;
import org.apache.rya.streams.kafka.KafkaTopics;
import org.apache.rya.streams.kafka.queries.KafkaQueryChangeLog;
import org.apache.rya.streams.kafka.serialization.VisibilityBindingSetDeserializer;
import org.apache.rya.streams.kafka.serialization.queries.QueryChangeDeserializer;
import org.apache.rya.streams.kafka.serialization.queries.QueryChangeSerializer;
import org.apache.rya.streams.kafka.topology.TopologyFactory;
import org.apache.rya.test.kafka.KafkaTestInstanceRule;
import org.apache.rya.test.kafka.KafkaTestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractScheduledService.Scheduler;

/**
 * Integration Test for adding a new query through a command, loading data from
 * a file, and then starting up the streams program.
 */
public class AddQueryAndLoadStatementsStreamsIT {
    private static final Path LUBM_FILE = Paths.get("src/test/resources/lubm-1uni-withschema.nt");
    private static final String LUBM_PREFIX = "http://swat.cse.lehigh.edu/onto/univ-bench.owl#";
    private static final int LUBM_EXPECTED_RESULTS_COUNT = 1874;

    private final String ryaInstance = UUID.randomUUID().toString();
    private QueryRepository queryRepo;

    @Rule
    public KafkaTestInstanceRule kafka = new KafkaTestInstanceRule(true);

    @Before
    public void setup() {
        // Make sure the topic that the change log uses exists.
        final String changeLogTopic = KafkaTopics.queryChangeLogTopic("" + ryaInstance);
        kafka.createTopic(changeLogTopic);

        // Setup the QueryRepository used by the test.
        final Producer<?, QueryChange> queryProducer = KafkaTestUtil.makeProducer(kafka, StringSerializer.class, QueryChangeSerializer.class);
        final Consumer<?, QueryChange> queryConsumer = KafkaTestUtil.fromStartConsumer(kafka, StringDeserializer.class, QueryChangeDeserializer.class);
        final QueryChangeLog changeLog = new KafkaQueryChangeLog(queryProducer, queryConsumer, changeLogTopic);
        queryRepo = new InMemoryQueryRepository(changeLog, Scheduler.newFixedRateSchedule(0L, 5, TimeUnit.SECONDS));
    }

    @Test
    public void testLubm() throws Exception {
        // Arguments that add a query to Rya Streams.
        final String query =
                "PREFIX lubm: <" + LUBM_PREFIX + "> \n" +
                "SELECT * WHERE \n" +
                "{ \n" +
                "  ?graduateStudent a lubm:GraduateStudent . \n" +
                "  ?underGradUniversity a lubm:University . \n"  +
                "  ?graduateStudent lubm:undergraduateDegreeFrom ?underGradUniversity . \n" +
                "}";

        final String query2 =
                "PREFIX lubm: <" + LUBM_PREFIX + "> \n" +
                "SELECT * WHERE \n" +
                "{ \n" +
                "  ?graduateStudent a lubm:GraduateStudent . \n" +
                "  ?underGradUniversity a lubm:University . \n"  +
                "  ?graduateStudent lubm:undergraduateDegreeFrom ?underGradUniversity . \n" +
                "}";

        final String[] addArgs = new String[] {
                "--ryaInstance", "" + ryaInstance,
                "--kafkaHostname", kafka.getKafkaHostname(),
                "--kafkaPort", kafka.getKafkaPort(),
                "--query", query,
                "--isActive", "true",
                "--isInsert", "false"
        };

        final String[] addArgs2 = new String[] {
                "--ryaInstance", "" + ryaInstance,
                "--kafkaHostname", kafka.getKafkaHostname(),
                "--kafkaPort", kafka.getKafkaPort(),
                "--query", query2,
                "--isActive", "true",
                "--isInsert", "false"
        };

        // Execute the command.
        final AddQueryCommand command = new AddQueryCommand();
        command.execute(addArgs);
        // Add the same query twice to confirm that joins aren't being performed
        // across both queries.
        command.execute(addArgs2);

        // Show that the query was added to the Query Repository.
        final Set<StreamsQuery> queries = queryRepo.list();
        assertEquals(2, queries.size());
        final StreamsQuery streamsQuery = queries.iterator().next();
        final UUID queryId = streamsQuery.getQueryId();
        assertEquals(query, queries.iterator().next().getSparql());

        // Load a file of statements into Kafka.
        final String visibilities = "";
        final String[] loadArgs = new String[] {
                "--ryaInstance", "" + ryaInstance,
                "--kafkaHostname", kafka.getKafkaHostname(),
                "--kafkaPort", kafka.getKafkaPort(),
                "--statementsFile", LUBM_FILE.toString(),
                "--visibilities", visibilities
        };

        // Load the file of statements into the Statements topic.
        new LoadStatementsCommand().execute(loadArgs);

        final String statementsTopic = KafkaTopics.statementsTopic(ryaInstance);
        final String resultsTopic = KafkaTopics.queryResultsTopic(ryaInstance, queryId);

        final TopologyFactory factory = new TopologyFactory();
        final TopologyBuilder builder = factory.build(query, statementsTopic, resultsTopic, new RandomUUIDFactory());

        // Start the streams program.
        final Properties props = kafka.createBootstrapServerConfig();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final AtomicReference<String> errorMessage = new AtomicReference<>();
        final KafkaStreams streams = new KafkaStreams(builder, new StreamsConfig(props));
        streams.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(final Thread thread, final Throwable throwable) {
                final String stackTrace = ExceptionUtils.getStackTrace(throwable);
                errorMessage.getAndSet("Kafka Streams threw an uncaught exception in thread (" + thread.getName() + "): " + stackTrace);
            }
        });
        streams.cleanUp();
        try {
            streams.start();

            // Wait for the streams application to start. Streams only see data after their consumers are connected.
            Thread.sleep(6000);

            // Wait for the final results to appear in the output topic and verify the expected Binding Sets were found.
            try(Consumer<String, VisibilityBindingSet> consumer = KafkaTestUtil.fromStartConsumer(kafka, StringDeserializer.class, VisibilityBindingSetDeserializer.class)) {
                // Register the topic.
                consumer.subscribe(Arrays.asList(resultsTopic));

                // Poll for the result.
                final Set<VisibilityBindingSet> results = Sets.newHashSet( KafkaTestUtil.pollForResults(500, 2 * LUBM_EXPECTED_RESULTS_COUNT, LUBM_EXPECTED_RESULTS_COUNT, consumer) );

                System.out.println("LUBM Query Results Count: " + results.size());
                // Show the correct binding sets results from the job.
                assertEquals(LUBM_EXPECTED_RESULTS_COUNT, results.size());
            }
        } finally {
            streams.close();
        }

        if (StringUtils.isNotBlank(errorMessage.get())) {
            fail(errorMessage.get());
        }
    }
}