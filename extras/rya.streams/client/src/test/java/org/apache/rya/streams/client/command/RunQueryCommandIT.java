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

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractScheduledService.Scheduler;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.api.model.VisibilityStatement;
import org.apache.rya.streams.api.entity.StreamsQuery;
import org.apache.rya.streams.api.interactor.LoadStatements;
import org.apache.rya.streams.api.queries.InMemoryQueryRepository;
import org.apache.rya.streams.api.queries.QueryChange;
import org.apache.rya.streams.api.queries.QueryChangeLog;
import org.apache.rya.streams.api.queries.QueryRepository;
import org.apache.rya.streams.client.RyaStreamsCommand.ArgumentsException;
import org.apache.rya.streams.client.RyaStreamsCommand.ExecutionException;
import org.apache.rya.streams.kafka.KafkaTopics;
import org.apache.rya.streams.kafka.interactor.KafkaLoadStatements;
import org.apache.rya.streams.kafka.queries.KafkaQueryChangeLog;
import org.apache.rya.streams.kafka.serialization.VisibilityBindingSetDeserializer;
import org.apache.rya.streams.kafka.serialization.VisibilityStatementSerializer;
import org.apache.rya.streams.kafka.serialization.queries.QueryChangeDeserializer;
import org.apache.rya.streams.kafka.serialization.queries.QueryChangeSerializer;
import org.apache.rya.test.kafka.KafkaTestInstanceRule;
import org.apache.rya.test.kafka.KafkaTestUtil;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * Integration tests the methods of {@link RunQueryCommand}.
 */
public class RunQueryCommandIT {
    private static final Path LUBM_FILE = Paths.get("src/test/resources/lubm-1uni-withschema.nt");
    private static final String LUBM_PREFIX = "http://swat.cse.lehigh.edu/onto/univ-bench.owl#";
    private static final int LUBM_EXPECTED_RESULTS_COUNT = 1874;

    private final String ryaInstance = UUID.randomUUID().toString();

    private QueryRepository queryRepo;
    private Producer<String, VisibilityStatement> stmtProducer = null;
    private Consumer<String, VisibilityBindingSet> resultConsumer = null;

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

        // Initialize the Statements Producer and the Results Consumer.
        stmtProducer = KafkaTestUtil.makeProducer(kafka, StringSerializer.class, VisibilityStatementSerializer.class);
        resultConsumer = KafkaTestUtil.fromStartConsumer(kafka, StringDeserializer.class, VisibilityBindingSetDeserializer.class);
    }

    @After
    public void cleanup() throws Exception {
        queryRepo.stopAsync();
        stmtProducer.close();
        resultConsumer.close();
    }

    @Test(expected = ExecutionException.class)
    public void runUnregisteredQuery() throws Exception {
        // Arguments that run a query that is not registered with Rya Streams.
        final String[] args = new String[] {
                "--ryaInstance", "" + ryaInstance,
                "--kafkaHostname", kafka.getKafkaHostname(),
                "--kafkaPort", kafka.getKafkaPort(),
                "--queryID", UUID.randomUUID().toString(),
                "--zookeepers", kafka.getZookeeperServers()
        };

        // Run the test. This will throw an exception.
        final RunQueryCommand command = new RunQueryCommand();
        command.execute(args);
    }

    @Test
    public void runQuery() throws Exception {
        // Register a query with the Query Repository.
        final StreamsQuery sQuery = queryRepo.add("SELECT * WHERE { ?person <urn:worksAt> ?business . }", true, false);

        // Arguments that run the query we just registered with Rya Streams.
        final String[] args = new String[] {
                "--ryaInstance", "" + ryaInstance,
                "--kafkaHostname", kafka.getKafkaHostname(),
                "--kafkaPort", kafka.getKafkaPort(),
                "--queryID", sQuery.getQueryId().toString(),
                "--zookeepers", kafka.getZookeeperServers()
        };

        // Create a new Thread that runs the command.
        final Thread commandThread = new Thread() {
            @Override
            public void run() {
                final RunQueryCommand command = new RunQueryCommand();
                try {
                    command.execute(args);
                } catch (final ArgumentsException | ExecutionException e) {
                    // Do nothing. Test will still fail because the expected results will be missing.
                }
            }
        };

        // Create the statements that will be loaded.
        final ValueFactory vf = SimpleValueFactory.getInstance();
        final List<VisibilityStatement> statements = new ArrayList<>();
        statements.add(new VisibilityStatement(vf.createStatement(
                vf.createIRI("urn:Alice"),
                vf.createIRI("urn:worksAt"),
                vf.createIRI("urn:BurgerJoint")), "a"));
        statements.add(new VisibilityStatement(vf.createStatement(
                vf.createIRI("urn:Bob"),
                vf.createIRI("urn:worksAt"),
                vf.createIRI("urn:TacoShop")), "a"));
        statements.add(new VisibilityStatement(vf.createStatement(
                vf.createIRI("urn:Charlie"),
                vf.createIRI("urn:worksAt"),
                vf.createIRI("urn:TacoShop")), "a"));

        // Create the expected results.
        final List<VisibilityBindingSet> expected = new ArrayList<>();
        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("person", vf.createIRI("urn:Alice"));
        bs.addBinding("business", vf.createIRI("urn:BurgerJoint"));
        expected.add(new VisibilityBindingSet(bs, "a"));
        bs = new MapBindingSet();
        bs.addBinding("person", vf.createIRI("urn:Bob"));
        bs.addBinding("business", vf.createIRI("urn:TacoShop"));
        expected.add(new VisibilityBindingSet(bs, "a"));
        bs = new MapBindingSet();
        bs.addBinding("person", vf.createIRI("urn:Charlie"));
        bs.addBinding("business", vf.createIRI("urn:TacoShop"));
        expected.add(new VisibilityBindingSet(bs, "a"));

        // Execute the test. This will result in a set of results that were read from the results topic.
        final List<VisibilityBindingSet> results;
        try {
            // Wait for the program to start.
            commandThread.start();
            Thread.sleep(5000);

            // Write some statements to the program.
            final String statementsTopic = KafkaTopics.statementsTopic(ryaInstance);
            final LoadStatements loadStatements = new KafkaLoadStatements(statementsTopic, stmtProducer);
            loadStatements.fromCollection(statements);

            // Read the output of the streams program.
            final String resultsTopic = KafkaTopics.queryResultsTopic(ryaInstance, sQuery.getQueryId());
            resultConsumer.subscribe( Lists.newArrayList(resultsTopic) );
            results = KafkaTestUtil.pollForResults(500, 6, 3, resultConsumer);
        } finally {
            // Tear down the test.
            commandThread.interrupt();
            commandThread.join(3000);
        }

        // Show the read results matched the expected ones.
        assertEquals(expected, results);
    }

    @Test
    public void runQueryFromFile() throws Exception {
        // NOTE: the order of the query statements previously led to join
        // issues. When "lubm:undergraduateDegreeFrom" was the first statement
        // in the where clause (as opposed to the last) the
        // KeyValueJoinStateStore blew up.
        // (This issue appears to have been resolved now though, but is tested
        // here)
        final String query =
                "PREFIX lubm: <" + LUBM_PREFIX + "> \n" +
                "SELECT * WHERE \n" +
                "{ \n" +
                "  ?graduateStudent lubm:undergraduateDegreeFrom ?underGradUniversity . \n" +
                "  ?graduateStudent a lubm:GraduateStudent . \n" +
                "  ?underGradUniversity a lubm:University . \n"  +
                "}";

        // Register a query with the Query Repository.
        final StreamsQuery sQuery = queryRepo.add(query, true, true);

        // Arguments that run the query we just registered with Rya Streams.
        final String[] args = new String[] {
                "--ryaInstance", "" + ryaInstance,
                "--kafkaHostname", kafka.getKafkaHostname(),
                "--kafkaPort", kafka.getKafkaPort(),
                "--queryID", sQuery.getQueryId().toString(),
                "--zookeepers", kafka.getZookeeperServers()
        };

        // Create a new Thread that runs the command.
        final Thread commandThread = new Thread() {
            @Override
            public void run() {
                final RunQueryCommand command = new RunQueryCommand();
                try {
                    command.execute(args);
                } catch (final ArgumentsException | ExecutionException e) {
                    // Do nothing. Test will still fail because the expected results will be missing.
                }
            }
        };

        // Execute the test. This will result in a set of results that were read from the results topic.
        final Set<VisibilityBindingSet> results = new HashSet<>();
        try {
            // Wait for the program to start.
            commandThread.start();
            Thread.sleep(5000);

            // Write some statements to the program.
            final String statementsTopic = KafkaTopics.statementsTopic(ryaInstance);
            final LoadStatements loadStatements = new KafkaLoadStatements(statementsTopic, stmtProducer);
            loadStatements.fromFile(LUBM_FILE, "");

            // Read the output of the streams program.
            final String resultsTopic = KafkaTopics.queryResultsTopic(ryaInstance, sQuery.getQueryId());
            resultConsumer.subscribe( Lists.newArrayList(resultsTopic) );
            results.addAll(KafkaTestUtil.pollForResults(500, 2 * LUBM_EXPECTED_RESULTS_COUNT, LUBM_EXPECTED_RESULTS_COUNT, resultConsumer));
        } finally {
            // Tear down the test.
            commandThread.interrupt();
            commandThread.join(3000);
        }

        System.out.println("LUBM Query Results Count: " + results.size());
        // Show the read results matched the expected ones.
        assertEquals(LUBM_EXPECTED_RESULTS_COUNT, results.size());
    }
}