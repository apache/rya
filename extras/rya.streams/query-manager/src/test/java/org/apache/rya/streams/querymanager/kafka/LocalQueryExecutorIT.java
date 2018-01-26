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
package org.apache.rya.streams.querymanager.kafka;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.api.model.VisibilityStatement;
import org.apache.rya.streams.api.entity.StreamsQuery;
import org.apache.rya.streams.api.interactor.LoadStatements;
import org.apache.rya.streams.kafka.KafkaStreamsFactory;
import org.apache.rya.streams.kafka.KafkaTopics;
import org.apache.rya.streams.kafka.SingleThreadKafkaStreamsFactory;
import org.apache.rya.streams.kafka.interactor.KafkaLoadStatements;
import org.apache.rya.streams.kafka.serialization.VisibilityBindingSetDeserializer;
import org.apache.rya.streams.kafka.serialization.VisibilityStatementSerializer;
import org.apache.rya.streams.querymanager.QueryExecutor;
import org.apache.rya.test.kafka.KafkaTestInstanceRule;
import org.apache.rya.test.kafka.KafkaTestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.impl.MapBindingSet;

import com.google.common.collect.Lists;

/**
 * Integration tests the methods of {@link LocalQueryExecutor}.
 */
public class LocalQueryExecutorIT {

    private final String ryaInstance = UUID.randomUUID().toString();

    private Producer<String, VisibilityStatement> stmtProducer = null;
    private Consumer<String, VisibilityBindingSet> resultConsumer = null;

    @Rule
    public KafkaTestInstanceRule kafka = new KafkaTestInstanceRule(false);

    @Before
    public void setup() {
        // Make sure the topic that the change log uses exists.
        final String changeLogTopic = KafkaTopics.queryChangeLogTopic("" + ryaInstance);
        kafka.createTopic(changeLogTopic);

        // Initialize the Statements Producer and the Results Consumer.
        stmtProducer = KafkaTestUtil.makeProducer(kafka, StringSerializer.class, VisibilityStatementSerializer.class);
        resultConsumer = KafkaTestUtil.fromStartConsumer(kafka, StringDeserializer.class, VisibilityBindingSetDeserializer.class);
    }

    @After
    public void cleanup() throws Exception {
        stmtProducer.close();
        resultConsumer.close();
    }

    @Test
    public void runQuery() throws Exception {
        // Test values.
        final String ryaInstance = "rya";
        final StreamsQuery sQuery = new StreamsQuery(UUID.randomUUID(), "SELECT * WHERE { ?person <urn:worksAt> ?business . }", true);

        // Create the statements that will be loaded.
        final ValueFactory vf = new ValueFactoryImpl();
        final List<VisibilityStatement> statements = new ArrayList<>();
        statements.add(new VisibilityStatement(vf.createStatement(
                vf.createURI("urn:Alice"),
                vf.createURI("urn:worksAt"),
                vf.createURI("urn:BurgerJoint")), "a"));
        statements.add(new VisibilityStatement(vf.createStatement(
                vf.createURI("urn:Bob"),
                vf.createURI("urn:worksAt"),
                vf.createURI("urn:TacoShop")), "a"));
        statements.add(new VisibilityStatement(vf.createStatement(
                vf.createURI("urn:Charlie"),
                vf.createURI("urn:worksAt"),
                vf.createURI("urn:TacoShop")), "a"));

        // Create the expected results.
        final List<VisibilityBindingSet> expected = new ArrayList<>();
        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("person", vf.createURI("urn:Alice"));
        bs.addBinding("business", vf.createURI("urn:BurgerJoint"));
        expected.add(new VisibilityBindingSet(bs, "a"));
        bs = new MapBindingSet();
        bs.addBinding("person", vf.createURI("urn:Bob"));
        bs.addBinding("business", vf.createURI("urn:TacoShop"));
        expected.add(new VisibilityBindingSet(bs, "a"));
        bs = new MapBindingSet();
        bs.addBinding("person", vf.createURI("urn:Charlie"));
        bs.addBinding("business", vf.createURI("urn:TacoShop"));
        expected.add(new VisibilityBindingSet(bs, "a"));

        // Start the executor that will be tested.
        final String kafkaServers = kafka.getKafkaHostname() + ":" + kafka.getKafkaPort();
        final KafkaStreamsFactory jobFactory = new SingleThreadKafkaStreamsFactory(kafkaServers);
        final QueryExecutor executor = new LocalQueryExecutor(jobFactory);
        executor.startAndWait();
        try {
            // Start the query.
            executor.startQuery(ryaInstance, sQuery);

            // Wait for the program to start.
            Thread.sleep(5000);

            // Write some statements to the program.
            final String statementsTopic = KafkaTopics.statementsTopic(ryaInstance);
            final LoadStatements loadStatements = new KafkaLoadStatements(statementsTopic, stmtProducer);
            loadStatements.fromCollection(statements);

            // Read the output of the streams program.
            final String resultsTopic = KafkaTopics.queryResultsTopic(sQuery.getQueryId());
            resultConsumer.subscribe( Lists.newArrayList(resultsTopic) );
            final List<VisibilityBindingSet> results = KafkaTestUtil.pollForResults(500, 6, 3, resultConsumer);
            assertEquals(expected, results);

        } finally {
            executor.stopAndWait();
        }
    }
}