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
import org.apache.rya.streams.api.exception.RyaStreamsException;
import org.apache.rya.streams.api.interactor.LoadStatements;
import org.apache.rya.streams.api.interactor.RunQuery;
import org.apache.rya.streams.api.queries.InMemoryQueryChangeLog;
import org.apache.rya.streams.api.queries.InMemoryQueryRepository;
import org.apache.rya.streams.api.queries.QueryRepository;
import org.apache.rya.streams.kafka.KafkaTopics;
import org.apache.rya.streams.kafka.serialization.VisibilityBindingSetDeserializer;
import org.apache.rya.streams.kafka.serialization.VisibilityStatementSerializer;
import org.apache.rya.streams.kafka.topology.TopologyFactory;
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
 * Integration tests the methods of {@link KafkaRunQuery}.
 */
public class KafkaRunQueryIT {

    @Rule
    public KafkaTestInstanceRule kafka = new KafkaTestInstanceRule(true);

    private Producer<String, VisibilityStatement> producer;
    private Consumer<String, VisibilityBindingSet> consumer;

    @Before
    public void setup() {
        producer = KafkaTestUtil.makeProducer(kafka, StringSerializer.class, VisibilityStatementSerializer.class);
        consumer = KafkaTestUtil.fromStartConsumer(kafka, StringDeserializer.class, VisibilityBindingSetDeserializer.class);
    }

    @After
    public void cleanup() {
        producer.close();
        consumer.close();
    }

    @Test
    public void runQuery() throws Exception {
        // Setup some constant that will be used through the test.
        final String ryaInstance = UUID.randomUUID().toString();
        final String statementsTopic = KafkaTopics.statementsTopic(ryaInstance);

        // This query is completely in memory, so it doesn't need to be closed.
        final QueryRepository queries = new InMemoryQueryRepository( new InMemoryQueryChangeLog() );

        // Add the query to the query repository.
        final StreamsQuery sQuery = queries.add("SELECT * WHERE { ?person <urn:worksAt> ?business . }");
        final UUID queryId = sQuery.getQueryId();
        final String resultsTopic = KafkaTopics.queryResultsTopic(queryId);

        // The thread that will run the tested interactor.
        final Thread testThread = new Thread() {
            @Override
            public void run() {
                final RunQuery runQuery = new KafkaRunQuery(
                        kafka.getKafkaHostname(),
                        kafka.getKafkaPort(),
                        statementsTopic,
                        resultsTopic,
                        queries,
                        new TopologyFactory());
                try {
                    runQuery.run(queryId);
                } catch (final RyaStreamsException e) {
                    // Do nothing. Test will still fail because the expected results will be missing.
                }
            }
        };

        // Create the topics.
        kafka.createTopic(statementsTopic);
        kafka.createTopic(resultsTopic);

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

        // Execute the test. This will result in a set of results that were read from the results topic.
        final List<VisibilityBindingSet> results;
        try {
            // Wait for the program to start.
            testThread.start();
            Thread.sleep(2000);

            // Write some statements to the program.
            final LoadStatements loadStatements = new KafkaLoadStatements(statementsTopic, producer);
            loadStatements.fromCollection(statements);

            // Read the output of the streams program.
            consumer.subscribe( Lists.newArrayList(resultsTopic) );
            results = KafkaTestUtil.pollForResults(500, 6, 3, consumer);
        } finally {
            // Tear down the test.
            testThread.interrupt();
            testThread.join(3000);
        }

        // Show the read results matched the expected ones.
        assertEquals(expected, results);
    }
}