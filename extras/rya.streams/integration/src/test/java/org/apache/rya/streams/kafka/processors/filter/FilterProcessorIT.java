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
package org.apache.rya.streams.kafka.processors.filter;

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
import org.apache.rya.streams.kafka.processors.filter.FilterProcessorSupplier.FilterProcessor;
import org.apache.rya.streams.kafka.serialization.VisibilityBindingSetDeserializer;
import org.apache.rya.streams.kafka.topology.TopologyFactory;
import org.apache.rya.test.kafka.KafkaTestInstanceRule;
import org.junit.Rule;
import org.junit.Test;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.impl.MapBindingSet;

/**
 * Integration tests the methods of {@link FilterProcessor}.
 */
public class FilterProcessorIT {

    @Rule
    public KafkaTestInstanceRule kafka = new KafkaTestInstanceRule(true);

    @Test
    public void showProcessorWorks() throws Exception {
        // Enumerate some topics that will be re-used
        final String ryaInstance = UUID.randomUUID().toString();
        final UUID queryId = UUID.randomUUID();
        final String statementsTopic = KafkaTopics.statementsTopic(ryaInstance);
        final String resultsTopic = KafkaTopics.queryResultsTopic(ryaInstance, queryId);

        // Get the RDF model objects that will be used to build the query.
        final String sparql =
                "SELECT * " +
                "WHERE { " +
                    "FILTER(?age < 10)" +
                    "?person <urn:age> ?age " +
                "}";

        // Setup a topology.
        final TopologyBuilder builder = new TopologyFactory().build(sparql, statementsTopic, resultsTopic, new RandomUUIDFactory());

        // Create the statements that will be input into the query.
        final ValueFactory vf = new ValueFactoryImpl();
        final List<VisibilityStatement> statements = new ArrayList<>();
        statements.add(new VisibilityStatement(vf.createStatement(vf.createURI("urn:Bob"), vf.createURI("urn:age"), vf.createLiteral(11)), "a"));
        statements.add(new VisibilityStatement(vf.createStatement(vf.createURI("urn:Alice"), vf.createURI("urn:age"), vf.createLiteral(9)), "a"));

        // Make the expected results.
        final Set<VisibilityBindingSet> expected = new HashSet<>();
        final MapBindingSet bs = new MapBindingSet();
        bs.addBinding("person", vf.createURI("urn:Alice"));
        bs.addBinding("age", vf.createLiteral(9));
        expected.add( new VisibilityBindingSet(bs, "a") );

        // Run the test.
        RyaStreamsTestUtil.runStreamProcessingTest(kafka, statementsTopic, resultsTopic, builder, statements, expected, VisibilityBindingSetDeserializer.class);
    }
}