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
package org.apache.rya.streams.kafka.processors.projection;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.rya.api.model.VisibilityStatement;
import org.apache.rya.streams.kafka.KafkaTopics;
import org.apache.rya.streams.kafka.RyaStreamsTestUtil;
import org.apache.rya.streams.kafka.processors.projection.MultiProjectionProcessorSupplier.MultiProjectionProcessor;
import org.apache.rya.streams.kafka.serialization.VisibilityStatementDeserializer;
import org.apache.rya.streams.kafka.topology.TopologyFactory;
import org.apache.rya.test.kafka.KafkaTestInstanceRule;
import org.junit.Rule;
import org.junit.Test;
import org.openrdf.model.BNode;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDF;

/**
 * Integration tests the methods of {@link MultiProjectionProcessor}.
 */
public class MultiProjectionProcessorIT {

    @Rule
    public KafkaTestInstanceRule kafka = new KafkaTestInstanceRule(true);

    @Test
    public void showProcessorWorks() throws Exception {
        // Enumerate some topics that will be re-used
        final String ryaInstance = UUID.randomUUID().toString();
        final UUID queryId = UUID.randomUUID();
        final String statementsTopic = KafkaTopics.statementsTopic(ryaInstance);
        final String resultsTopic = KafkaTopics.queryResultsTopic(ryaInstance, queryId);

        // Create a topology for the Query that will be tested.
        final String sparql =
                "CONSTRUCT {" +
                    "_:b a <urn:movementObservation> ; " +
                    "<urn:location> ?location ; " +
                    "<urn:direction> ?direction ; " +
                "}" +
                "WHERE {" +
                    "?thing <urn:corner> ?location ." +
                    "?thing <urn:compass> ?direction." +
                "}";

        final String bNodeId = UUID.randomUUID().toString();
        final TopologyBuilder builder = new TopologyFactory().build(sparql, statementsTopic, resultsTopic, () -> bNodeId);

        // Create the statements that will be input into the query.
        final ValueFactory vf = new ValueFactoryImpl();
        final List<VisibilityStatement> statements = new ArrayList<>();
        statements.add( new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:car1"), vf.createURI("urn:compass"), vf.createURI("urn:NW")), "a") );
        statements.add( new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:car1"), vf.createURI("urn:corner"), vf.createURI("urn:corner1")), "a") );

        // Make the expected results.
        final Set<VisibilityStatement> expected = new HashSet<>();
        final BNode blankNode = vf.createBNode(bNodeId);

        expected.add(new VisibilityStatement(vf.createStatement(blankNode, RDF.TYPE, vf.createURI("urn:movementObservation")), "a"));
        expected.add(new VisibilityStatement(vf.createStatement(blankNode, vf.createURI("urn:direction"), vf.createURI("urn:NW")), "a"));
        expected.add(new VisibilityStatement(vf.createStatement(blankNode, vf.createURI("urn:location"), vf.createURI("urn:corner1")), "a"));

        // Run the test.
        RyaStreamsTestUtil.runStreamProcessingTest(kafka, statementsTopic, resultsTopic, builder, statements, expected, VisibilityStatementDeserializer.class);
    }
}