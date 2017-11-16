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

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.rya.api.function.join.NaturalJoin;
import org.apache.rya.api.function.projection.MultiProjectionEvaluator;
import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.api.model.VisibilityStatement;
import org.apache.rya.streams.kafka.KafkaTestUtil;
import org.apache.rya.streams.kafka.KafkaTopics;
import org.apache.rya.streams.kafka.RdfTestUtil;
import org.apache.rya.streams.kafka.processors.ProcessorResult;
import org.apache.rya.streams.kafka.processors.ProcessorResult.BinaryResult;
import org.apache.rya.streams.kafka.processors.ProcessorResult.BinaryResult.Side;
import org.apache.rya.streams.kafka.processors.ProcessorResult.UnaryResult;
import org.apache.rya.streams.kafka.processors.StatementPatternProcessorSupplier;
import org.apache.rya.streams.kafka.processors.join.JoinProcessorSupplier;
import org.apache.rya.streams.kafka.processors.output.BindingSetOutputFormatterSupplier.BindingSetOutputFormatter;
import org.apache.rya.streams.kafka.processors.projection.MultiProjectionProcessorSupplier.MultiProjectionProcessor;
import org.apache.rya.streams.kafka.serialization.VisibilityBindingSetSerde;
import org.apache.rya.streams.kafka.serialization.VisibilityBindingSetSerializer;
import org.apache.rya.streams.kafka.serialization.VisibilityStatementDeserializer;
import org.apache.rya.test.kafka.KafkaTestInstanceRule;
import org.junit.Rule;
import org.junit.Test;
import org.openrdf.model.BNode;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.algebra.MultiProjection;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.impl.MapBindingSet;

import com.google.common.collect.Lists;

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
        final String resultsTopic = KafkaTopics.queryResultsTopic(queryId);

        // Get the RDF model objects that will be used to build the query.
        final StatementPattern sp1 = RdfTestUtil.getSp("SELECT * WHERE { ?thing <urn:corner> ?location . }");
        final StatementPattern sp2 = RdfTestUtil.getSp("SELECT * WHERE { ?thing <urn:compass> ?direction . }");
        final MultiProjection multiProjection = RdfTestUtil.getMultiProjection(
                "CONSTRUCT {" +
                    "_:b a <urn:movementObservation> ; " +
                    "<urn:location> ?location ; " +
                    "<urn:direction> ?direction ; " +
                "}" +
                "WHERE {" +
                    "?thing <urn:corner> ?location ." +
                    "?thing <urn:compass> ?direction." +
                "}");

        // Setup a topology.
        final TopologyBuilder builder = new TopologyBuilder();
        builder.addSource("STATEMENTS", new StringDeserializer(), new VisibilityStatementDeserializer(), statementsTopic);
        builder.addProcessor("SP1", new StatementPatternProcessorSupplier(sp1,
                result -> ProcessorResult.make( new BinaryResult(Side.LEFT, result) )), "STATEMENTS");
        builder.addProcessor("SP2", new StatementPatternProcessorSupplier(sp2,
                result -> ProcessorResult.make( new BinaryResult(Side.RIGHT, result) )), "STATEMENTS");

        builder.addProcessor("NATURAL_JOIN", new JoinProcessorSupplier(
                "NATURAL_JOIN",
                new NaturalJoin(),
                Lists.newArrayList("thing"),
                Lists.newArrayList("thing", "location", "direction"),
                result -> ProcessorResult.make( new UnaryResult(result) )), "SP1", "SP2");

        final StateStoreSupplier joinStoreSupplier =
                Stores.create( "NATURAL_JOIN" )
                  .withStringKeys()
                  .withValues(new VisibilityBindingSetSerde())
                  .inMemory()
                  .build();
        builder.addStateStore(joinStoreSupplier, "NATURAL_JOIN");

        final String blankNodeId = UUID.randomUUID().toString();
        builder.addProcessor("MULTIPROJECTION", new MultiProjectionProcessorSupplier(
                MultiProjectionEvaluator.make(multiProjection, () -> blankNodeId),
                result -> ProcessorResult.make(new UnaryResult(result))), "NATURAL_JOIN");

        builder.addProcessor("SINK_FORMATTER", BindingSetOutputFormatter::new, "MULTIPROJECTION");
        builder.addSink("QUERY_RESULTS", resultsTopic, new StringSerializer(), new VisibilityBindingSetSerializer(), "SINK_FORMATTER");

        // Create the statements that will be input into the query.
        final ValueFactory vf = new ValueFactoryImpl();
        final List<VisibilityStatement> statements = new ArrayList<>();
        statements.add( new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:car1"), vf.createURI("urn:compass"), vf.createURI("urn:NW")), "a") );
        statements.add( new VisibilityStatement(
                vf.createStatement(vf.createURI("urn:car1"), vf.createURI("urn:corner"), vf.createURI("urn:corner1")), "a") );

        // Make the expected results.
        final Set<VisibilityBindingSet> expected = new HashSet<>();
        final BNode blankNode = vf.createBNode(blankNodeId);

        MapBindingSet expectedBs = new MapBindingSet();
        expectedBs.addBinding("subject", blankNode);
        expectedBs.addBinding("predicate", RDF.TYPE);
        expectedBs.addBinding("object", vf.createURI("urn:movementObservation"));

        expectedBs = new MapBindingSet();
        expectedBs.addBinding("subject", blankNode);
        expectedBs.addBinding("predicate", vf.createURI("urn:direction"));
        expectedBs.addBinding("object", vf.createURI("urn:NW"));


        expectedBs = new MapBindingSet();
        expectedBs.addBinding("subject", blankNode);
        expectedBs.addBinding("predicate", vf.createURI("urn:location"));
        expectedBs.addBinding("object", vf.createURI("urn:corner1"));

        // Run the test.
        KafkaTestUtil.runStreamProcessingTest(kafka, statementsTopic, resultsTopic, builder, 2000, statements, expected);
    }
}