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
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.rya.api.function.projection.ProjectionEvaluator;
import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.api.model.VisibilityStatement;
import org.apache.rya.streams.kafka.KafkaTestUtil;
import org.apache.rya.streams.kafka.KafkaTopics;
import org.apache.rya.streams.kafka.RdfTestUtil;
import org.apache.rya.streams.kafka.processors.ProcessorResult;
import org.apache.rya.streams.kafka.processors.StatementPatternProcessorSupplier;
import org.apache.rya.streams.kafka.processors.ProcessorResult.UnaryResult;
import org.apache.rya.streams.kafka.processors.StatementPatternProcessorSupplier.StatementPatternProcessor;
import org.apache.rya.streams.kafka.processors.output.BindingSetOutputFormatterSupplier.BindingSetOutputFormatter;
import org.apache.rya.streams.kafka.processors.projection.ProjectionProcessorSupplier;
import org.apache.rya.streams.kafka.serialization.VisibilityBindingSetSerializer;
import org.apache.rya.streams.kafka.serialization.VisibilityStatementDeserializer;
import org.apache.rya.test.kafka.KafkaTestInstanceRule;
import org.junit.Rule;
import org.junit.Test;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.impl.MapBindingSet;

import com.google.common.collect.Sets;

/**
 * Integration tests the methods of {@link StatementPatternProcessor}.
 */
public class ProjectionProcessorIT {

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
        final String sparql =
                "SELECT (?person AS ?p) ?otherPerson " +
                "WHERE { " +
                    "?person <urn:talksTo> ?otherPerson . " +
                "}";
        final Projection projection = RdfTestUtil.getProjection(sparql);
        final StatementPattern sp = RdfTestUtil.getSp(sparql);

        // Setup a topology.
        final TopologyBuilder builder = new TopologyBuilder();

        // The topic that Statements are written to is used as a source.
        builder.addSource("STATEMENTS", new StringDeserializer(), new VisibilityStatementDeserializer(), statementsTopic);

        // Add a processor that handles the first statement pattern.
        builder.addProcessor("SP1", new StatementPatternProcessorSupplier(sp, result -> ProcessorResult.make( new UnaryResult(result) )), "STATEMENTS");

        // Add a processor that handles the projection.
        builder.addProcessor("P1", new ProjectionProcessorSupplier(
                ProjectionEvaluator.make(projection),
                result -> ProcessorResult.make(new UnaryResult(result))), "SP1");

        // Add a processor that formats the VisibilityBindingSet for output.
        builder.addProcessor("SINK_FORMATTER", BindingSetOutputFormatter::new, "P1");

        // Add a sink that writes the data out to a new Kafka topic.
        builder.addSink("QUERY_RESULTS", resultsTopic, new StringSerializer(), new VisibilityBindingSetSerializer(), "SINK_FORMATTER");

        // Load some data into the input topic.
        final ValueFactory vf = new ValueFactoryImpl();
        final List<VisibilityStatement> statements = new ArrayList<>();
        statements.add( new VisibilityStatement(vf.createStatement(vf.createURI("urn:Alice"), vf.createURI("urn:talksTo"), vf.createURI("urn:Bob")), "a") );

        // Show the correct binding set results from the job.
        final Set<VisibilityBindingSet> expected = new HashSet<>();

        final MapBindingSet expectedBs = new MapBindingSet();
        expectedBs.addBinding("p", vf.createURI("urn:Alice"));
        expectedBs.addBinding("otherPerson", vf.createURI("urn:Bob"));
        expected.add(new VisibilityBindingSet(expectedBs, "a"));

        KafkaTestUtil.runStreamProcessingTest(kafka, statementsTopic, resultsTopic, builder, 2000, statements, Sets.newHashSet(expected));
    }
}