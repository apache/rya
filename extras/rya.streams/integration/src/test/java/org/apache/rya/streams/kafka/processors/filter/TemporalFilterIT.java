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

import static org.junit.Assert.assertEquals;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.rya.api.function.projection.RandomUUIDFactory;
import org.apache.rya.api.function.temporal.TemporalURIs;
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
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.algebra.evaluation.function.Function;
import org.openrdf.query.algebra.evaluation.function.FunctionRegistry;
import org.openrdf.query.impl.MapBindingSet;

/**
 * Integration tests the temporal methods of {@link FilterProcessor}.
 */
public class TemporalFilterIT {
    private static final ValueFactory vf = new ValueFactoryImpl();
    private static final String TEMPORAL = "http://rya.apache.org/ns/temporal";
    private static final ZonedDateTime TIME = ZonedDateTime.parse("2015-12-30T12:00:00Z");
    private static final ZonedDateTime TIME_10 = ZonedDateTime.parse("2015-12-30T12:00:10Z");
    private static final ZonedDateTime TIME_20 = ZonedDateTime.parse("2015-12-30T12:00:20Z");

    @Rule
    public KafkaTestInstanceRule kafka = new KafkaTestInstanceRule(false);

    @Test
    public void temporalFunctionsRegistered() {
        int count = 0;
        final Collection<Function> funcs = FunctionRegistry.getInstance().getAll();
        for (final Function fun : funcs) {
            if (fun.getURI().startsWith(TEMPORAL)) {
                count++;
            }
        }

        // There are 4 temporal functions registered, ensure that there are 4.
        assertEquals(4, count);
    }

    @Test
    public void showEqualsWorks() throws Exception {
        // Enumerate some topics that will be re-used
        final String ryaInstance = UUID.randomUUID().toString();
        final UUID queryId = UUID.randomUUID();
        final String statementsTopic = KafkaTopics.statementsTopic(ryaInstance);
        final String resultsTopic = KafkaTopics.queryResultsTopic(queryId);

        // Get the RDF model objects that will be used to build the query.
        final String sparql =
                "PREFIX time: <http://www.w3.org/2006/time/> \n"
                        + "PREFIX tempf: <" + TemporalURIs.NAMESPACE + ">\n"
                        + "SELECT * \n"
                        + "WHERE { \n"
                        + "  <urn:time> time:atTime ?date .\n"
                        + " FILTER(tempf:equals(?date, \"" + TIME.toString() + "\")) "
                        + "}";
        // Setup a topology.
        final TopologyBuilder builder = new TopologyFactory().build(sparql, statementsTopic, resultsTopic, new RandomUUIDFactory());

        // Create the statements that will be input into the query.
        final ValueFactory vf = new ValueFactoryImpl();
        final List<VisibilityStatement> statements = getStatements();

        // Make the expected results.
        final Set<VisibilityBindingSet> expected = new HashSet<>();
        final MapBindingSet bs = new MapBindingSet();
        bs.addBinding("date", vf.createLiteral(TIME.toString()));
        expected.add( new VisibilityBindingSet(bs, "a") );

        // Run the test.
        RyaStreamsTestUtil.runStreamProcessingTest(kafka, statementsTopic, resultsTopic, builder, statements, expected, VisibilityBindingSetDeserializer.class);
    }

    @Test
    public void showBeforeWorks() throws Exception {
        // Enumerate some topics that will be re-used
        final String ryaInstance = UUID.randomUUID().toString();
        final UUID queryId = UUID.randomUUID();
        final String statementsTopic = KafkaTopics.statementsTopic(ryaInstance);
        final String resultsTopic = KafkaTopics.queryResultsTopic(queryId);

        // Get the RDF model objects that will be used to build the query.
        final String sparql =
                "PREFIX time: <http://www.w3.org/2006/time/> \n"
                        + "PREFIX tempf: <" + TemporalURIs.NAMESPACE + ">\n"
                        + "SELECT * \n"
                        + "WHERE { \n"
                        + "  <urn:time> time:atTime ?date .\n"
                        + " FILTER(tempf:before(?date, \"" + TIME_10.toString() + "\")) "
                        + "}";
        // Setup a topology.
        final TopologyBuilder builder = new TopologyFactory().build(sparql, statementsTopic, resultsTopic, new RandomUUIDFactory());

        // Create the statements that will be input into the query.
        final ValueFactory vf = new ValueFactoryImpl();
        final List<VisibilityStatement> statements = getStatements();

        // Make the expected results.
        final Set<VisibilityBindingSet> expected = new HashSet<>();
        final MapBindingSet bs = new MapBindingSet();
        bs.addBinding("date", vf.createLiteral(TIME.toString()));
        expected.add( new VisibilityBindingSet(bs, "a") );

        // Run the test.
        RyaStreamsTestUtil.runStreamProcessingTest(kafka, statementsTopic, resultsTopic, builder, statements, expected, VisibilityBindingSetDeserializer.class);
    }

    @Test
    public void showAfterWorks() throws Exception {
        // Enumerate some topics that will be re-used
        final String ryaInstance = UUID.randomUUID().toString();
        final UUID queryId = UUID.randomUUID();
        final String statementsTopic = KafkaTopics.statementsTopic(ryaInstance);
        final String resultsTopic = KafkaTopics.queryResultsTopic(queryId);

        // Get the RDF model objects that will be used to build the query.
        final String sparql =
                "PREFIX time: <http://www.w3.org/2006/time/> \n"
                        + "PREFIX tempf: <" + TemporalURIs.NAMESPACE + ">\n"
                        + "SELECT * \n"
                        + "WHERE { \n"
                        + "  <urn:time> time:atTime ?date .\n"
                        + " FILTER(tempf:after(?date, \"" + TIME_10.toString() + "\")) "
                        + "}";
        // Setup a topology.
        final TopologyBuilder builder = new TopologyFactory().build(sparql, statementsTopic, resultsTopic, new RandomUUIDFactory());

        // Create the statements that will be input into the query.
        final ValueFactory vf = new ValueFactoryImpl();
        final List<VisibilityStatement> statements = getStatements();

        // Make the expected results.
        final Set<VisibilityBindingSet> expected = new HashSet<>();
        final MapBindingSet bs = new MapBindingSet();
        bs.addBinding("date", vf.createLiteral(TIME_20.toString()));
        expected.add( new VisibilityBindingSet(bs, "a") );

        // Run the test.
        RyaStreamsTestUtil.runStreamProcessingTest(kafka, statementsTopic, resultsTopic, builder, statements, expected, VisibilityBindingSetDeserializer.class);
    }

    @Test
    public void showWithinWorks() throws Exception {
        // Enumerate some topics that will be re-used
        final String ryaInstance = UUID.randomUUID().toString();
        final UUID queryId = UUID.randomUUID();
        final String statementsTopic = KafkaTopics.statementsTopic(ryaInstance);
        final String resultsTopic = KafkaTopics.queryResultsTopic(queryId);

        // Get the RDF model objects that will be used to build the query.
        final String sparql =
                "PREFIX time: <http://www.w3.org/2006/time/> \n"
                        + "PREFIX tempf: <" + TemporalURIs.NAMESPACE + ">\n"
                        + "SELECT * \n"
                        + "WHERE { \n"
                        + "  <urn:time> time:atTime ?date .\n"
                        + " FILTER(tempf:within(?date, \"" + TIME.toString() + "/" + TIME_20.toString() + "\")) "
                        + "}";
        // Setup a topology.
        final TopologyBuilder builder = new TopologyFactory().build(sparql, statementsTopic, resultsTopic, new RandomUUIDFactory());

        // Create the statements that will be input into the query.
        final ValueFactory vf = new ValueFactoryImpl();
        final List<VisibilityStatement> statements = getStatements();

        // Make the expected results.
        final Set<VisibilityBindingSet> expected = new HashSet<>();
        final MapBindingSet bs = new MapBindingSet();
        bs.addBinding("date", vf.createLiteral(TIME_10.toString()));
        expected.add( new VisibilityBindingSet(bs, "a") );

        // Run the test.
        RyaStreamsTestUtil.runStreamProcessingTest(kafka, statementsTopic, resultsTopic, builder, statements, expected, VisibilityBindingSetDeserializer.class);
    }

    private List<VisibilityStatement> getStatements() throws Exception {
        final List<VisibilityStatement> statements = new ArrayList<>();
        statements.add(new VisibilityStatement(statement(TIME), "a"));
        statements.add(new VisibilityStatement(statement(TIME_10), "a"));
        statements.add(new VisibilityStatement(statement(TIME_20), "a"));
        return statements;
    }

    private static Statement statement(final ZonedDateTime time) {
        final Resource subject = vf.createURI("urn:time");
        final URI predicate = vf.createURI("http://www.w3.org/2006/time/atTime");
        final Value object = vf.createLiteral(time.toString());
        return new StatementImpl(subject, predicate, object);
    }
}
