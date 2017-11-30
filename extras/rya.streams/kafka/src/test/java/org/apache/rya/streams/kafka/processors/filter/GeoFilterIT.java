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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.rya.api.function.projection.RandomUUIDFactory;
import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.api.model.VisibilityStatement;
import org.apache.rya.indexing.GeoConstants;
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

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.io.WKTWriter;

/**
 * Integration tests the methods of {@link FilterProcessor}.
 */
public class GeoFilterIT {
    private static final String GEO = "http://www.opengis.net/def/function/geosparql/";
    private static final GeometryFactory GF = new GeometryFactory();
    private static final Geometry ZERO = GF.createPoint(new Coordinate(0, 0));
    private static final Geometry ONE = GF.createPoint(new Coordinate(1, 1));

    @Rule
    public KafkaTestInstanceRule kafka = new KafkaTestInstanceRule(true);

    @Test
    public void showGeoFunctionsRegistered() {
        int count = 0;
        final Collection<Function> funcs = FunctionRegistry.getInstance().getAll();
        for (final Function fun : funcs) {
            if (fun.getURI().startsWith(GEO)) {
                count++;
            }
        }

        // There are 30 geo functions registered, ensure that there are 30.
        assertEquals(30, count);
    }

    @Test
    public void showProcessorWorks() throws Exception {
        // Enumerate some topics that will be re-used
        final String ryaInstance = UUID.randomUUID().toString();
        final UUID queryId = UUID.randomUUID();
        final String statementsTopic = KafkaTopics.statementsTopic(ryaInstance);
        final String resultsTopic = KafkaTopics.queryResultsTopic(queryId);

        // Get the RDF model objects that will be used to build the query.
        final String sparql =
                "PREFIX time: <http://www.w3.org/2006/time#> \n"
                        + "PREFIX tempo: <tag:rya-rdf.org,2015:temporal#> \n"
                        + "PREFIX geo: <http://www.opengis.net/ont/geosparql#>\n"
                        + "PREFIX geof: <" + GEO + ">\n"
                        + "SELECT * \n"
                        + "WHERE { \n"
                        + "  <urn:event1> geo:asWKT ?point .\n"
                        + " FILTER(geof:sfWithin(?point, \"POLYGON((-3 -2, -3 2, 1 2, 1 -2, -3 -2))\"^^geo:wktLiteral)) "
                        + "}";

        // Setup a topology.
        final TopologyBuilder builder = new TopologyFactory().build(sparql, statementsTopic, resultsTopic, new RandomUUIDFactory());

        // Create the statements that will be input into the query.
        final ValueFactory vf = new ValueFactoryImpl();
        final List<VisibilityStatement> statements = getStatements();

        // Make the expected results.
        final Set<VisibilityBindingSet> expected = new HashSet<>();
        final MapBindingSet bs = new MapBindingSet();
        final WKTWriter w = new WKTWriter();
        bs.addBinding("point", vf.createLiteral(w.write(ZERO), GeoConstants.XMLSCHEMA_OGC_WKT));
        expected.add( new VisibilityBindingSet(bs, "a") );

        // Run the test.
        RyaStreamsTestUtil.runStreamProcessingTest(kafka, statementsTopic, resultsTopic, builder, 2000, statements, expected, VisibilityBindingSetDeserializer.class);
    }

    private List<VisibilityStatement> getStatements() throws Exception {
        final List<VisibilityStatement> statements = new ArrayList<>();
        // geo 2x2 points
        statements.add(new VisibilityStatement(statement(ZERO), "a"));
        statements.add(new VisibilityStatement(statement(ONE), "a"));
        return statements;
    }

    private static Statement statement(final Geometry geo) {
        final ValueFactory vf = new ValueFactoryImpl();
        final Resource subject = vf.createURI("urn:event1");
        final URI predicate = GeoConstants.GEO_AS_WKT;
        final WKTWriter w = new WKTWriter();
        final Value object = vf.createLiteral(w.write(geo), GeoConstants.XMLSCHEMA_OGC_WKT);
        return new StatementImpl(subject, predicate, object);
    }
}