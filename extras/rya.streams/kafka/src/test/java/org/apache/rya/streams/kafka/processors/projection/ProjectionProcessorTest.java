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

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.rya.api.function.projection.ProjectionEvaluator;
import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.streams.kafka.RdfTestUtil;
import org.apache.rya.streams.kafka.processors.ProcessorResult;
import org.apache.rya.streams.kafka.processors.ProcessorResult.UnaryResult;
import org.apache.rya.streams.kafka.processors.projection.ProjectionProcessorSupplier.ProjectionProcessor;
import org.junit.Test;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.impl.MapBindingSet;

/**
 * Unit tests the methods of {@link ProjectionProcessor}.
 */
public class ProjectionProcessorTest {

    @Test
    public void showProjectionFunctionIsCalled() throws Exception {
        // A query whose projection does not change anything.
        final Projection projection = RdfTestUtil.getProjection(
                "SELECT (?person AS ?p) (?employee AS ?e) ?business " +
                "WHERE { " +
                    "?person <urn:talksTo> ?employee . " +
                    "?employee <urn:worksAt> ?business . " +
                "}");

        // Create a Binding Set that contains the result of the WHERE clause.
        final ValueFactory vf = new ValueFactoryImpl();
        final MapBindingSet inputBs = new MapBindingSet();
        inputBs.addBinding("person", vf.createURI("urn:Alice"));
        inputBs.addBinding("employee", vf.createURI("urn:Bob"));
        inputBs.addBinding("business", vf.createURI("urn:TacoJoint"));
        final VisibilityBindingSet inputVisBs = new VisibilityBindingSet(inputBs, "a");

        // The expected binding set changes the "person" binding name to "p" and "employee" to "e".
        final MapBindingSet expectedBs = new MapBindingSet();
        expectedBs.addBinding("p", vf.createURI("urn:Alice"));
        expectedBs.addBinding("e", vf.createURI("urn:Bob"));
        expectedBs.addBinding("business", vf.createURI("urn:TacoJoint"));
        final VisibilityBindingSet expectedVisBs = new VisibilityBindingSet(expectedBs, "a");

        // Show it resulted in the correct output BindingSet.
        final ProcessorContext context = mock(ProcessorContext.class);
        final ProjectionProcessor processor = new ProjectionProcessor(
                ProjectionEvaluator.make(projection),
                result -> ProcessorResult.make(new UnaryResult(result)));
        processor.init(context);

        processor.process("key", ProcessorResult.make(new UnaryResult(inputVisBs)));

        // Verify the expected binding set was emitted.
        final ProcessorResult expected = ProcessorResult.make(new UnaryResult(expectedVisBs));
        verify(context, times(1)).forward(eq("key"), eq(expected));
    }
}