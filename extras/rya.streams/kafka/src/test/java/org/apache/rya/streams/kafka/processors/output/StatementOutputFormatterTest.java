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
package org.apache.rya.streams.kafka.processors.output;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.api.model.VisibilityStatement;
import org.apache.rya.streams.kafka.processors.ProcessorResult;
import org.apache.rya.streams.kafka.processors.ProcessorResult.BinaryResult;
import org.apache.rya.streams.kafka.processors.ProcessorResult.BinaryResult.Side;
import org.apache.rya.streams.kafka.processors.ProcessorResult.UnaryResult;
import org.apache.rya.streams.kafka.processors.output.StatementOutputFormatterSupplier.StatementOutputFormatter;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.junit.Test;

/**
 * Unit tests the methods of {@link StatementOutputFormatter}.
 */
public class StatementOutputFormatterTest {

    @Test
    public void unaryResult() {
        // Create the input binding set.
        final ValueFactory vf = SimpleValueFactory.getInstance();
        final MapBindingSet bindingSet = new MapBindingSet();
        bindingSet.addBinding("subject", vf.createIRI("urn:Alice"));
        bindingSet.addBinding("predicate", vf.createIRI("urn:age"));
        bindingSet.addBinding("object", vf.createLiteral(34));
        final VisibilityBindingSet visBs = new VisibilityBindingSet(bindingSet, "a");

        // Mock the processor context that will be invoked.
        final ProcessorContext context = mock(ProcessorContext.class);

        // Run the test.
        final StatementOutputFormatter formatter = new StatementOutputFormatter();
        formatter.init(context);
        formatter.process("key", ProcessorResult.make(new UnaryResult(visBs)));

        // Verify the mock was invoked with the expected output.
        final VisibilityStatement expectedStmt = new VisibilityStatement(vf.createStatement(
                vf.createIRI("urn:Alice"),
                vf.createIRI("urn:age"),
                vf.createLiteral(34)), "a");
        verify(context, times(1)).forward(eq("key"), eq(expectedStmt));
    }

    @Test
    public void binaryResult() {
        // Create the input binding set.
        final ValueFactory vf = SimpleValueFactory.getInstance();
        final MapBindingSet bindingSet = new MapBindingSet();
        bindingSet.addBinding("subject", vf.createIRI("urn:Alice"));
        bindingSet.addBinding("predicate", vf.createIRI("urn:age"));
        bindingSet.addBinding("object", vf.createLiteral(34));
        final VisibilityBindingSet visBs = new VisibilityBindingSet(bindingSet, "a");

        // Mock the processor context that will be invoked.
        final ProcessorContext context = mock(ProcessorContext.class);

        // Run the test.
        final StatementOutputFormatter formatter = new StatementOutputFormatter();
        formatter.init(context);
        formatter.process("key", ProcessorResult.make(new BinaryResult(Side.LEFT, visBs)));

        // Verify the mock was invoked with the expected output.
        final VisibilityStatement expectedStmt = new VisibilityStatement(vf.createStatement(
                vf.createIRI("urn:Alice"),
                vf.createIRI("urn:age"),
                vf.createLiteral(34)), "a");
        verify(context, times(1)).forward(eq("key"), eq(expectedStmt));
    }

    @Test
    public void missingSubject() {
        // Create the input binding set.
        final ValueFactory vf = SimpleValueFactory.getInstance();
        final MapBindingSet bindingSet = new MapBindingSet();
        bindingSet.addBinding("predicate", vf.createIRI("urn:age"));
        bindingSet.addBinding("object", vf.createLiteral(34));
        final VisibilityBindingSet visBs = new VisibilityBindingSet(bindingSet, "a");

        // Mock the processor context that will be invoked.
        final ProcessorContext context = mock(ProcessorContext.class);

        // Run the test.
        final StatementOutputFormatter formatter = new StatementOutputFormatter();
        formatter.init(context);
        formatter.process("key", ProcessorResult.make(new UnaryResult(visBs)));

        // Verify the mock was never invoked.
        verify(context, times(0)).forward(any(), any());
    }

    @Test
    public void subjectWrongType() {
        // Create the input binding set.
        final ValueFactory vf = SimpleValueFactory.getInstance();
        final MapBindingSet bindingSet = new MapBindingSet();
        bindingSet.addBinding("subject", vf.createLiteral("Alice"));
        bindingSet.addBinding("predicate", vf.createIRI("urn:age"));
        bindingSet.addBinding("object", vf.createLiteral(34));
        final VisibilityBindingSet visBs = new VisibilityBindingSet(bindingSet, "a");

        // Mock the processor context that will be invoked.
        final ProcessorContext context = mock(ProcessorContext.class);

        // Run the test.
        final StatementOutputFormatter formatter = new StatementOutputFormatter();
        formatter.init(context);
        formatter.process("key", ProcessorResult.make(new UnaryResult(visBs)));

        // Verify the mock was never invoked.
        verify(context, times(0)).forward(any(), any());
    }

    @Test
    public void missingPredicate() {
        // Create the input binding set.
        final ValueFactory vf = SimpleValueFactory.getInstance();
        final MapBindingSet bindingSet = new MapBindingSet();
        bindingSet.addBinding("subject", vf.createIRI("urn:Alice"));
        bindingSet.addBinding("object", vf.createLiteral(34));
        final VisibilityBindingSet visBs = new VisibilityBindingSet(bindingSet, "a");

        // Mock the processor context that will be invoked.
        final ProcessorContext context = mock(ProcessorContext.class);

        // Run the test.
        final StatementOutputFormatter formatter = new StatementOutputFormatter();
        formatter.init(context);
        formatter.process("key", ProcessorResult.make(new UnaryResult(visBs)));

        // Verify the mock was never invoked.
        verify(context, times(0)).forward(any(), any());
    }

    @Test
    public void predicateWrongType() {
        // Create the input binding set.
        final ValueFactory vf = SimpleValueFactory.getInstance();
        final MapBindingSet bindingSet = new MapBindingSet();
        bindingSet.addBinding("subject", vf.createIRI("urn:Alice"));
        bindingSet.addBinding("predicate", vf.createLiteral("age"));
        bindingSet.addBinding("object", vf.createLiteral(34));
        final VisibilityBindingSet visBs = new VisibilityBindingSet(bindingSet, "a");

        // Mock the processor context that will be invoked.
        final ProcessorContext context = mock(ProcessorContext.class);

        // Run the test.
        final StatementOutputFormatter formatter = new StatementOutputFormatter();
        formatter.init(context);
        formatter.process("key", ProcessorResult.make(new UnaryResult(visBs)));

        // Verify the mock was never invoked.
        verify(context, times(0)).forward(any(), any());
    }

    @Test
    public void missingObject() {
        // Create the input binding set.
        final ValueFactory vf = SimpleValueFactory.getInstance();
        final MapBindingSet bindingSet = new MapBindingSet();
        bindingSet.addBinding("subject", vf.createIRI("urn:Alice"));
        bindingSet.addBinding("predicate", vf.createIRI("urn:age"));
        final VisibilityBindingSet visBs = new VisibilityBindingSet(bindingSet, "a");

        // Mock the processor context that will be invoked.
        final ProcessorContext context = mock(ProcessorContext.class);

        // Run the test.
        final StatementOutputFormatter formatter = new StatementOutputFormatter();
        formatter.init(context);
        formatter.process("key", ProcessorResult.make(new UnaryResult(visBs)));

        // Verify the mock was never invoked.
        verify(context, times(0)).forward(any(), any());
    }
}