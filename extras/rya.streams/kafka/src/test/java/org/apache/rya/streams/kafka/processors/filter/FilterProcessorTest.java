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

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.rya.api.function.Filter.FilterEvaluator;
import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.streams.kafka.RdfTestUtil;
import org.apache.rya.streams.kafka.processors.ProcessorResult;
import org.apache.rya.streams.kafka.processors.ProcessorResult.UnaryResult;
import org.apache.rya.streams.kafka.processors.filter.FilterProcessorSupplier.FilterProcessor;
import org.junit.Test;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.impl.MapBindingSet;

/**
 * Unit tests the methods of {@link FilterProcessor}.
 */
public class FilterProcessorTest {

    @Test
    public void showFilterFunctionIsCalled() throws Exception {
        // Read the filter object from a SPARQL query.
        final Filter filter = RdfTestUtil.getFilter(
                "SELECT * " +
                "WHERE { " +
                    "FILTER(?age < 10)" +
                    "?person <urn:age> ?age " +
                "}");

        // Create a Binding Set that will be passed into the Filter function based on the where clause.
        final ValueFactory vf = new ValueFactoryImpl();
        final MapBindingSet bs = new MapBindingSet();
        bs.addBinding("person", vf.createURI("urn:Alice"));
        bs.addBinding("age", vf.createLiteral(9));
        final VisibilityBindingSet inputVisBs = new VisibilityBindingSet(bs, "a");

        // Mock the processor context that will be invoked.
        final ProcessorContext context = mock(ProcessorContext.class);

        // Run the test.
        final FilterProcessor processor = new FilterProcessor(
                FilterEvaluator.make(filter),
                result -> ProcessorResult.make(new UnaryResult(result)));
        processor.init(context);
        processor.process("key", ProcessorResult.make(new UnaryResult(inputVisBs)));

        // Verify the binding set was passed through.
        verify(context, times(1)).forward(eq("key"), eq(ProcessorResult.make(new UnaryResult(inputVisBs))));

    }
}