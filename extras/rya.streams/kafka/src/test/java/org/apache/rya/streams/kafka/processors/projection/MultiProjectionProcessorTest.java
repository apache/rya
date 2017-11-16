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

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.rya.api.function.projection.MultiProjectionEvaluator;
import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.streams.kafka.RdfTestUtil;
import org.apache.rya.streams.kafka.processors.ProcessorResult;
import org.apache.rya.streams.kafka.processors.ProcessorResult.ResultType;
import org.apache.rya.streams.kafka.processors.ProcessorResult.UnaryResult;
import org.apache.rya.streams.kafka.processors.projection.MultiProjectionProcessorSupplier.MultiProjectionProcessor;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.openrdf.model.BNode;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.algebra.MultiProjection;
import org.openrdf.query.impl.MapBindingSet;

/**
 * Unit test the methods of {@link MultiProjectionProcessor}.
 */
public class MultiProjectionProcessorTest {

    @Test
    public void showProjectionFunctionIsCalled() throws Exception {
        // The SPARQL that will define the projection.
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

        // Create a Binding Set that contains the result of the WHERE clause.
        final ValueFactory vf = new ValueFactoryImpl();
        final MapBindingSet inputBs = new MapBindingSet();
        inputBs.addBinding("location", vf.createURI("urn:corner1"));
        inputBs.addBinding("direction", vf.createURI("urn:NW"));
        final VisibilityBindingSet inputVisBs = new VisibilityBindingSet(inputBs, "a|b");

        // Make the expected results.
        final Set<VisibilityBindingSet> expected = new HashSet<>();
        final String blankNodeId = UUID.randomUUID().toString();
        final BNode blankNode = vf.createBNode(blankNodeId);

        MapBindingSet expectedBs = new MapBindingSet();
        expectedBs.addBinding("subject", blankNode);
        expectedBs.addBinding("predicate", RDF.TYPE);
        expectedBs.addBinding("object", vf.createURI("urn:movementObservation"));
        expected.add(new VisibilityBindingSet(expectedBs, "a|b"));

        expectedBs = new MapBindingSet();
        expectedBs.addBinding("subject", blankNode);
        expectedBs.addBinding("predicate", vf.createURI("urn:direction"));
        expectedBs.addBinding("object", vf.createURI("urn:NW"));
        expected.add(new VisibilityBindingSet(expectedBs, "a|b"));

        expectedBs = new MapBindingSet();
        expectedBs.addBinding("subject", blankNode);
        expectedBs.addBinding("predicate", vf.createURI("urn:location"));
        expectedBs.addBinding("object", vf.createURI("urn:corner1"));
        expected.add(new VisibilityBindingSet(expectedBs, "a|b"));

        // Mock the processor context that will be invoked.
        final ProcessorContext context = mock(ProcessorContext.class);

        // Run the test.
        final MultiProjectionProcessor processor = new MultiProjectionProcessor(
                MultiProjectionEvaluator.make(multiProjection, () -> blankNodeId),
                result -> ProcessorResult.make(new UnaryResult(result)));
        processor.init(context);
        processor.process("key", ProcessorResult.make(new UnaryResult(inputVisBs)));

        final ArgumentCaptor<ProcessorResult> results = ArgumentCaptor.forClass(ProcessorResult.class);
        verify(context, times(3)).forward(any(), results.capture());

        final Set<VisibilityBindingSet> resultBindingSets = results.getAllValues().stream()
                .map(result -> {
                    return (result.getType() == ResultType.UNARY) ? result.getUnary().getResult() : result.getBinary().getResult();
                })
                .collect(Collectors.toSet());

        assertEquals(expected, resultBindingSets);
    }
}