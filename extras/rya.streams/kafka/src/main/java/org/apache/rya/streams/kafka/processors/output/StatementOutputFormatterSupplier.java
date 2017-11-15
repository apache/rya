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

import java.util.Collection;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.api.model.VisibilityStatement;
import org.apache.rya.streams.kafka.processors.ProcessorResult;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

import com.google.common.collect.Sets;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Supplies instance of {@link StatementOutputFormatter}
 */
@DefaultAnnotation(NonNull.class)
public class StatementOutputFormatterSupplier implements ProcessorSupplier<Object, ProcessorResult> {

    @Override
    public Processor<Object, ProcessorResult> get() {
        return new StatementOutputFormatter();
    }

    /**
     * Converts {@link VisiblityBindingSet}s that contain a "subject", "predicate", and "object" binding into a
     * {@link VisibilityStatement} and then forwards that to the downstream processor.
     */
    @DefaultAnnotation(NonNull.class)
    public static final class StatementOutputFormatter implements Processor<Object, ProcessorResult> {

        private static final ValueFactory VF = new ValueFactoryImpl();
        private static final Collection<String> REQURIED_BINDINGS = Sets.newHashSet("subject", "predicate", "object");

        private ProcessorContext processorContext;

        @Override
        public void init(final ProcessorContext context) {
            processorContext = context;
        }

        @Override
        public void process(final Object key, final ProcessorResult value) {
            VisibilityBindingSet result = null;
            switch(value.getType()) {
                case UNARY:
                    result = value.getUnary().getResult();
                    break;

                case BINARY:
                    result = value.getBinary().getResult();
                    break;
            }

            if(result != null && result.getBindingNames().containsAll(REQURIED_BINDINGS)) {
                // Make sure the Subject is the correct type.
                final Value subjVal = result.getValue("subject");
                if(!(subjVal instanceof Resource)) {
                    return;
                }

                // Make sure the Predicate is the correct type.
                final Value predVal = result.getValue("predicate");
                if(!(predVal instanceof URI)) {
                    return;
                }

                // Forward the visibility statement.
                final Statement statement = VF.createStatement(
                        (Resource) subjVal,
                        (URI) predVal,
                        result.getValue("object"));
                processorContext.forward(key, new VisibilityStatement(statement, result.getVisibility()));
            }
        }

        @Override
        public void punctuate(final long timestamp) {
            // Do nothing.
        }

        @Override
        public void close() {
            // Do nothing.
        }
    }
}