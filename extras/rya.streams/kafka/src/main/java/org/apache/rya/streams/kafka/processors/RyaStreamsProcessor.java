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
package org.apache.rya.streams.kafka.processors;

import static java.util.Objects.requireNonNull;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * All Rya Streams {@link Processor} implementations emit {@link ProcessorResult} objects. This
 * abstract class holds onto the {@link ProcessorResultFactory} that is used to augment results
 * before sending them to the downstream processor via {@link ProcessorContext#forward(Object, Object)}.
 */
@DefaultAnnotation(NonNull.class)
public abstract class RyaStreamsProcessor implements Processor<Object, ProcessorResult> {

    private final ProcessorResultFactory resultFactory;

    /**
     * Constructs an instance of {@link RyaStreamsProcessor}.
     *
     * @param resultFactory - The {@link ProcessorResultFactory} the child class will used to format results
     *   before sending them to {@link ProcessorContext#forward(Object, Object)}. (not null)
     */
    public RyaStreamsProcessor(final ProcessorResultFactory resultFactory) {
        this.resultFactory = requireNonNull(resultFactory);
    }

    /**
     * @return The {@link ProcessorResultFactory} the child class will used to format results
     *   before sending them to {@link ProcessorContext#forward(Object, Object)}.
     */
    public ProcessorResultFactory getResultFactory() {
        return resultFactory;
    }
}