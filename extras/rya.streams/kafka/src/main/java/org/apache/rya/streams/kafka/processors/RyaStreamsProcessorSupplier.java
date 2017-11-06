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

import org.apache.kafka.streams.processor.ProcessorSupplier;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A {@link ProcessorSupplier} that should be implemented for each {@link RyaStreamsProcessor} that is implemented.
 */
@DefaultAnnotation(NonNull.class)
public abstract class RyaStreamsProcessorSupplier implements ProcessorSupplier<Object, ProcessorResult> {

    private final ProcessorResultFactory resultFactory;

    /**
     * Constructs an instance of {@link RyaStreamsProcessorSupplier}.
     *
     * @param resultFactory - The {@link ProcessorResultFactory} that will be used by processors created by this class. (not null)
     */
    public RyaStreamsProcessorSupplier(final ProcessorResultFactory resultFactory) {
        this.resultFactory = requireNonNull(resultFactory);
    }

    /**
     * @return The {@link ProcessorResultFactory} that will be used by processors created by this class. (not null)
     */
    public ProcessorResultFactory getResultFactory() {
        return resultFactory;
    }
}