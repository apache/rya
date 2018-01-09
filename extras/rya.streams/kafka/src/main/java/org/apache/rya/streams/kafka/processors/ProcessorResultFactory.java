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

import org.apache.rya.api.model.VisibilityBindingSet;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Augments a {@link VisibilityBidingSet] that needs to be output by a {@link RyaStreamsProcessor}
 * so that it has all of the information the downstream processor needs as well as whatever
 * key the downstream processor requires.
 */
@DefaultAnnotation(NonNull.class)
public interface ProcessorResultFactory {

    /**
     * Augments a {@link VisibilityBidingSet] that needs to be output by a {@link RyaStreamsProcessor}
     * so that it has all of the information the downstream processor needs as well as whatever
     * key the downstream processor requires.
     *
     * @param result - The result that is being emitted. (not null)
     * @return A {@link ProcessorResult} that is formatted for the downstream processor.
     */
    public ProcessorResult make(VisibilityBindingSet result);
}