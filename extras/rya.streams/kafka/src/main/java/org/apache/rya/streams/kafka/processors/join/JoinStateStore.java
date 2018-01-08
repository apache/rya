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
package org.apache.rya.streams.kafka.processors.join;

import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.api.utils.CloseableIterator;
import org.apache.rya.streams.kafka.processors.ProcessorResult.BinaryResult;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Provides a mechanism for storing {@link VisibilityBindingSet}s that have been emitted from either side of
 * a Join and a way to fetch all {@link VisibilityBindingSet}s that join with it from the other side.
 */
@DefaultAnnotation(NonNull.class)
public interface JoinStateStore {

    /**
     * Store a {@link VisibilityBindingSet} based on the side it was emitted from.
     *
     * @param result - The result whose value will be stored. (not null)
     */
    public void store(BinaryResult result);

    /**
     * Get the previously stored {@link VisibilityBindingSet}s that join with the provided result.
     *
     * @param result - The value that will be joined with. (not null)
     * @return The {@link VisibilityBinidngSet}s that join with {@code result}.
     */
    public CloseableIterator<VisibilityBindingSet> getJoinedValues(BinaryResult result);
}