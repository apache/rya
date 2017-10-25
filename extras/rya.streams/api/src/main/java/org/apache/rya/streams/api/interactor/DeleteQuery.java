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
package org.apache.rya.streams.api.interactor;

import java.util.UUID;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Deletes a SPARQL Query that is being processed by Rya Streams from the system. That query will no longer be processed.
 */
@DefaultAnnotation(NonNull.class)
public interface DeleteQuery {

    /**
     * Delete a SPARQL Query that is being processed by Rya Streams from the system.
     *
     * @param queryId - Identifies which query will be removed. (not null)
     * @throws RyaStreamsException The query could not be removed from the system.
     */
    public void delete(final UUID queryId) throws RyaStreamsException;
}