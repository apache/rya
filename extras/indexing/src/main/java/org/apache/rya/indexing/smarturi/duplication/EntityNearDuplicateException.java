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
package org.apache.rya.indexing.smarturi.duplication;

import org.apache.rya.indexing.entity.model.Entity;
import org.apache.rya.indexing.entity.storage.EntityStorage.EntityStorageException;

/**
 * An {@link Entity} could not be created because another entity is a nearly
 * identical duplicate based on the configured tolerances.
 */
public class EntityNearDuplicateException extends EntityStorageException {
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance of {@link EntityNearDuplicateException}.
     * @param message the message to be displayed by the exception.
     */
    public EntityNearDuplicateException(final String message) {
        super(message);
    }

    /**
     * Creates a new instance of {@link EntityNearDuplicateException}.
     * @param message the message to be displayed by the exception.
     * @param throwable the source {#link Throwable} cause of the exception.
     */
    public EntityNearDuplicateException(final String message, final Throwable throwable) {
        super(message, throwable);
    }
}