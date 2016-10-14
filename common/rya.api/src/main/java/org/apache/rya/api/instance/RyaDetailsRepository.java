package org.apache.rya.api.instance;

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

import javax.annotation.ParametersAreNonnullByDefault;

/**
 * Provides access to the {@link RyaDetails} information that describes
 * an instance of Rya.
 */
@ParametersAreNonnullByDefault
public interface RyaDetailsRepository {

    /**
     * Check whether the details for this instance of Rya have been initialized or not.
     *
     * @return {@code true} if it has been initialized; otherwise {@code false}.
     * @throws RyaDetailsRepositoryException Something caused this operation to fail.
     */
    public boolean isInitialized() throws RyaDetailsRepositoryException;

    /**
     * Initializes the {@link RyaDetails} that is stored for an instance of Rya.
     *
     * @param details - A Rya instance's details at installation time. (not null)
     * @throws AlreadyInitializedException This repository has already been initialized.
     * @throws RyaDetailsRepositoryException Something caused this operation to fail.
     */
    public void initialize(RyaDetails details) throws AlreadyInitializedException, RyaDetailsRepositoryException;

    /**
     * Get the {@link RyaDetails} that describe this instance of Rya.
     *
     * @return The details that describe this instance of Rya.
     * @throws NotInitializedException The Rya instance's details have not been initialized yet.
     * @throws RyaDetailsRepositoryException Something caused this operation to fail.
     */
    public RyaDetails getRyaInstanceDetails() throws NotInitializedException, RyaDetailsRepositoryException;

    /**
     * Update the {@link RyaDetails} that describe this instance of Rya.
     *
     * @param oldDetails - The copy of the details that have been updated. (not null)
     * @param newDetails - The updated details. (not null)
     * @throws NotInitializedException The Rya instance's details have not been initialized yet.
     * @throws ConcurrentUpdateException An update couldn't be performed because
     * the old state of the object no longer matches the current state.
     * @throws RyaDetailsRepositoryException Something caused this operation to fail.
     */
    public void update(RyaDetails oldDetails, RyaDetails newDetails) throws NotInitializedException, ConcurrentUpdateException, RyaDetailsRepositoryException;

    /**
     * The root exception of all {@link RyaDetailsRepository} operations.
     */
    public static class RyaDetailsRepositoryException extends Exception {
        private static final long serialVersionUID = 1L;

        public RyaDetailsRepositoryException(final String message) {
            super(message);
        }

        public RyaDetailsRepositoryException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * You can not initialize a {@link RyaDetailsRepository} that has already
     * been initialized.
     */
    public static class AlreadyInitializedException extends RyaDetailsRepositoryException {
        private static final long serialVersionUID = 1L;

        public AlreadyInitializedException(final String message) {
            super(message);
        }
    }

    /**
     * Some methods of {@link RyaDetailsRepository} may only be invoked after
     * it has been initialized.
     */
    public static class NotInitializedException extends RyaDetailsRepositoryException {
        private static final long serialVersionUID = 1L;

        public NotInitializedException(final String message) {
            super(message);
        }

        public NotInitializedException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * An update couldn't be performed because the old state of the object no
     * longer matches the current state.
     */
    public static class ConcurrentUpdateException extends RyaDetailsRepositoryException {
        private static final long serialVersionUID = 1L;

        public ConcurrentUpdateException(final String message) {
            super(message);
        }
    }
}