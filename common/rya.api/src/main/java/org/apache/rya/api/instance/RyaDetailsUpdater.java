/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.api.instance;

import static java.util.Objects.requireNonNull;

import javax.annotation.ParametersAreNonnullByDefault;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.rya.api.instance.RyaDetailsRepository.ConcurrentUpdateException;
import org.apache.rya.api.instance.RyaDetailsRepository.RyaDetailsRepositoryException;
import org.apache.rya.api.instance.RyaDetailsUpdater.RyaDetailsMutator.CouldNotApplyMutationException;

/**
 * A utility that will attempt to commit a change to a Rya instance's details
 * until it either takes or the mutation can no longer be applied. This can
 * can be used in place of boilerplate code that handles the concurrent nature
 * of details updates.
 */
@ParametersAreNonnullByDefault
public class RyaDetailsUpdater {
    private static final Logger log = LoggerFactory.getLogger(RyaDetailsUpdater.class);

    /**
     * Applies a mutation to a an instance of {@link RyaDetails}.
     */
    @ParametersAreNonnullByDefault
    public static interface RyaDetailsMutator {

        /**
         * Applies a mutation to a {@link RyaDetails} object.
         *
         * @param originalDetails - The {@link RyaDetails} that were just fetched
         *   from the {@link RyaDetailsRepository}.
         * @return The updated details.
         * @throws CouldNotApplyMutationException The mutation could not be applied
         *   to the supplied {@link RyaDetails}. This can be used to break out of
         *   the update loop when the details are in a state the mutation can not
         *   be applied to.
         */
        public RyaDetails mutate(RyaDetails originalDetails) throws CouldNotApplyMutationException;

        /**
         * Indicates a mutation could not be applied to an instance of {@link RyaDetails}.
         */
        public static class CouldNotApplyMutationException extends Exception {
            private static final long serialVersionUID = 1L;

            /**
             * Constructs a new exception with the specified detail message.  The
             * cause is not initialized, and may subsequently be initialized by
             * a call to {@link #initCause(Throwable)}.
             *
             * @param   message   the detail message. The detail message is saved for
             *          later retrieval by the {@link #getMessage()} method.
             */
            public CouldNotApplyMutationException(final String message) {
                super(message);
            }
        }
    }

    private final RyaDetailsRepository repo;

    /**
     * Constructs an instance of {@link RyaDetailsUpdater}.
     *
     * @param repo - The repository that this updater will commit changes to. (not null)
     */
    public RyaDetailsUpdater(final RyaDetailsRepository repo) {
        this.repo = requireNonNull(repo);
    }

    /**
     * Updates an instance of {@link RyaDetails} by fetching the details from
     * the {@link RyaDetailsRepository} that was supplied at construction time,
     * applying the {@link RyaDetailsMutator} to them, and them committing the
     * change. If the update failed because of a concurrent update problem, then
     * it will try again. The updater will continue to do this until the changes
     * take or another exception type is thrown.
     *
     * @param mutator - The mutator that will be used to apply a chagne to the
     *   repository's {@link RyaDetails}.
     * @throws RyaDetailsRepositoryException A repository side error caused
     *   the update to fail. This could be a communications problem with the
     *   store repository, uninitialized, etc.
     * @throws CouldNotApplyMutationException An application side error caused
     *   the update to fail. This is thrown by the mutator when the details
     *   would be in an illegal state if the mutation were to be applied.
     */
    public void update(final RyaDetailsMutator mutator) throws RyaDetailsRepositoryException, CouldNotApplyMutationException {
        requireNonNull(mutator);

        boolean updated = false;
        while(!updated) {
            try {
                final RyaDetails original = repo.getRyaInstanceDetails();
                final RyaDetails mutated = mutator.mutate(original);
                repo.update(original, mutated);
                updated = true;
            } catch(final ConcurrentUpdateException e) {
                log.debug("Failed to update the details because another application changed them. Trying again.", e);
            }
        }
    }
}