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
package org.apache.rya.api.client;

import static java.util.Objects.requireNonNull;

import org.apache.rya.api.instance.RyaDetails;
import org.apache.rya.api.instance.RyaDetails.RyaStreamsDetails;
import org.apache.rya.api.instance.RyaDetailsRepository;
import org.apache.rya.api.instance.RyaDetailsRepository.RyaDetailsRepositoryException;
import org.apache.rya.api.instance.RyaDetailsUpdater;
import org.apache.rya.api.instance.RyaDetailsUpdater.RyaDetailsMutator.CouldNotApplyMutationException;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A base class that implements the core functionality of the {@link SetRyaStreamsConfiguration} interactor.
 * Subclasses just need to implement {@link #getRyaDetailsRepo(String)} so that the common code may update
 * any implementation of that repository.
 */
@DefaultAnnotation(NonNull.class)
public abstract class SetRyaStreamsConfigurationBase implements SetRyaStreamsConfiguration {

    private final InstanceExists instanceExists;

    /**
     * Constructs an instance of {@link SetRyaStreamsConfigurationBase}.
     *
     * @param instanceExists - The interactor used to verify Rya instances exist. (not null)
     */
    public SetRyaStreamsConfigurationBase(final InstanceExists instanceExists) {
        this.instanceExists = requireNonNull(instanceExists);
    }

    /**
     * Get a {@link RyaDetailsRepository} that is connected to a specific instance of Rya.
     *
     * @param ryaInstance - The Rya instance the repository must be connected to. (not null)
     * @return A {@link RyaDetailsRepository} connected to the specified Rya instance.
     */
    protected abstract RyaDetailsRepository getRyaDetailsRepo(String ryaInstance);

    @Override
    public void setRyaStreamsConfiguration(final String ryaInstance, final RyaStreamsDetails streamsDetails) throws InstanceDoesNotExistException, RyaClientException{
        requireNonNull(ryaInstance);
        requireNonNull(streamsDetails);

        // Verify the Rya Instance exists.
        if(!instanceExists.exists(ryaInstance)) {
            throw new InstanceDoesNotExistException("There is no Rya instance named '" + ryaInstance + "' in this storage.");
        }

        // Update the old details object using the provided Rya Streams details.
        final RyaDetailsRepository repo = getRyaDetailsRepo(ryaInstance);
        try {
            new RyaDetailsUpdater(repo).update(oldDetails -> {
                final RyaDetails.Builder builder = RyaDetails.builder(oldDetails);
                builder.setRyaStreamsDetails(streamsDetails);
                return builder.build();
            });
        } catch (CouldNotApplyMutationException | RyaDetailsRepositoryException e) {
            throw new RyaClientException("Unable to update which Rya Streams subsystem is used by the '" +
                    ryaInstance + "' Rya instance.", e);
        }
    }
}