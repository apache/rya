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
package org.apache.rya.api.client.accumulo;

import static java.util.Objects.requireNonNull;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.accumulo.core.client.Connector;

import com.google.common.base.Optional;

import org.apache.rya.accumulo.instance.AccumuloRyaInstanceDetailsRepository;
import org.apache.rya.api.client.GetInstanceDetails;
import org.apache.rya.api.client.InstanceDoesNotExistException;
import org.apache.rya.api.client.InstanceExists;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.api.instance.RyaDetails;
import org.apache.rya.api.instance.RyaDetailsRepository;
import org.apache.rya.api.instance.RyaDetailsRepository.NotInitializedException;
import org.apache.rya.api.instance.RyaDetailsRepository.RyaDetailsRepositoryException;

/**
 * An Accumulo implementation of the {@link GetInstanceDetails} command.
 */
@ParametersAreNonnullByDefault
public class AccumuloGetInstanceDetails extends AccumuloCommand implements GetInstanceDetails {

    private final InstanceExists instanceExists;

    /**
     * Constructs an instance of {@link AccumuloGetInstanceDetails}.
     *
     * @param connectionDetails - Details about the values that were used to create the connector to the cluster. (not null)
     * @param connector - Provides programatic access to the instance of Accumulo that hosts Rya instance. (not null)
     */
    public AccumuloGetInstanceDetails(final AccumuloConnectionDetails connectionDetails, final Connector connector) {
        super(connectionDetails, connector);
        instanceExists = new AccumuloInstanceExists(connectionDetails, connector);
    }

    @Override
    public Optional<RyaDetails> getDetails(final String instanceName) throws InstanceDoesNotExistException, RyaClientException {
        requireNonNull(instanceName);

        // Ensure the Rya instance exists.
        if(!instanceExists.exists(instanceName)) {
            throw new InstanceDoesNotExistException(String.format("There is no Rya instance named '%s'.", instanceName));
        }

        // If the instance has details, then return them.
        final RyaDetailsRepository detailsRepo = new AccumuloRyaInstanceDetailsRepository(getConnector(), instanceName);
        try {
            return Optional.of( detailsRepo.getRyaInstanceDetails() );
        } catch (final NotInitializedException e) {
            return Optional.absent();
        } catch (final RyaDetailsRepositoryException e) {
            throw new RyaClientException("Could not fetch the Rya instance's details.", e);
        }
    }
}