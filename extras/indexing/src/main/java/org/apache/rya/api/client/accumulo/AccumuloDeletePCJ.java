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
import org.apache.rya.indexing.pcj.fluo.api.DeletePcj;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage.PCJStorageException;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPcjStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

import org.apache.fluo.api.client.FluoClient;
import org.apache.rya.api.client.DeletePCJ;
import org.apache.rya.api.client.GetInstanceDetails;
import org.apache.rya.api.client.InstanceDoesNotExistException;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.api.instance.RyaDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails.FluoDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails.PCJDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails.PCJDetails.PCJUpdateStrategy;

/**
 * An Accumulo implementation of the {@link DeletePCJ} command.
 */
@ParametersAreNonnullByDefault
public class AccumuloDeletePCJ extends AccumuloCommand implements DeletePCJ {

    private static final Logger log = LoggerFactory.getLogger(AccumuloDeletePCJ.class);

    private final GetInstanceDetails getInstanceDetails;

    /**
     * Constructs an instance of {@link AccumuloDeletePCJ}.
     *
     * @param connectionDetails - Details about the values that were used to create the connector to the cluster. (not null)
     * @param connector - Provides programatic access to the instance of Accumulo that hosts Rya instance. (not null)
     */
    public AccumuloDeletePCJ(final AccumuloConnectionDetails connectionDetails, final Connector connector) {
        super(connectionDetails, connector);
        getInstanceDetails = new AccumuloGetInstanceDetails(connectionDetails, connector);
    }

    @Override
    public void deletePCJ(final String instanceName, final String pcjId) throws InstanceDoesNotExistException, RyaClientException {
        requireNonNull(instanceName);
        requireNonNull(pcjId);

        final Optional<RyaDetails> originalDetails = getInstanceDetails.getDetails(instanceName);
        final boolean ryaInstanceExists = originalDetails.isPresent();
        if(!ryaInstanceExists) {
            throw new InstanceDoesNotExistException(String.format("The '%s' instance of Rya does not exist.", instanceName));
        }

        final boolean pcjIndexingEnabeld = originalDetails.get().getPCJIndexDetails().isEnabled();
        if(!pcjIndexingEnabeld) {
            throw new RyaClientException(String.format("The '%s' instance of Rya does not have PCJ Indexing enabled.", instanceName));
        }

        final boolean pcjExists = originalDetails.get().getPCJIndexDetails().getPCJDetails().containsKey( pcjId );
        if(!pcjExists) {
            throw new RyaClientException(String.format("The '%s' instance of Rya does not have PCJ with ID '%s'.", instanceName, pcjId));
        }

        // If the PCJ was being maintained by a Fluo application, then stop that process.
        final PCJIndexDetails pcjIndexDetails  = originalDetails.get().getPCJIndexDetails();
        final PCJDetails droppedPcjDetails = pcjIndexDetails.getPCJDetails().get( pcjId );
        if(droppedPcjDetails.getUpdateStrategy().isPresent()) {
            if(droppedPcjDetails.getUpdateStrategy().get() == PCJUpdateStrategy.INCREMENTAL) {
                final Optional<FluoDetails> fluoDetailsHolder = pcjIndexDetails.getFluoDetails();

                if(fluoDetailsHolder.isPresent()) {
                    final String fluoAppName = pcjIndexDetails.getFluoDetails().get().getUpdateAppName();
                    stopUpdatingPCJ(fluoAppName, pcjId);
                } else {
                    log.error(String.format("Could not stop the Fluo application from updating the PCJ because the Fluo Details are " +
                            "missing for the Rya instance named '%s'.", instanceName));
                }
            }
        }

        // Drop the table that holds the PCJ results from Accumulo.
        final PrecomputedJoinStorage pcjs = new AccumuloPcjStorage(getConnector(), instanceName);
        try {
            pcjs.dropPcj(pcjId);
        } catch (final PCJStorageException e) {
            throw new RyaClientException("Could not drop the PCJ's table from Accumulo.", e);
        }
    }

    private void stopUpdatingPCJ(final String fluoAppName, final String pcjId) {
        requireNonNull(fluoAppName);
        requireNonNull(pcjId);

        // Connect to the Fluo application that is updating this instance's PCJs.
        final AccumuloConnectionDetails cd = super.getAccumuloConnectionDetails();
        final FluoClient fluoClient = new FluoClientFactory().connect(
                cd.getUsername(),
                new String(cd.getPassword()),
                cd.getInstanceName(),
                cd.getZookeepers(),
                fluoAppName);

        // Delete the PCJ from the Fluo App.
        new DeletePcj(1000).deletePcj(fluoClient, pcjId);
    }
}
