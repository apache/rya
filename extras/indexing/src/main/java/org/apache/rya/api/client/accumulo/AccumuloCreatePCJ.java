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

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Objects.requireNonNull;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.accumulo.core.client.Connector;
import org.apache.rya.indexing.pcj.storage.PcjException;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage.PCJStorageException;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPcjStorage;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;

import com.google.common.base.Optional;

import org.apache.fluo.api.client.FluoClient;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.accumulo.instance.AccumuloRyaInstanceDetailsRepository;
import org.apache.rya.api.client.CreatePCJ;
import org.apache.rya.api.client.GetInstanceDetails;
import org.apache.rya.api.client.InstanceDoesNotExistException;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.api.instance.RyaDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails.FluoDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails.PCJDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails.PCJDetails.PCJUpdateStrategy;
import org.apache.rya.api.instance.RyaDetailsRepository;
import org.apache.rya.api.instance.RyaDetailsRepository.RyaDetailsRepositoryException;
import org.apache.rya.api.instance.RyaDetailsUpdater;
import org.apache.rya.api.instance.RyaDetailsUpdater.RyaDetailsMutator;
import org.apache.rya.api.instance.RyaDetailsUpdater.RyaDetailsMutator.CouldNotApplyMutationException;
import org.apache.rya.rdftriplestore.RdfCloudTripleStore;
import org.apache.rya.rdftriplestore.RyaSailRepository;

/**
 * An Accumulo implementation of the {@link CreatePCJ} command.
 */
@ParametersAreNonnullByDefault
public class AccumuloCreatePCJ extends AccumuloCommand implements CreatePCJ {

    private final GetInstanceDetails getInstanceDetails;

    /**
     * Constructs an instance of {@link AccumuloCreatePCJ}.
     *
     * @param connectionDetails - Details about the values that were used to create the connector to the cluster. (not null)
     * @param connector - Provides programatic access to the instance of Accumulo that hosts Rya instance. (not null)
     */
    public AccumuloCreatePCJ(final AccumuloConnectionDetails connectionDetails, final Connector connector) {
        super(connectionDetails, connector);
        getInstanceDetails = new AccumuloGetInstanceDetails(connectionDetails, connector);
    }

    @Override
    public String createPCJ(final String instanceName, final String sparql) throws InstanceDoesNotExistException, RyaClientException {
        requireNonNull(instanceName);
        requireNonNull(sparql);

        final Optional<RyaDetails> ryaDetailsHolder = getInstanceDetails.getDetails(instanceName);
        final boolean ryaInstanceExists = ryaDetailsHolder.isPresent();
        if(!ryaInstanceExists) {
            throw new InstanceDoesNotExistException(String.format("The '%s' instance of Rya does not exist.", instanceName));
        }

        final PCJIndexDetails pcjIndexDetails = ryaDetailsHolder.get().getPCJIndexDetails();
        final boolean pcjIndexingEnabeld = pcjIndexDetails.isEnabled();
        if(!pcjIndexingEnabeld) {
            throw new RyaClientException(String.format("The '%s' instance of Rya does not have PCJ Indexing enabled.", instanceName));
        }

        // Create the PCJ table that will receive the index results.
        final String pcjId;
        final PrecomputedJoinStorage pcjStorage = new AccumuloPcjStorage(getConnector(), instanceName);
        try {
            pcjId = pcjStorage.createPcj(sparql);
        } catch (final PCJStorageException e) {
            throw new RyaClientException("Problem while initializing the PCJ table.", e);
        }

        // If a Fluo application is being used, task it with updating the PCJ.
        final Optional<FluoDetails> fluoDetailsHolder = pcjIndexDetails.getFluoDetails();
        if(fluoDetailsHolder.isPresent()) {
            final String fluoAppName = fluoDetailsHolder.get().getUpdateAppName();
            try {
                updateFluoApp(instanceName, fluoAppName, pcjStorage, pcjId);
            } catch (RepositoryException | MalformedQueryException | SailException | QueryEvaluationException | PcjException e) {
                throw new RyaClientException("Problem while initializing the Fluo application with the new PCJ.", e);
            }

            // Update the Rya Details to indicate the PCJ is being updated incrementally.
            final RyaDetailsRepository detailsRepo = new AccumuloRyaInstanceDetailsRepository(getConnector(), instanceName);
            try {
                new RyaDetailsUpdater(detailsRepo).update(new RyaDetailsMutator() {
                    @Override
                    public RyaDetails mutate(final RyaDetails originalDetails) throws CouldNotApplyMutationException {
                        // Update the original PCJ Details to indicate they are incrementally updated.
                        final PCJDetails originalPCJDetails = originalDetails.getPCJIndexDetails().getPCJDetails().get(pcjId);
                        final PCJDetails.Builder mutatedPCJDetails = PCJDetails.builder( originalPCJDetails )
                            .setUpdateStrategy( PCJUpdateStrategy.INCREMENTAL );

                        // Replace the old PCJ Details with the updated ones.
                        final RyaDetails.Builder builder = RyaDetails.builder(originalDetails);
                        builder.getPCJIndexDetails().addPCJDetails( mutatedPCJDetails );
                        return builder.build();
                    }
                });
            } catch (RyaDetailsRepositoryException | CouldNotApplyMutationException e) {
                throw new RyaClientException("Problem while updating the Rya instance's Details to indicate the PCJ is being incrementally updated.", e);
            }
        }

        // Return the ID that was assigned to the PCJ.
        return pcjId;
    }

    private void updateFluoApp(final String ryaInstance, final String fluoAppName, final PrecomputedJoinStorage pcjStorage, final String pcjId) throws RepositoryException, MalformedQueryException, SailException, QueryEvaluationException, PcjException {
        requireNonNull(pcjStorage);
        requireNonNull(pcjId);

        // Connect to the Fluo application that is updating this instance's PCJs.
        final AccumuloConnectionDetails cd = super.getAccumuloConnectionDetails();
        final FluoClient fluoClient = new FluoClientFactory().connect(
                cd.getUsername(),
                new String(cd.getPassword()),
                cd.getInstanceName(),
                cd.getZookeepers(),
                fluoAppName);

        // Setup the Rya client that is able to talk to scan Rya's statements.
        final RyaSailRepository ryaSailRepo = makeRyaRepository(getConnector(), ryaInstance);

        // Initialize the PCJ within the Fluo application.
        final org.apache.rya.indexing.pcj.fluo.api.CreatePcj fluoCreatePcj = new org.apache.rya.indexing.pcj.fluo.api.CreatePcj();
        fluoCreatePcj.withRyaIntegration(pcjId, pcjStorage, fluoClient, ryaSailRepo);
    }

    private static RyaSailRepository makeRyaRepository(final Connector connector, final String ryaInstance) throws RepositoryException {
        checkNotNull(connector);
        checkNotNull(ryaInstance);

        // Setup Rya configuration values.
        final AccumuloRdfConfiguration ryaConf = new AccumuloRdfConfiguration();
        ryaConf.setTablePrefix( ryaInstance );

        // Connect to the Rya repo using the provided Connector.
        final AccumuloRyaDAO accumuloRyaDao = new AccumuloRyaDAO();
        accumuloRyaDao.setConnector(connector);
        accumuloRyaDao.setConf(ryaConf);

        final RdfCloudTripleStore ryaStore = new RdfCloudTripleStore();
        ryaStore.setRyaDAO(accumuloRyaDao);

        final RyaSailRepository ryaRepo = new RyaSailRepository(ryaStore);
        ryaRepo.initialize();
        return ryaRepo;
    }
}
