/*
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

import java.util.Set;

import org.apache.accumulo.core.client.Connector;
import org.apache.fluo.api.client.FluoClient;
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
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.indexing.pcj.fluo.api.CreateFluoPcj;
import org.apache.rya.indexing.pcj.fluo.app.query.UnsupportedQueryException;
import org.apache.rya.indexing.pcj.storage.PcjException;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage.PCJStorageException;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPcjStorage;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.sail.SailException;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * An Accumulo implementation of the {@link CreatePCJ} command.
 */
@DefaultAnnotation(NonNull.class)
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
    public String createPCJ(final String instanceName, final String sparql, Set<ExportStrategy> strategies) throws InstanceDoesNotExistException, RyaClientException {
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
        try(final PrecomputedJoinStorage pcjStorage = new AccumuloPcjStorage(getConnector(), instanceName)) {
            pcjId = pcjStorage.createPcj(sparql);

            // If a Fluo application is being used, task it with updating the PCJ.
            final Optional<FluoDetails> fluoDetailsHolder = pcjIndexDetails.getFluoDetails();
            if(fluoDetailsHolder.isPresent()) {
                final String fluoAppName = fluoDetailsHolder.get().getUpdateAppName();
                try {
                    updateFluoApp(instanceName, fluoAppName, pcjId, sparql, strategies);
                } catch (RepositoryException | MalformedQueryException | SailException | QueryEvaluationException | PcjException | RyaDAOException e) {
                    throw new RyaClientException("Problem while initializing the Fluo application with the new PCJ.", e);
                } catch (UnsupportedQueryException e) {
                    throw new RyaClientException("The new PCJ could not be initialized because it either contains an unsupported query node "
                            + "or an invalid ExportStrategy for the given QueryType.  Projection queries can be exported to either Rya or Kafka,"
                            + "unless they contain an aggregation, in which case they can only be exported to Kafka.  Construct queries can be exported"
                            + "to Rya and Kafka, and Periodic queries can only be exported to Rya.");
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
        } catch (final PCJStorageException e) {
            throw new RyaClientException("Problem while initializing the PCJ table.", e);
        }
    }

    @Override
    public String createPCJ(String instanceName, String sparql) throws InstanceDoesNotExistException, RyaClientException {
        return createPCJ(instanceName, sparql, Sets.newHashSet(ExportStrategy.RYA));
    }
    
    
    private void updateFluoApp(final String ryaInstance, final String fluoAppName, final String pcjId, String sparql, Set<ExportStrategy> strategies) throws RepositoryException, MalformedQueryException, SailException, QueryEvaluationException, PcjException, RyaDAOException, UnsupportedQueryException {
        requireNonNull(sparql);
        requireNonNull(pcjId);
        requireNonNull(strategies);

        // Connect to the Fluo application that is updating this instance's PCJs.
        final AccumuloConnectionDetails cd = super.getAccumuloConnectionDetails();
        try(final FluoClient fluoClient = new FluoClientFactory().connect(
                cd.getUsername(),
                new String(cd.getUserPass()),
                cd.getInstanceName(),
                cd.getZookeepers(),
                fluoAppName)) {
            // Initialize the PCJ within the Fluo application.
            final CreateFluoPcj fluoCreatePcj = new CreateFluoPcj();
            fluoCreatePcj.withRyaIntegration(pcjId, sparql, strategies, fluoClient, getConnector(), ryaInstance);
        }
    }

}