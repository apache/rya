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
package org.apache.rya.api.client.accumulo;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.log4j.Logger;
import org.apache.rya.indexing.pcj.storage.PcjMetadata;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage.PCJStorageException;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPcjStorage;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;

import com.google.common.base.Optional;

import info.aduna.iteration.CloseableIteration;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.instance.AccumuloRyaInstanceDetailsRepository;
import org.apache.rya.api.client.BatchUpdatePCJ;
import org.apache.rya.api.client.InstanceDoesNotExistException;
import org.apache.rya.api.client.PCJDoesNotExistException;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.api.instance.RyaDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails.PCJDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails.PCJDetails.PCJUpdateStrategy;
import org.apache.rya.api.instance.RyaDetailsRepository;
import org.apache.rya.api.instance.RyaDetailsRepository.NotInitializedException;
import org.apache.rya.api.instance.RyaDetailsRepository.RyaDetailsRepositoryException;
import org.apache.rya.api.instance.RyaDetailsUpdater;
import org.apache.rya.api.instance.RyaDetailsUpdater.RyaDetailsMutator;
import org.apache.rya.api.instance.RyaDetailsUpdater.RyaDetailsMutator.CouldNotApplyMutationException;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.rdftriplestore.inference.InferenceEngineException;
import org.apache.rya.sail.config.RyaSailFactory;

/**
 * Uses an in memory Rya Client to batch update a PCJ index.
 */
public class AccumuloBatchUpdatePCJ extends AccumuloCommand implements BatchUpdatePCJ {

    private static final Logger log = Logger.getLogger(AccumuloBatchUpdatePCJ.class);

    /**
     * Constructs an instance of {@link AccumuloBatchUpdatePCJ}.
     *
     * @param connectionDetails - Details about the values that were used to create the connector to the cluster. (not null)
     * @param connector - Provides programmatic access to the instance of Accumulo that hosts Rya instance. (not null)
     */
    public AccumuloBatchUpdatePCJ(final AccumuloConnectionDetails connectionDetails, final Connector connector) {
        super(connectionDetails, connector);
    }

    @Override
    public void batchUpdate(final String ryaInstanceName, final String pcjId) throws InstanceDoesNotExistException, PCJDoesNotExistException, RyaClientException {
        requireNonNull(ryaInstanceName);
        requireNonNull(pcjId);
        verifyPCJState(ryaInstanceName, pcjId);
        updatePCJResults(ryaInstanceName, pcjId);
        updatePCJMetadata(ryaInstanceName, pcjId);
    }

    private void verifyPCJState(final String ryaInstanceName, final String pcjId) throws RyaClientException {
        try {
            // Fetch the Rya instance's details.
            final RyaDetailsRepository detailsRepo = new AccumuloRyaInstanceDetailsRepository(super.getConnector(), ryaInstanceName);
            final RyaDetails ryaDetails = detailsRepo.getRyaInstanceDetails();

            // Ensure PCJs are enabled.
            if(!ryaDetails.getPCJIndexDetails().isEnabled()) {
                throw new RyaClientException("PCJs are not enabled for the Rya instance named '" + ryaInstanceName + "'.");
            }

            // Ensure the PCJ exists.
            if(!ryaDetails.getPCJIndexDetails().getPCJDetails().containsKey(pcjId)) {
                throw new PCJDoesNotExistException("The PCJ with id '" + pcjId + "' does not exist within Rya instance '" + ryaInstanceName + "'.");
            }

            // Ensure the PCJ is not already being incrementally updated.
            final PCJDetails pcjDetails = ryaDetails.getPCJIndexDetails().getPCJDetails().get(pcjId);
            final Optional<PCJUpdateStrategy> updateStrategy = pcjDetails.getUpdateStrategy();
            if(updateStrategy.isPresent() && updateStrategy.get() == PCJUpdateStrategy.INCREMENTAL) {
                throw new RyaClientException("The PCJ with id '" + pcjId + "' is already being updated incrementally.");
            }
        } catch(final NotInitializedException e) {
            throw new InstanceDoesNotExistException("No RyaDetails are initialized for the Rya instance named '" + ryaInstanceName + "'.", e);
        } catch (final RyaDetailsRepositoryException e) {
            throw new RyaClientException("Could not fetch the RyaDetails for the Rya instance named '" + ryaInstanceName + "'.", e);
        }
    }

    private void updatePCJResults(final String ryaInstanceName, final String pcjId) throws InstanceDoesNotExistException, PCJDoesNotExistException, RyaClientException {
        // Things that have to be closed before we exit.
        Sail sail = null;
        SailConnection sailConn = null;
        CloseableIteration<? extends BindingSet, QueryEvaluationException> results = null;

        try {
            // Create an instance of Sail backed by the Rya instance.
            sail = connectToRya(ryaInstanceName);

            // Purge the old results from the PCJ.
            final PrecomputedJoinStorage pcjStorage = new AccumuloPcjStorage(super.getConnector(), ryaInstanceName);
            try {
                pcjStorage.purge(pcjId);
            } catch (final PCJStorageException e) {
                throw new RyaClientException("Could not batch update PCJ with ID '" + pcjId + "' because the old " +
                        "results could not be purged from it.", e);
            }

            try {
                // Parse the PCJ's SPARQL query.
                final PcjMetadata metadata = pcjStorage.getPcjMetadata(pcjId);
                final String sparql = metadata.getSparql();
                final SPARQLParser parser = new SPARQLParser();
                final ParsedQuery parsedQuery = parser.parseQuery(sparql, null);

                // Execute the query.
                sailConn = sail.getConnection();
                results = sailConn.evaluate(parsedQuery.getTupleExpr(), null, null, false);

                // Load the results into the PCJ table.
                final List<VisibilityBindingSet> batch = new ArrayList<>(1000);

                while(results.hasNext()) {
                    final VisibilityBindingSet result = new VisibilityBindingSet(results.next(), "");
                    batch.add(result);

                    if(batch.size() == 1000) {
                        pcjStorage.addResults(pcjId, batch);
                        batch.clear();
                    }
                }

                if(!batch.isEmpty()) {
                    pcjStorage.addResults(pcjId, batch);
                    batch.clear();
                }
            } catch(final MalformedQueryException | PCJStorageException | SailException | QueryEvaluationException e) {
                throw new RyaClientException("Fail to batch load new results into the PCJ with ID '" + pcjId + "'.", e);
            }
        } finally {
            if(results != null) {
                try {
                    results.close();
                } catch (final QueryEvaluationException e) {
                    log.warn(e.getMessage(), e);
                }
            }

            if(sailConn != null) {
                try {
                    sailConn.close();
                } catch (final SailException e) {
                    log.warn(e.getMessage(), e);
                }
            }

            if(sail != null) {
                try {
                    sail.shutDown();
                } catch (final SailException e) {
                    log.warn(e.getMessage(), e);
                }
            }
        }
    }

    private Sail connectToRya(final String ryaInstanceName) throws RyaClientException {
        try {
            final AccumuloConnectionDetails connectionDetails = super.getAccumuloConnectionDetails();

            final AccumuloRdfConfiguration ryaConf = new AccumuloRdfConfiguration();
            ryaConf.setTablePrefix(ryaInstanceName);
            ryaConf.set(ConfigUtils.CLOUDBASE_USER, connectionDetails.getUsername());
            ryaConf.set(ConfigUtils.CLOUDBASE_PASSWORD, new String(connectionDetails.getPassword()));
            ryaConf.set(ConfigUtils.CLOUDBASE_ZOOKEEPERS, connectionDetails.getZookeepers());
            ryaConf.set(ConfigUtils.CLOUDBASE_INSTANCE, connectionDetails.getInstanceName());

            // Turn PCJs off so that we will only scan the core Rya tables while building the PCJ results.
            ryaConf.set(ConfigUtils.USE_PCJ, "false");

            return RyaSailFactory.getInstance(ryaConf);
        } catch (SailException | AccumuloException | AccumuloSecurityException | RyaDAOException | InferenceEngineException e) {
            throw new RyaClientException("Could not connect to the Rya instance named '" + ryaInstanceName + "'.", e);
        }
    }

    private void updatePCJMetadata(final String ryaInstanceName, final String pcjId) throws RyaClientException {
        // Update the PCJ's metadata to indicate it was just batch updated.
        try {
            final RyaDetailsRepository detailsRepo = new AccumuloRyaInstanceDetailsRepository(super.getConnector(), ryaInstanceName);

            new RyaDetailsUpdater(detailsRepo).update(new RyaDetailsMutator() {
                @Override
                public RyaDetails mutate(final RyaDetails originalDetails) throws CouldNotApplyMutationException {
                    // Update the original PCJ Details to indicate they were batch updated.
                    final PCJDetails originalPCJDetails = originalDetails.getPCJIndexDetails().getPCJDetails().get(pcjId);
                    final PCJDetails.Builder mutatedPCJDetails = PCJDetails.builder( originalPCJDetails )
                            .setUpdateStrategy( PCJUpdateStrategy.BATCH )
                            .setLastUpdateTime( new Date());

                    // Replace the old PCJ Details with the updated ones.
                    final RyaDetails.Builder builder = RyaDetails.builder(originalDetails);
                    builder.getPCJIndexDetails().addPCJDetails( mutatedPCJDetails );
                    return builder.build();
                }
            });
        } catch (final RyaDetailsRepositoryException | CouldNotApplyMutationException e) {
            throw new RyaClientException("Could not update the PCJ's metadata.", e);
        }
    }
}