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
package org.apache.rya.api.client.mongo;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.rya.api.client.BatchUpdatePCJ;
import org.apache.rya.api.client.InstanceDoesNotExistException;
import org.apache.rya.api.client.InstanceExists;
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
import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.indexing.pcj.storage.PcjMetadata;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage.PCJStorageException;
import org.apache.rya.indexing.pcj.storage.mongo.MongoPcjStorage;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;
import org.apache.rya.mongodb.instance.MongoRyaInstanceDetailsRepository;
import org.apache.rya.rdftriplestore.inference.InferenceEngineException;
import org.apache.rya.sail.config.RyaSailFactory;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResultHandlerBase;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;

/**
 * A Mongo implementation of {@link MongoBatchUpdatePCJ}.
 * <p>
 * <b>Note:</b>
 * <p>
 * Using this batch updater can create data leaks since the Sail layer does not
 * support {@link VisibilityBindingSet}s.
 */
public class MongoBatchUpdatePCJ implements BatchUpdatePCJ {
    private static final Logger log = LoggerFactory.getLogger(MongoBatchUpdatePCJ.class);

    private final MongoConnectionDetails connectionDetails;
    private final MongoClient mongoClient;
    private final InstanceExists instanceExists;

    /**
     * Constructs an instance of {@link MongoBatchUpdatePCJ}.
     *
     * @param connectionDetails - Details to connect to the server. (not null)
     * @param instanceExists - The interactor used to check if a Rya instance exists. (not null)
     * @param mongoClient - The {@link MongoClient} to use when batch updating. (not null)
     */
    public MongoBatchUpdatePCJ(
            final MongoConnectionDetails connectionDetails,
            final MongoClient mongoClient,
            final MongoInstanceExists instanceExists) {
        this.connectionDetails = requireNonNull(connectionDetails);
        this.mongoClient = requireNonNull(mongoClient);
        this.instanceExists = requireNonNull(instanceExists);
    }

    @Override
    public void batchUpdate(final String ryaInstanceName, final String pcjId) throws InstanceDoesNotExistException, PCJDoesNotExistException, RyaClientException {
        requireNonNull(ryaInstanceName);
        requireNonNull(pcjId);

        checkState(instanceExists.exists(ryaInstanceName), "The instance: " + ryaInstanceName + " does not exist.");

        verifyPCJState(ryaInstanceName, pcjId, mongoClient);
        updatePCJResults(ryaInstanceName, pcjId, mongoClient);
        updatePCJMetadata(ryaInstanceName, pcjId, mongoClient);
    }

    private void verifyPCJState(final String ryaInstanceName, final String pcjId, final MongoClient client) throws RyaClientException {
        try {
            // Fetch the Rya instance's details.
            final RyaDetailsRepository detailsRepo = new MongoRyaInstanceDetailsRepository(client, ryaInstanceName);
            final RyaDetails ryaDetails = detailsRepo.getRyaInstanceDetails();

            // Ensure PCJs are enabled.
            if(!ryaDetails.getPCJIndexDetails().isEnabled()) {
                throw new RyaClientException("PCJs are not enabled for the Rya instance named '" + ryaInstanceName + "'.");
            }

            // Ensure the PCJ exists.
            if(!ryaDetails.getPCJIndexDetails().getPCJDetails().containsKey(pcjId)) {
                throw new PCJDoesNotExistException("The PCJ with id '" + pcjId + "' does not exist within Rya instance '" + ryaInstanceName + "'.");
            }

        } catch(final NotInitializedException e) {
            throw new InstanceDoesNotExistException("No RyaDetails are initialized for the Rya instance named '" + ryaInstanceName + "'.", e);
        } catch (final RyaDetailsRepositoryException e) {
            throw new RyaClientException("Could not fetch the RyaDetails for the Rya instance named '" + ryaInstanceName + "'.", e);
        }
    }

    private void updatePCJResults(final String ryaInstanceName, final String pcjId, final MongoClient client) throws InstanceDoesNotExistException, PCJDoesNotExistException, RyaClientException {
        // Things that have to be closed before we exit.
        Sail sail = null;
        SailConnection sailConn = null;

        try(final PrecomputedJoinStorage pcjStorage = new MongoPcjStorage(client, ryaInstanceName)) {
            // Create an instance of Sail backed by the Rya instance.
            sail = connectToRya(ryaInstanceName);
            final SailRepository sailRepo = new SailRepository(sail);
            final SailRepositoryConnection sailRepoConn = sailRepo.getConnection();
            // Purge the old results from the PCJ.
            try {
                pcjStorage.purge(pcjId);
            } catch (final PCJStorageException e) {
                throw new RyaClientException("Could not batch update PCJ with ID '" + pcjId + "' because the old " +
                        "results could not be purged from it.", e);
            }

            // Parse the PCJ's SPARQL query.
            final PcjMetadata metadata = pcjStorage.getPcjMetadata(pcjId);
            final String sparql = metadata.getSparql();
            sailConn = sail.getConnection();
            final TupleQuery tupleQuery = sailRepoConn.prepareTupleQuery(QueryLanguage.SPARQL, sparql);

            // Execute the query.
            final List<VisibilityBindingSet> batch = new ArrayList<>(1000);
            tupleQuery.evaluate(new TupleQueryResultHandlerBase() {
                @Override
                public void handleSolution(final BindingSet bindingSet) throws TupleQueryResultHandlerException {
                    final VisibilityBindingSet result = new VisibilityBindingSet(bindingSet, "");
                    log.warn("Visibility information on the binding set is lost during a batch update."
                            + "  This can create data leaks.");
                    batch.add(result);

                    if(batch.size() == 1000) {
                        try {
                            pcjStorage.addResults(pcjId, batch);
                        } catch (final PCJStorageException e) {
                            throw new TupleQueryResultHandlerException("Fail to batch load new results into the PCJ with ID '" + pcjId + "'.", e);
                        }
                        batch.clear();
                    }
                }
            });

            if(!batch.isEmpty()) {
                pcjStorage.addResults(pcjId, batch);
                batch.clear();
            }
        } catch(final MalformedQueryException | PCJStorageException | SailException |
                QueryEvaluationException | RepositoryException | TupleQueryResultHandlerException e) {
            throw new RyaClientException("Fail to batch load new results into the PCJ with ID '" + pcjId + "'.", e);
        } finally {
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
            final MongoDBRdfConfiguration ryaConf = connectionDetails.build(ryaInstanceName);
            return RyaSailFactory.getInstance(ryaConf);
        } catch (SailException | AccumuloException | AccumuloSecurityException | RyaDAOException | InferenceEngineException e) {
            throw new RyaClientException("Could not connect to the Rya instance named '" + ryaInstanceName + "'.", e);
        }
    }

    private void updatePCJMetadata(final String ryaInstanceName, final String pcjId, final MongoClient client) throws RyaClientException {
        // Update the PCJ's metadata to indicate it was just batch updated.
        try {
            final RyaDetailsRepository detailsRepo = new MongoRyaInstanceDetailsRepository(client, ryaInstanceName);

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
