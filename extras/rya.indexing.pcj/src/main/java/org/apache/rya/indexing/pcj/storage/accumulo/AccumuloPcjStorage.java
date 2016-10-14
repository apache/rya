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
package org.apache.rya.indexing.pcj.storage.accumulo;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.rya.indexing.pcj.storage.PCJIdFactory;
import org.apache.rya.indexing.pcj.storage.PcjMetadata;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;

import org.apache.rya.accumulo.instance.AccumuloRyaInstanceDetailsRepository;
import org.apache.rya.api.instance.RyaDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails.PCJDetails;
import org.apache.rya.api.instance.RyaDetailsRepository;
import org.apache.rya.api.instance.RyaDetailsRepository.RyaDetailsRepositoryException;
import org.apache.rya.api.instance.RyaDetailsUpdater;
import org.apache.rya.api.instance.RyaDetailsUpdater.RyaDetailsMutator;
import org.apache.rya.api.instance.RyaDetailsUpdater.RyaDetailsMutator.CouldNotApplyMutationException;

/**
 * An Accumulo backed implementation of {@link PrecomputedJoinStorage}.
 */
@ParametersAreNonnullByDefault
public class AccumuloPcjStorage implements PrecomputedJoinStorage {

    // Factories that are used to create new PCJs.
    private final PCJIdFactory pcjIdFactory = new PCJIdFactory();
    private final PcjTableNameFactory pcjTableNameFactory = new PcjTableNameFactory();
    private final PcjVarOrderFactory pcjVarOrderFactory = new ShiftVarOrderFactory();

    // Objects used to interact with the PCJ tables associated with an instance of Rya.
    private final Connector accumuloConn;
    private final String ryaInstanceName;
    private final PcjTables pcjTables = new PcjTables();

    // Used to update the instance's metadata.
    private final RyaDetailsRepository ryaDetailsRepo;

    /**
     * Constructs an instance of {@link AccumuloPcjStorage}.
     *
     * @param accumuloConn - The connector that will be used to connect to  Accumulo. (not null)
     * @param ryaInstanceName - The name of the RYA instance that will be accessed. (not null)
     */
    public AccumuloPcjStorage(final Connector accumuloConn, final String ryaInstanceName) {
        this.accumuloConn = requireNonNull(accumuloConn);
        this.ryaInstanceName = requireNonNull(ryaInstanceName);
        ryaDetailsRepo = new AccumuloRyaInstanceDetailsRepository(accumuloConn, ryaInstanceName);
    }

    @Override
    public List<String> listPcjs() throws PCJStorageException {
        try {
            final RyaDetails details = ryaDetailsRepo.getRyaInstanceDetails();
            final PCJIndexDetails pcjIndexDetails = details.getPCJIndexDetails();
            final List<String> pcjIds = new ArrayList<>( pcjIndexDetails.getPCJDetails().keySet() );
            return pcjIds;
        } catch (final RyaDetailsRepositoryException e) {
            throw new PCJStorageException("Could not check to see if RyaDetails exist for the instance.", e);
        }
    }

    @Override
    public String createPcj(final String sparql) throws PCJStorageException {
        requireNonNull(sparql);

        // Create the variable orders that will be used within Accumulo to store the PCJ.
        final Set<VariableOrder> varOrders;
        try {
            varOrders = pcjVarOrderFactory.makeVarOrders(sparql);
        } catch (final MalformedQueryException e) {
            throw new PCJStorageException("Can not create the PCJ. The SPARQL is malformed.", e);
        }

        // Update the Rya Details for this instance to include the new PCJ table.
        final String pcjId = pcjIdFactory.nextId();
        try {
            new RyaDetailsUpdater(ryaDetailsRepo).update(
                    new RyaDetailsMutator() {
                        @Override
                        public RyaDetails mutate(final RyaDetails originalDetails) {
                            // Create the new PCJ's details.
                            final PCJDetails.Builder newPcjDetails = PCJDetails.builder().setId( pcjId );

                            // Add them to the instance's details.
                            final RyaDetails.Builder mutated = RyaDetails.builder(originalDetails);
                            mutated.getPCJIndexDetails().addPCJDetails( newPcjDetails );
                            return mutated.build();
                        }
                    });
        } catch (final RyaDetailsRepositoryException | CouldNotApplyMutationException e) {
            throw new PCJStorageException(String.format("Could not create a new PCJ for Rya instance '%s' " +
                    "because of a problem while updating the instance's details.", ryaInstanceName), e);
        }

        // Create the table that will hold the PCJ's results.
        final String pcjTableName = pcjTableNameFactory.makeTableName(ryaInstanceName, pcjId);
        pcjTables.createPcjTable(accumuloConn, pcjTableName, varOrders, sparql);
        return pcjId;
    }

    @Override
    public PcjMetadata getPcjMetadata(final String pcjId) throws PCJStorageException {
        requireNonNull(pcjId);
        final String pcjTableName = pcjTableNameFactory.makeTableName(ryaInstanceName, pcjId);
        return pcjTables.getPcjMetadata(accumuloConn, pcjTableName);
    }

    @Override
    public void addResults(final String pcjId, final Collection<VisibilityBindingSet> results) throws PCJStorageException {
        requireNonNull(pcjId);
        requireNonNull(results);
        final String pcjTableName = pcjTableNameFactory.makeTableName(ryaInstanceName, pcjId);
        pcjTables.addResults(accumuloConn, pcjTableName, results);
    }

    @Override
    public Iterable<BindingSet> listResults(final String pcjId) throws PCJStorageException {
        requireNonNull(pcjId);

        try {
            // Fetch my authorizations.
            final String myUsername = accumuloConn.whoami();
            final Authorizations myAuths = accumuloConn.securityOperations().getUserAuthorizations( myUsername );

            // Scan the PCJ table.
            final String pcjTableName = pcjTableNameFactory.makeTableName(ryaInstanceName, pcjId);
            return pcjTables.listResults(accumuloConn, pcjTableName, myAuths);

        } catch (AccumuloException | AccumuloSecurityException e) {
            throw new PCJStorageException("Could not list the results because I can not look up my Authorizations.", e);
        }
    }

    @Override
    public void purge(final String pcjId) throws PCJStorageException {
        requireNonNull(pcjId);
        final String pcjTableName = pcjTableNameFactory.makeTableName(ryaInstanceName, pcjId);
        pcjTables.purgePcjTable(accumuloConn, pcjTableName);
    }

    @Override
    public void dropPcj(final String pcjId) throws PCJStorageException {
        requireNonNull(pcjId);

        // Update the Rya Details for this instance to no longer include the PCJ.
        try {
            new RyaDetailsUpdater(ryaDetailsRepo).update(
                    new RyaDetailsMutator() {
                        @Override
                        public RyaDetails mutate(final RyaDetails originalDetails) {
                            // Drop the PCJ's metadata from the instance's metadata.
                            final RyaDetails.Builder mutated = RyaDetails.builder(originalDetails);
                            mutated.getPCJIndexDetails().removePCJDetails(pcjId);
                            return mutated.build();
                        }
                    });
        } catch (final RyaDetailsRepositoryException | CouldNotApplyMutationException e) {
            throw new PCJStorageException(String.format("Could not drop an existing PCJ for Rya instance '%s' " +
                    "because of a problem while updating the instance's details.", ryaInstanceName), e);
        }

        // Delete the table that hold's the PCJ's results.
        final String pcjTableName = pcjTableNameFactory.makeTableName(ryaInstanceName, pcjId);
        pcjTables.dropPcjTable(accumuloConn, pcjTableName);
    }

    @Override
    public void close() throws PCJStorageException {
        // Accumulo Connectors don't require closing.
    }
}