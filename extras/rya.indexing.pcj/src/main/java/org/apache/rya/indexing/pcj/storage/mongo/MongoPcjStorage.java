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
package org.apache.rya.indexing.pcj.storage.mongo;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.rya.api.instance.RyaDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails.PCJDetails;
import org.apache.rya.api.instance.RyaDetailsRepository.RyaDetailsRepositoryException;
import org.apache.rya.api.instance.RyaDetailsUpdater;
import org.apache.rya.api.instance.RyaDetailsUpdater.RyaDetailsMutator.CouldNotApplyMutationException;
import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.api.utils.CloseableIterator;
import org.apache.rya.indexing.pcj.storage.PCJIdFactory;
import org.apache.rya.indexing.pcj.storage.PcjMetadata;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage;
import org.apache.rya.mongodb.instance.MongoRyaInstanceDetailsRepository;
import org.openrdf.query.BindingSet;

import com.mongodb.MongoClient;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A mongo backed implementation of {@link PrecomputedJoinStorage}.
 */
@DefaultAnnotation(NonNull.class)
public class MongoPcjStorage implements PrecomputedJoinStorage {
    public static final String PCJ_COLLECTION_NAME = "pcjs";
    // Used to update the instance's metadata.
    private final MongoRyaInstanceDetailsRepository ryaDetailsRepo;

    private final String ryaInstanceName;

    // Factories that are used to create new PCJs.
    private final PCJIdFactory pcjIdFactory = new PCJIdFactory();

    private final MongoPcjDocuments pcjDocs;

    /**
     * Constructs an instance of {@link MongoPcjStorage}.
     *
     * @param client - The {@link MongoClient} that will be used to connect to Mongodb. (not null)
     * @param ryaInstanceName - The name of the RYA instance that will be accessed. (not null)
     */
    public MongoPcjStorage(final MongoClient client, final String ryaInstanceName) {
        requireNonNull(client);
        this.ryaInstanceName = requireNonNull(ryaInstanceName);
        pcjDocs = new MongoPcjDocuments(client, ryaInstanceName);
        ryaDetailsRepo = new MongoRyaInstanceDetailsRepository(client, ryaInstanceName);
    }

    @Override
    public String createPcj(final String sparql) throws PCJStorageException {
        requireNonNull(sparql);

        // Update the Rya Details for this instance to include the new PCJ
        // table.
        final String pcjId = pcjIdFactory.nextId();

        try {
            new RyaDetailsUpdater(ryaDetailsRepo).update(originalDetails -> {
                // Create the new PCJ's details.
                final PCJDetails.Builder newPcjDetails = PCJDetails.builder().setId(pcjId);

                // Add them to the instance's details.
                final RyaDetails.Builder mutated = RyaDetails.builder(originalDetails);
                mutated.getPCJIndexDetails().addPCJDetails(newPcjDetails);
                return mutated.build();
            });
        } catch (final RyaDetailsRepositoryException | CouldNotApplyMutationException e) {
            throw new PCJStorageException(String.format("Could not create a new PCJ for Rya instance '%s' "
                    + "because of a problem while updating the instance's details.", ryaInstanceName), e);
        }

        // Create the objectID of the document to house the PCJ results.
        pcjDocs.createPcj(pcjId, sparql);

        // Add access to the PCJ table to all users who are authorized for this
        // instance of Rya.
        return pcjId;
    }


    @Override
    public PcjMetadata getPcjMetadata(final String pcjId) throws PCJStorageException {
        requireNonNull(pcjId);
        return pcjDocs.getPcjMetadata(pcjId);
    }

    @Override
    public void addResults(final String pcjId, final Collection<VisibilityBindingSet> results) throws PCJStorageException {
        requireNonNull(pcjId);
        requireNonNull(results);
        pcjDocs.addResults(pcjId, results);
    }


    @Override
    public CloseableIterator<BindingSet> listResults(final String pcjId) throws PCJStorageException {
        requireNonNull(pcjId);
        // Scan the PCJ table.
        return pcjDocs.listResults(pcjId);
    }

    @Override
    public void purge(final String pcjId) throws PCJStorageException {
        requireNonNull(pcjId);
        pcjDocs.purgePcjs(pcjId);
    }

    @Override
    public void dropPcj(final String pcjId) throws PCJStorageException {
        requireNonNull(pcjId);

        // Update the Rya Details for this instance to no longer include the
        // PCJ.
        try {
            new RyaDetailsUpdater(ryaDetailsRepo).update(originalDetails -> {
                // Drop the PCJ's metadata from the instance's metadata.
                final RyaDetails.Builder mutated = RyaDetails.builder(originalDetails);
                mutated.getPCJIndexDetails().removePCJDetails(pcjId);
                return mutated.build();
            });
        } catch (final RyaDetailsRepositoryException | CouldNotApplyMutationException e) {
            throw new PCJStorageException(String.format("Could not drop an existing PCJ for Rya instance '%s' "
                    + "because of a problem while updating the instance's details.", ryaInstanceName), e);
        }

        // Delete the table that hold's the PCJ's results.
        pcjDocs.dropPcj(pcjId);
    }

    @Override
    public List<String> listPcjs() throws PCJStorageException {
        try {
            final RyaDetails details = ryaDetailsRepo.getRyaInstanceDetails();
            final PCJIndexDetails pcjIndexDetails = details.getPCJIndexDetails();
            final List<String> pcjIds = new ArrayList<>(pcjIndexDetails.getPCJDetails().keySet());
            return pcjIds;
        } catch (final RyaDetailsRepositoryException e) {
            throw new PCJStorageException("Could not check to see if RyaDetails exist for the instance.", e);
        }
    }

    @Override
    public void close() throws PCJStorageException {
    }
}