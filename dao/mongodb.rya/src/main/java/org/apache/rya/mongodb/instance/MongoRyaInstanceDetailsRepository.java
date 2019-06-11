package org.apache.rya.mongodb.instance;

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

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Objects.requireNonNull;

import org.apache.rya.api.instance.RyaDetails;
import org.apache.rya.api.instance.RyaDetailsRepository;
import org.apache.rya.mongodb.instance.MongoDetailsAdapter.MalformedRyaDetailsException;
import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.result.UpdateResult;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * An implementation of {@link RyaDetailsRepository} that stores a Rya
 * instance's {@link RyaDetails} in a Mongo document.
 */
@DefaultAnnotation(NonNull.class)
public class MongoRyaInstanceDetailsRepository implements RyaDetailsRepository {
    public static final String INSTANCE_DETAILS_COLLECTION_NAME = "instance_details";

    private final MongoDatabase db;
    private final String instanceName;

    /**
     * Constructs an instance of {@link MongoRyaInstanceDetailsRepository}.
     *
     * @param client - Connects to the instance of Mongo that hosts the Rya instance. (not null)
     * @param instanceName - The name of the Rya instance this repository represents. (not null)
     */
    public MongoRyaInstanceDetailsRepository(final MongoClient client, final String instanceName) {
        checkNotNull(client);
        this.instanceName = requireNonNull( instanceName );
        // the rya instance is the Mongo db name. This ignores any collection-prefix.
        db = client.getDatabase(this.instanceName);
    }

    @Override
    public boolean isInitialized() throws RyaDetailsRepositoryException {
        final MongoCollection<Document> col = db.getCollection(INSTANCE_DETAILS_COLLECTION_NAME);
        return col.countDocuments() == 1;
    }

    @Override
    public void initialize(final RyaDetails details) throws AlreadyInitializedException, RyaDetailsRepositoryException {
        // Preconditions.
        requireNonNull( details );

        if(!details.getRyaInstanceName().equals( instanceName )) {
            throw new RyaDetailsRepositoryException("The instance name that was in the provided 'details' does not match " +
                    "the instance name that this repository is connected to. Make sure you're connected to the" +
                    "correct Rya instance.");
        }

        if(isInitialized()) {
            throw new AlreadyInitializedException("The repository has already been initialized for the Rya instance named '" +
                    instanceName + "'.");
        }

        // Create the document that hosts the details if it has not been created yet.
        db.createCollection(INSTANCE_DETAILS_COLLECTION_NAME, new CreateCollectionOptions());
        final MongoCollection<Document> col = db.getCollection(INSTANCE_DETAILS_COLLECTION_NAME);

        // Write the details to the collection.
        col.insertOne(MongoDetailsAdapter.toDocument(details), new InsertOneOptions());
    }

    @Override
    public RyaDetails getRyaInstanceDetails() throws NotInitializedException, RyaDetailsRepositoryException {
        // Preconditions.
        if(!isInitialized()) {
            throw new NotInitializedException("Could not fetch the details for the Rya instanced named '" +
                    instanceName + "' because it has not been initialized yet.");
        }

        // Fetch the value from the collection.
        final MongoCollection<Document> col = db.getCollection(INSTANCE_DETAILS_COLLECTION_NAME);
        //There should only be one document in the collection.
        final Document mongoObj = col.find().first();

        try{
            // Deserialize it.
            return MongoDetailsAdapter.toRyaDetails( mongoObj );
        } catch (final MalformedRyaDetailsException e) {
            throw new RyaDetailsRepositoryException("The existing details details are malformed.", e);
        }
    }

    @Override
    public void update(final RyaDetails oldDetails, final RyaDetails newDetails)
            throws NotInitializedException, ConcurrentUpdateException, RyaDetailsRepositoryException {
        // Preconditions.
        requireNonNull(oldDetails);
        requireNonNull(newDetails);

        if(!newDetails.getRyaInstanceName().equals( instanceName )) {
            throw new RyaDetailsRepositoryException("The instance name that was in the provided 'newDetails' does not match " +
                    "the instance name that this repository is connected to. Make sure you're connected to the" +
                    "correct Rya instance.");
        }

        if(!isInitialized()) {
            throw new NotInitializedException("Could not update the details for the Rya instanced named '" +
                    instanceName + "' because it has not been initialized yet.");
        }

        if(oldDetails.equals(newDetails)) {
            return;
        }

        final MongoCollection<Document> col = db.getCollection(INSTANCE_DETAILS_COLLECTION_NAME);
        final Document oldObj = MongoDetailsAdapter.toDocument(oldDetails);
        final Document newObj = MongoDetailsAdapter.toDocument(newDetails);
        final UpdateResult result = col.replaceOne(oldObj, newObj, new ReplaceOptions());

        //since there is only 1 document, there should only be 1 update.
        if(result.getModifiedCount() != 1) {
            throw new ConcurrentUpdateException("Could not update the details for the Rya instance named '" +
                instanceName + "' because the old value is out of date.");
        }
    }
}
