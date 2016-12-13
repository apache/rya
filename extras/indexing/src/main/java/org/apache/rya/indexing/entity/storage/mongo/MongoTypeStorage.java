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
package org.apache.rya.indexing.entity.storage.mongo;

import static java.util.Objects.requireNonNull;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.rya.indexing.entity.model.Type;
import org.apache.rya.indexing.entity.storage.CloseableIterator;
import org.apache.rya.indexing.entity.storage.TypeStorage;
import org.apache.rya.indexing.entity.storage.mongo.DocumentConverter.DocumentConverterException;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.google.common.base.Optional;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;

import mvm.rya.api.domain.RyaURI;

/**
 * A Mongo DB implementation of {@link TypeStorage}.
 */
@ParametersAreNonnullByDefault
public class MongoTypeStorage implements TypeStorage {

    private static final String COLLECTION_NAME = "entity-types";

    private static final TypeDocumentConverter TYPE_CONVERTER = new TypeDocumentConverter();

    /**
     * A client connected to the Mongo instance that hosts the Rya instance.
     */
    private final MongoClient mongo;

    /**
     * The name of the Rya instance the {@link Type}s are for.
     */
    private final String ryaInstanceName;

    /**
     * Constructs an instance of {@link MongoTypeStorage}.
     *
     * @param mongo - A client connected to the Mongo instance that hosts the Rya instance. (not null)
     * @param ryaInstanceName - The name of the Rya instance the {@link Type}s are for. (not null)
     */
    public MongoTypeStorage(MongoClient mongo, String ryaInstanceName) {
        this.mongo = requireNonNull(mongo);
        this.ryaInstanceName = requireNonNull(ryaInstanceName);
    }

    @Override
    public void create(Type type) throws TypeStorageException {
        requireNonNull(type);

        try {
            mongo.getDatabase(ryaInstanceName)
                .getCollection(COLLECTION_NAME)
                .insertOne(TYPE_CONVERTER.toDocument(type));

        } catch(final MongoException e) {
            throw new TypeStorageException("Failed to create Type with ID '" + type.getId().getData() + "'.", e);
        }
    }

    @Override
    public Optional<Type> get(RyaURI typeId) throws TypeStorageException {
        requireNonNull(typeId);

        try {
            final Document document = mongo.getDatabase(ryaInstanceName)
                .getCollection(COLLECTION_NAME)
                .find( makeIdFilter(typeId) )
                .first();

            return document == null ?
                    Optional.absent() :
                    Optional.of( TYPE_CONVERTER.fromDocument(document) );

        } catch(final MongoException | DocumentConverterException e) {
            throw new TypeStorageException("Could not get the Type with ID '" + typeId.getData() + "'.", e);
        }
    }

    @Override
    public CloseableIterator<Type> search(RyaURI propertyName) throws TypeStorageException {
        requireNonNull(propertyName);

        try {
            // Create a Filter that finds Types who have the provided property names.
            final Bson byPropertyName = Filters.eq(TypeDocumentConverter.PROPERTY_NAMES, propertyName.getData());

            final MongoCursor<Document> cursor = mongo.getDatabase(ryaInstanceName)
                .getCollection(COLLECTION_NAME)
                .find( byPropertyName )
                .iterator();

            return new ConvertingCursor<Type>(document -> {
                try {
                    return TYPE_CONVERTER.fromDocument(document);
                } catch (final Exception e) {
                    throw new RuntimeException("Could not convert the Document '" + document + "' into a Type.", e);
                }
            }, cursor);

        } catch(final MongoException e) {
            throw new TypeStorageException("Could not fetch Types that include the property '" + propertyName.getData() + "'.", e);
        }
    }

    @Override
    public boolean delete(RyaURI typeId) throws TypeStorageException {
        requireNonNull(typeId);

        try {
            final Document deleted = mongo.getDatabase(ryaInstanceName)
                .getCollection(COLLECTION_NAME)
                .findOneAndDelete( makeIdFilter(typeId) );

            return deleted != null;

        } catch(final MongoException e) {
            throw new TypeStorageException("Could not delete the Type with ID '" + typeId.getData() + "'.", e);
        }
    }

    private static Bson makeIdFilter(RyaURI typeId) {
        return Filters.eq(TypeDocumentConverter.ID, typeId.getData());
    }
}