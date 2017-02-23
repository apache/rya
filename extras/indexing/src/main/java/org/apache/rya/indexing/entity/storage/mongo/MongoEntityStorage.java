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

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.indexing.entity.model.Entity;
import org.apache.rya.indexing.entity.model.Property;
import org.apache.rya.indexing.entity.model.Type;
import org.apache.rya.indexing.entity.model.TypedEntity;
import org.apache.rya.indexing.entity.storage.EntityStorage;
import org.apache.rya.indexing.entity.storage.mongo.ConvertingCursor.Converter;
import org.apache.rya.indexing.entity.storage.mongo.DocumentConverter.DocumentConverterException;
import org.apache.rya.indexing.entity.storage.mongo.key.MongoDbSafeKey;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.google.common.base.Joiner;
import com.mongodb.ErrorCategory;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A Mongo DB implementation of {@link EntityStorage}.
 */
@DefaultAnnotation(NonNull.class)
public class MongoEntityStorage implements EntityStorage {

    protected static final String COLLECTION_NAME = "entity-entities";

    private static final EntityDocumentConverter ENTITY_CONVERTER = new EntityDocumentConverter();

    /**
     * A client connected to the Mongo instance that hosts the Rya instance.
     */
    protected final MongoClient mongo;

    /**
     * The name of the Rya instance the {@link TypedEntity}s are for.
     */
    protected final String ryaInstanceName;

    /**
     * Constructs an instance of {@link MongoEntityStorage}.
     *
     * @param mongo - A client connected to the Mongo instance that hosts the Rya instance. (not null)
     * @param ryaInstanceName - The name of the Rya instance the {@link TypedEntity}s are for. (not null)
     */
    public MongoEntityStorage(final MongoClient mongo, final String ryaInstanceName) {
        this.mongo = requireNonNull(mongo);
        this.ryaInstanceName = requireNonNull(ryaInstanceName);
    }

    @Override
    public void create(final Entity entity) throws EntityStorageException {
        requireNonNull(entity);

        try {
            mongo.getDatabase(ryaInstanceName)
                .getCollection(COLLECTION_NAME)
                .insertOne( ENTITY_CONVERTER.toDocument(entity) );

        } catch(final MongoException e) {
            final ErrorCategory category = ErrorCategory.fromErrorCode( e.getCode() );
            if(category == ErrorCategory.DUPLICATE_KEY) {
                throw new EntityAlreadyExistsException("Failed to create Entity with Subject '" + entity.getSubject().getData() + "'.", e);
            }
            throw new EntityStorageException("Failed to create Entity with Subject '" + entity.getSubject().getData() + "'.", e);
        }
    }

    @Override
    public void update(final Entity old, final Entity updated) throws StaleUpdateException, EntityStorageException {
        requireNonNull(old);
        requireNonNull(updated);

        // The updated entity must have the same Subject as the one it is replacing.
        if(!old.getSubject().equals(updated.getSubject())) {
            throw new EntityStorageException("The old Entity and the updated Entity must have the same Subject. " +
                    "Old Subject: " + old.getSubject().getData() + ", Updated Subject: " + updated.getSubject().getData());
        }

        // Make sure the updated Entity has a higher verison.
        if(old.getVersion() >= updated.getVersion()) {
            throw new EntityStorageException("The old Entity's version must be less than the updated Entity's version." +
                    " Old version: " + old.getVersion() + " Updated version: " + updated.getVersion());
        }

        final Set<Bson> filters = new HashSet<>();

        // Must match the old entity's Subject.
        filters.add( makeSubjectFilter(old.getSubject()) );

        // Must match the old entity's Version.
        filters.add( makeVersionFilter(old.getVersion()) );

        // Do a find and replace.
        final Bson oldEntityFilter = Filters.and(filters);
        final Document updatedDoc = ENTITY_CONVERTER.toDocument(updated);

        final MongoCollection<Document> collection = mongo.getDatabase(ryaInstanceName).getCollection(COLLECTION_NAME);
        if(collection.findOneAndReplace(oldEntityFilter, updatedDoc) == null) {
            throw new StaleUpdateException("Could not update the Entity with Subject '" + updated.getSubject().getData() + ".");
        }
    }

    @Override
    public Optional<Entity> get(final RyaURI subject) throws EntityStorageException {
        requireNonNull(subject);

        try {
            final Document document = mongo.getDatabase(ryaInstanceName)
                .getCollection(COLLECTION_NAME)
                .find( Filters.eq(EntityDocumentConverter.SUBJECT, subject.getData()) )
                .first();

            return document == null ?
                    Optional.empty() :
                    Optional.of( ENTITY_CONVERTER.fromDocument(document) );

        } catch(final MongoException | DocumentConverterException e) {
            throw new EntityStorageException("Could not get the Entity with Subject '" + subject.getData() + "'.", e);
        }
    }

    @Override
    public ConvertingCursor<TypedEntity> search(final Optional<RyaURI> subject, final Type type, final Set<Property> properties) throws EntityStorageException {
        requireNonNull(type);
        requireNonNull(properties);

        try {
            // Match the specified Property values.
            final Set<Bson> filters = properties.stream()
                    .flatMap(property -> makePropertyFilters(type.getId(), property))
                    .collect(Collectors.toSet());

            // Only match explicitly Typed entities.
            filters.add( makeExplicitTypeFilter(type.getId()) );

            // Get a cursor over the Mongo Document that represent the search results.
            final MongoCursor<Document> cursor = mongo.getDatabase(ryaInstanceName)
                .getCollection(COLLECTION_NAME)
                .find(Filters.and(filters))
                .iterator();

            // Define that Converter that converts from Document into TypedEntity.
            final Converter<TypedEntity> converter = document -> {
                try {
                    final Entity entity = ENTITY_CONVERTER.fromDocument(document);
                    final Optional<TypedEntity> typedEntity = entity.makeTypedEntity( type.getId() );
                    if(!typedEntity.isPresent()) {
                        throw new RuntimeException("Entity with Subject '" + entity.getSubject() +
                                "' could not be cast into Type '" + type.getId() + "'.");
                    }
                    return typedEntity.get();

                } catch (final DocumentConverterException e) {
                    throw new RuntimeException("Document '" + document + "' could not be parsed into an Entity.", e);
                }
            };

            // Return a cursor that performs the conversion.
            return new ConvertingCursor<TypedEntity>(converter, cursor);

        } catch(final MongoException e) {
            throw new EntityStorageException("Could not search Entity.", e);
        }
    }

    @Override
    public boolean delete(final RyaURI subject) throws EntityStorageException {
        requireNonNull(subject);

        try {
            final Document deleted = mongo.getDatabase(ryaInstanceName)
                .getCollection(COLLECTION_NAME)
                .findOneAndDelete( makeSubjectFilter(subject) );

            return deleted != null;

        } catch(final MongoException e) {
            throw new EntityStorageException("Could not delete the Entity with Subject '" + subject.getData() + "'.", e);
        }
    }

    private static Bson makeSubjectFilter(final RyaURI subject) {
        return Filters.eq(EntityDocumentConverter.SUBJECT, subject.getData());
    }

    private static Bson makeVersionFilter(final int version) {
        return Filters.eq(EntityDocumentConverter.VERSION, version);
    }

    private static Bson makeExplicitTypeFilter(final RyaURI typeId) {
        return Filters.eq(EntityDocumentConverter.EXPLICIT_TYPE_IDS, typeId.getData());
    }

    private static Stream<Bson> makePropertyFilters(final RyaURI typeId, final Property property) {
        final String propertyName = property.getName().getData();
        final String encodedPropertyName = MongoDbSafeKey.encodeKey(propertyName);

        // Must match the property's data type.
        final String dataTypePath = Joiner.on(".").join(
                new String[]{EntityDocumentConverter.PROPERTIES, typeId.getData(), encodedPropertyName, RyaTypeDocumentConverter.DATA_TYPE});
        final String propertyDataType = property.getValue().getDataType().stringValue();
        final Bson dataTypeFilter = Filters.eq(dataTypePath, propertyDataType);

        // Must match the property's value.
        final String valuePath = Joiner.on(".").join(
                new String[]{EntityDocumentConverter.PROPERTIES, typeId.getData(), encodedPropertyName, RyaTypeDocumentConverter.VALUE});
        final String propertyValue = property.getValue().getData();
        final Bson valueFilter = Filters.eq(valuePath, propertyValue);

        return Stream.of(dataTypeFilter, valueFilter);
    }
}