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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.log4j.Logger;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.indexing.entity.model.Entity;
import org.apache.rya.indexing.entity.model.Property;
import org.apache.rya.indexing.entity.model.Type;
import org.apache.rya.indexing.entity.model.TypedEntity;
import org.apache.rya.indexing.entity.storage.EntityStorage;
import org.apache.rya.indexing.entity.storage.TypeStorage.TypeStorageException;
import org.apache.rya.indexing.entity.storage.mongo.ConvertingCursor.Converter;
import org.apache.rya.indexing.entity.storage.mongo.DocumentConverter.DocumentConverterException;
import org.apache.rya.indexing.entity.storage.mongo.key.MongoDbSafeKey;
import org.apache.rya.indexing.smarturi.SmartUriException;
import org.apache.rya.indexing.smarturi.duplication.DuplicateDataDetector;
import org.apache.rya.indexing.smarturi.duplication.EntityNearDuplicateException;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
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
    private static final Logger log = Logger.getLogger(MongoEntityStorage.class);

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

    private final DuplicateDataDetector duplicateDataDetector;
    private MongoTypeStorage mongoTypeStorage = null;

    /**
     * Constructs an instance of {@link MongoEntityStorage}.
     *
     * @param mongo - A client connected to the Mongo instance that hosts the Rya instance. (not null)
     * @param ryaInstanceName - The name of the Rya instance the {@link TypedEntity}s are for. (not null)
     * @throws ConfigurationException
     */
    public MongoEntityStorage(final MongoClient mongo, final String ryaInstanceName) throws EntityStorageException {
        this(mongo, ryaInstanceName, null);
    }

    /**
     * Constructs an instance of {@link MongoEntityStorage}.
     *
     * @param mongo - A client connected to the Mongo instance that hosts the Rya instance. (not null)
     * @param ryaInstanceName - The name of the Rya instance the {@link TypedEntity}s are for. (not null)
     * @param duplicateDataDetector - The {@link DuplicateDataDetector}.
     * @throws EntityStorageException
     */
    public MongoEntityStorage(final MongoClient mongo, final String ryaInstanceName, final DuplicateDataDetector duplicateDataDetector) throws EntityStorageException {
        this.mongo = requireNonNull(mongo);
        this.ryaInstanceName = requireNonNull(ryaInstanceName);
        if (duplicateDataDetector == null) {
            try {
                this.duplicateDataDetector = new DuplicateDataDetector();
            } catch (final ConfigurationException e) {
                throw new EntityStorageException("Could not create duplicate data detector.", e);
            }
        } else {
            this.duplicateDataDetector = duplicateDataDetector;
        }
    }

    @Override
    public void create(final Entity entity) throws EntityStorageException {
        requireNonNull(entity);

        try {
            final boolean hasDuplicate = detectDuplicates(entity);

            if (!hasDuplicate) {
                mongo.getDatabase(ryaInstanceName)
                    .getCollection(COLLECTION_NAME)
                    .insertOne( ENTITY_CONVERTER.toDocument(entity) );
            } else {
                throw new EntityNearDuplicateException("Duplicate data found and will not be inserted for Entity with Subject: "  + entity);
            }
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

    private boolean detectDuplicates(final Entity entity) throws EntityStorageException {
        boolean hasDuplicate = false;
        if (duplicateDataDetector.isDetectionEnabled()) {
            if (mongoTypeStorage == null) {
                mongoTypeStorage = new MongoTypeStorage(mongo, ryaInstanceName);
            }

            // Grab all entities that have all the same explicit types as our
            // original Entity.
            final List<Entity> comparisonEntities = searchHasAllExplicitTypes(entity.getExplicitTypeIds());

            // Now that we have our set of potential duplicates, compare them.
            // We can stop when we find one duplicate.
            for (final Entity compareEntity : comparisonEntities) {
                try {
                    hasDuplicate = duplicateDataDetector.compareEntities(entity, compareEntity);
                } catch (final SmartUriException e) {
                    throw new EntityStorageException("Encountered an error while comparing entities.", e);
                }
                if (hasDuplicate) {
                    break;
                }
            }
        }
        return hasDuplicate;
    }

    /**
     * Searches the Entity storage for all Entities that contain all the
     * specified explicit type IDs.
     * @param explicitTypeIds the {@link ImmutableList} of {@link RyaURI}s that
     * are being searched for.
     * @return the {@link List} of {@link Entity}s that have all the specified
     * explicit type IDs. If nothing was found an empty {@link List} is
     * returned.
     * @throws EntityStorageException
     */
    private List<Entity> searchHasAllExplicitTypes(final ImmutableList<RyaURI> explicitTypeIds) throws EntityStorageException {
        // Grab the first type from the explicit type IDs.
        RyaURI firstType = null;
        if (!explicitTypeIds.isEmpty()) {
            firstType = explicitTypeIds.get(0);
        }

        // Check if that type exists anywhere in storage.
        final List<RyaURI> subjects = new ArrayList<>();
        Optional<Type> type;
        try {
            type = mongoTypeStorage.get(firstType);
        } catch (final TypeStorageException e) {
            throw new EntityStorageException("Unable to get entity type: " + firstType, e);
        }
        if (type.isPresent()) {
            // Grab the subjects for all the types we found matching "firstType"
            final ConvertingCursor<TypedEntity> cursor = search(Optional.empty(), type.get(), Collections.emptySet());
            while (cursor.hasNext()) {
                final TypedEntity typedEntity = cursor.next();
                final RyaURI subject = typedEntity.getSubject();
                subjects.add(subject);
            }
        }

        // Now grab all the Entities that have the subjects we found.
        final List<Entity> hasAllExplicitTypesEntities = new ArrayList<>();
        for (final RyaURI subject : subjects) {
            final Optional<Entity> entityFromSubject = get(subject);
            if (entityFromSubject.isPresent()) {
                final Entity candidateEntity = entityFromSubject.get();
                // Filter out any entities that don't have all the same
                // types associated with them as our original list of explicit
                // type IDs. We already know the entities we found have
                // "firstType" but now we have access to all the other types
                // they have.
                if (candidateEntity.getExplicitTypeIds().containsAll(explicitTypeIds)) {
                    hasAllExplicitTypesEntities.add(candidateEntity);
                }
            }
        }

        return hasAllExplicitTypesEntities;
    }
}