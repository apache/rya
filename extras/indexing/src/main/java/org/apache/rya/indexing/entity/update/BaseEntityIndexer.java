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
package org.apache.rya.indexing.entity.update;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Collections.singleton;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.groupingBy;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.indexing.entity.model.Entity;
import org.apache.rya.indexing.entity.model.Property;
import org.apache.rya.indexing.entity.model.Type;
import org.apache.rya.indexing.entity.storage.EntityStorage;
import org.apache.rya.indexing.entity.storage.TypeStorage;
import org.apache.rya.indexing.entity.storage.TypeStorage.TypeStorageException;
import org.apache.rya.indexing.entity.storage.mongo.ConvertingCursor;
import org.apache.rya.indexing.mongodb.IndexingException;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;
import org.apache.rya.mongodb.MongoSecondaryIndex;
import org.openrdf.model.URI;
import org.openrdf.model.vocabulary.RDF;

import com.google.common.base.Objects;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A base class that may be used to update an {@link EntityStorage} as new
 * {@link RyaStatement}s are added to/removed from the Rya instance.
 */
@DefaultAnnotation(NonNull.class)
public abstract class BaseEntityIndexer implements EntityIndexer, MongoSecondaryIndex {

    /**
     * When this URI is the Predicate of a Statement, it indicates a {@link Type} for an {@link Entity}.
     */
    private static final RyaURI TYPE_URI = new RyaURI( RDF.TYPE.toString() );

    private final AtomicReference<MongoDBRdfConfiguration> configuration = new AtomicReference<>();
    private final AtomicReference<EntityStorage> entities = new AtomicReference<>();
    private final AtomicReference<TypeStorage> types = new AtomicReference<>();

    @Override
    public void setConf(final Configuration conf) {
        requireNonNull(conf);
        entities.set( getEntityStorage(conf) );
        types.set( getTypeStorage(conf) );
    }

    @Override
    public Configuration getConf() {
        return configuration.get();
    }

    @Override
    public void storeStatement(final RyaStatement statement) throws IOException {
        requireNonNull(statement);
        storeStatements( singleton(statement) );
    }

    @Override
    public void storeStatements(final Collection<RyaStatement> statements) throws IOException {
        requireNonNull(statements);

        final Map<RyaURI,List<RyaStatement>> groupedBySubject = statements.stream()
            .collect(groupingBy(RyaStatement::getSubject));

        for(final Entry<RyaURI, List<RyaStatement>> entry : groupedBySubject.entrySet()) {
            try {
                updateEntity(entry.getKey(), entry.getValue());
            } catch (final IndexingException e) {
                throw new IOException("Failed to update the Entity index.", e);
            }
        }
    }

    /**
     * Updates a {@link Entity} to reflect new {@link RyaStatement}s.
     *
     * @param subject - The Subject of the {@link Entity} the statements are for. (not null)
     * @param statements - Statements that the {@link Entity} will be updated with. (not null)
     * @throws IndexingException
     */
    private void updateEntity(final RyaURI subject, final Collection<RyaStatement> statements) throws IndexingException {
        requireNonNull(subject);
        requireNonNull(statements);

        final EntityStorage entities = this.entities.get();
        final TypeStorage types = this.types.get();
        checkState(entities != null, "Must set this indexers configuration before storing statements.");
        checkState(types != null, "Must set this indexers configuration before storing statements.");

        new EntityUpdater(entities).update(subject, old -> {
            // Create a builder with the updated Version.
            final Entity.Builder updated;
            if(!old.isPresent()) {
                updated = Entity.builder()
                        .setSubject(subject)
                        .setVersion(0);
            } else {
                final int updatedVersion = old.get().getVersion() + 1;
                updated = Entity.builder(old.get())
                        .setVersion( updatedVersion );
            }

            // Update the entity based on the Statements.
            for(final RyaStatement statement : statements) {

                // The Statement is setting an Explicit Type ID for the Entity.
                if(Objects.equal(TYPE_URI, statement.getPredicate())) {
                    final RyaURI typeId = new RyaURI(statement.getObject().getData());
                    updated.setExplicitType(typeId);
                }

                // The Statement is adding a Property to the Entity.
                else {
                    final RyaURI propertyName = statement.getPredicate();
                    final RyaType propertyValue = statement.getObject();

                    try(final ConvertingCursor<Type> typesIt = types.search(propertyName)) {
                        // Set the Property for each type that includes the Statement's predicate.
                        while(typesIt.hasNext()) {
                            final RyaURI typeId = typesIt.next().getId();
                            updated.setProperty(typeId, new Property(propertyName, propertyValue));
                        }
                    } catch (final TypeStorageException | IOException e) {
                        throw new RuntimeException("Failed to fetch Types that include the property name '" +
                                statement.getPredicate().getData() + "'.", e);
                    }
                }
            }

            return Optional.of( updated.build() );
        });
    }

    @Override
    public void deleteStatement(final RyaStatement statement) throws IOException {
        requireNonNull(statement);

        final EntityStorage entities = this.entities.get();
        final TypeStorage types = this.types.get();
        checkState(entities != null, "Must set this indexers configuration before storing statements.");
        checkState(types != null, "Must set this indexers configuration before storing statements.");

        try {
            new EntityUpdater(entities).update(statement.getSubject(), old -> {
                // If there is no Entity for the subject of the statement, then do nothing.
                if(!old.isPresent()) {
                    return Optional.empty();
                }

                final Entity oldEntity = old.get();

                // Increment the version of the Entity.
                final Entity.Builder updated = Entity.builder(oldEntity);
                updated.setVersion(oldEntity.getVersion() + 1);

                if(TYPE_URI.equals(statement.getPredicate())) {
                    // If the Type ID already isn't in the list of explicit types, then do nothing.
                    final RyaURI typeId = new RyaURI( statement.getObject().getData() );
                    if(!oldEntity.getExplicitTypeIds().contains(typeId)) {
                        return Optional.empty();
                    }

                    // Otherwise remove it from the list.
                    updated.unsetExplicitType(typeId);
                } else {
                    // If the deleted property appears within the old entity's properties, then remove it.
                    final RyaURI deletedPropertyName = statement.getPredicate();

                    boolean propertyWasPresent = false;
                    for(final RyaURI typeId : oldEntity.getProperties().keySet()) {
                        for(final RyaURI propertyName : oldEntity.getProperties().get(typeId).keySet()) {
                            if(deletedPropertyName.equals(propertyName)) {
                                propertyWasPresent = true;
                                updated.unsetProperty(typeId, deletedPropertyName);
                            }
                        }
                    }

                    // If no properties were removed, then do nothing.
                    if(!propertyWasPresent) {
                        return Optional.empty();
                    }
                }

                return Optional.of( updated.build() );
            });
        } catch (final IndexingException e) {
            throw new IOException("Failed to update the Entity index.", e);
        }
    }

    @Override
    public String getTableName() {
        // Storage details have been abstracted away from the indexer.
        return null;
    }

    @Override
    public void flush() throws IOException {
        // We do not need to do anything to flush since we do not batch work.
    }

    @Override
    public void close() throws IOException {
        // Nothing to close.
    }

    @Override
    public void dropGraph(final RyaURI... graphs) {
        // We do not support graphs when performing entity centric indexing.
    }

    @Override
    public Set<URI> getIndexablePredicates() {
        // This isn't used anywhere in Rya, so it will not be implemented.
        return null;
    }
}