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
package org.apache.rya.indexing.entity.model;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;

import org.apache.http.annotation.Immutable;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.indexing.entity.storage.EntityStorage;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * An {@link Entity} is a named concept that has at least one defined structure
 * and a bunch of values that fit within each of those structures. A structure is
 * defined by a {@link Type}. A value that fits within that Type is a {@link Property}.
 * </p>
 * For example, suppose we want to represent a type of icecream as an Entity.
 * First we must define what properties an icecream entity may have:
 * <pre>
 *                 Type ID: &lt;urn:icecream>
 *              Properties: &lt;urn:brand>
 *                          &lt;urn:flavor>
 *                          &lt;urn:ingredients>
 *                          &lt;urn:nutritionalInformation>
 * </pre>
 * Now we can represent our icecream whose brand is "Awesome Icecream" and whose
 * flavor is "Chocolate", but has no ingredients or nutritional information, as
 * an Entity by doing the following:
 * <pre>
 * final Entity entity = Entity.builder()
 *              .setSubject(new RyaURI("urn:GTIN-14/00012345600012"))
 *              .setExplicitType(new RyaURI("urn:icecream"))
 *              .setProperty(new RyaURI("urn:icecream"), new Property(new RyaURI("urn:brand"), new RyaType(XMLSchema.STRING, "Awesome Icecream")))
 *              .setProperty(new RyaURI("urn:icecream"), new Property(new RyaURI("urn:flavor"), new RyaType(XMLSchema.STRING, "Chocolate")))
 *              .build();
 * </pre>
 * The two types of Entities that may be created are implicit and explicit.
 * An implicit Entity is one who has at least one {@link Property} that matches
 * the {@link Type}, but nothing has explicitly indicated it is of  that Type.
 * Once something has done so, it is an explicitly typed Entity.
 */
@Immutable
@DefaultAnnotation(NonNull.class)
public class Entity {

    private final RyaURI subject;
    private final ImmutableList<RyaURI> explicitTypeIds;

    // First key is Type ID.
    // Second key is Property Name.
    // Value is the Property value for a specific type.
    private final ImmutableMap<RyaURI, ImmutableMap<RyaURI, Property>> properties;

    private final int version;

    /**
     * To construct an instances of this class, use {@link Builder}.
     */
    private Entity(
            final RyaURI subject,
            final ImmutableList<RyaURI> explicitTypeIds,
            final ImmutableMap<RyaURI, ImmutableMap<RyaURI, Property>> typeProperties,
            final int version) {
        this.subject = requireNonNull(subject);
        this.explicitTypeIds = requireNonNull(explicitTypeIds);
        properties = requireNonNull(typeProperties);
        this.version = version;
    }

    /**
     * @return Identifies the thing that is being represented as an Entity.
     */
    public RyaURI getSubject() {
        return subject;
    }

    /**
     * @return {@link Type}s that have been explicitly applied to the {@link Entity}.
     */
    public ImmutableList<RyaURI> getExplicitTypeIds() {
        return explicitTypeIds;
    }

    /**
     * @return All {@link Property}s that have been set for the Entity, grouped by Type ID.
     */
    public ImmutableMap<RyaURI, ImmutableMap<RyaURI, Property>> getProperties() {
        return properties;
    }

    /**
     * @return The version of this Entity. This value is used by the {@link EntityStorage}
     *   to prevent stale updates.
     */
    public int getVersion() {
        return version;
    }

    @Override
    public int hashCode() {
        return Objects.hash(subject, explicitTypeIds, properties, version);
    }

    @Override
    public boolean equals(final Object o) {
        if(this == o) {
            return true;
        }
        if(o instanceof Entity) {
            final Entity entity = (Entity) o;
            return Objects.equals(subject, entity.subject) &&
                    Objects.equals(explicitTypeIds, entity.explicitTypeIds) &&
                    Objects.equals(properties, entity.properties) &&
                    version == entity.version;
        }
        return false;
    }

    /**
     * Builds an {@link TypedEntity} using this object's values for the specified {@link Type}.
     *
     * @param typeId - The ID of the Type the TypedEntity will be for. (not null)
     * @return A TypedEntity using this object's values if any properties for the Type
     *    are present or if the Type was explicitly set. Otherwise an empty {@link Optional}.
     */
    public Optional<TypedEntity> makeTypedEntity(final RyaURI typeId) {
        requireNonNull(typeId);

        final boolean explicitlyHasType = explicitTypeIds.contains(typeId);
        final boolean hasTypesProperties = properties.containsKey(typeId);

        // The case where the MongoEntity can be represented as the typeId's Type.
        if(explicitlyHasType || hasTypesProperties) {
            // Set required fields.
            final TypedEntity.Builder builder = TypedEntity.builder()
                    .setId( subject )
                    .setTypeId( typeId )
                    .setExplicitelyTyped( explicitTypeIds.contains(typeId) );

            // Set Type's properties if present.
            if(properties.containsKey(typeId)) {
                properties.get(typeId).forEach( (propertyName, property) -> builder.setProperty(property));
            }

            return Optional.of( builder.build() );
        }

        // This MongoEntity can not be represented by the typeId's Type.
        return Optional.empty();
    }

    /**
     * @return An empty instance of {@link Builder}.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Create a {@link Builder} initialized with an {@link Entity}'s values.
     *
     * @param entity - The Entity the builder will be based on. (not null)
     * @return A {@link Builder} loaded with {@code entity}'s values.
     */
    public static Builder builder(final Entity entity) {
        return new Builder(entity);
    }

    /**
     * Builds instances of {@link Entity}.
     */
    @DefaultAnnotation(NonNull.class)
    public static class Builder {

        private RyaURI subject = null;
        private final List<RyaURI> explicitTypes = new ArrayList<>();
        private final Map<RyaURI, Map<RyaURI, Property>> properties = new HashMap<>();

        private int version = 0;

        /**
         * Constructs an empty instance of {@link Builder}.
         */
        public Builder() { }

        /**
         * Constructs an instance of {@link Builder}.
         *
         * @param entity - The Entity the builder will be based on. (not null)
         */
        public Builder(final Entity entity) {
            requireNonNull(entity);

            subject = entity.getSubject();
            explicitTypes.addAll( entity.getExplicitTypeIds() );

            for(final Entry<RyaURI, ImmutableMap<RyaURI, Property>> entry : entity.getProperties().entrySet()) {
                properties.put(entry.getKey(), Maps.newHashMap(entry.getValue()));
            }

            version = entity.getVersion();
        }

        /**
         * @param subject - Identifies the {@link TypedEntity}.
         * @return This {@link Builder} so that method invocations may be chained.
         */
        public Builder setSubject(@Nullable final RyaURI subject) {
            this.subject = subject;
            return this;
        }

        /**
         * @param typeId - A {@link Type} that has been explicity set for the {@link TypedEntity}.
         * @return This {@link Builder} so that method invocations may be chained.
         */
        public Builder setExplicitType(@Nullable final RyaURI typeId) {
            if(typeId != null) {
                explicitTypes.add(typeId);
            }
            return this;
        }

        /**
         * Removed a Type ID from the set of explicit Type IDs.
         *
         * @param typeId - The Type ID to remove from the set of explicit types.
         * @return This {@link Builder} so that method invocations may be chained.
         */
        public Builder unsetExplicitType(@Nullable final RyaURI typeId) {
            if(typeId != null) {
                explicitTypes.remove(typeId);
            }
            return this;
        }

        /**
         * Adds a {@link Property} for a specific {@link Type} of {@link TypedEntity}.
         *
         * @param typeId - The Type the Property is for.
         * @param property - The Property values to add.
         * @return This {@link Builder} so that method invocations may be chained.
         */
        public Builder setProperty(@Nullable final RyaURI typeId, @Nullable final Property property) {
            if(typeId != null && property != null) {
                if(!properties.containsKey(typeId)) {
                    properties.put(typeId, new HashMap<>());
                }

                properties.get(typeId).put(property.getName(), property);
            }
            return this;
        }

        /**
         * Removes a {@link Property} for a specific {@link Type} of {@link TypedEntity}.
         *
         * @param typeId - The Type the Property will be removed from.
         * @param propertyName - The name of the Property to remove.
         * @return This {@link Builder} so that method invocations may be chained.
         */
        public Builder unsetProperty(@Nullable final RyaURI typeId, @Nullable final RyaURI propertyName) {
            if(typeId != null && propertyName != null) {
                if(properties.containsKey(typeId)) {
                    final Map<RyaURI, Property> typedProperties = properties.get(typeId);
                    if(typedProperties.containsKey(propertyName)) {
                        typedProperties.remove(propertyName);
                    }
                }
            }
            return this;
        }

        /**
         * @param version - The version of this Entity. This value is used by the
         * {@link EntityStorage} to prevent stale updates.
         * @return This {@link Builder} so that method invocations may be chained.
         */
        public Builder setVersion(final int version) {
            this.version = version;
            return this;
        }

        /**
         * @return Builds an instance of {@link Entity} using this builder's values.
         */
        public Entity build() {
            final ImmutableMap.Builder<RyaURI, ImmutableMap<RyaURI, Property>> propertiesBuilder = ImmutableMap.builder();
            for(final Entry<RyaURI, Map<RyaURI, Property>> entry : properties.entrySet()) {
                propertiesBuilder.put(entry.getKey(), ImmutableMap.copyOf( entry.getValue() ));
            }

            return new Entity(subject,
                    ImmutableList.copyOf( explicitTypes ),
                    propertiesBuilder.build(),
                    version);
        }
    }
}