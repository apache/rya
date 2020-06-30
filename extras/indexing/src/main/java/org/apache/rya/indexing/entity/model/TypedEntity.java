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

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import jdk.nashorn.internal.ir.annotations.Immutable;
import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.api.domain.RyaType;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * A {@link TypedEntity} is a view of an {@link Entity} that has had a specific
 * {@link Type} applied to it.
 */
@Immutable
@DefaultAnnotation(NonNull.class)
public class TypedEntity {

    /**
     * The Subject of the {@link Entity} this view was derived from.
     */
    private final RyaIRI subject;

    /**
     * The ID of the {@link Type} that defines the structure of this TypedEntity.
     */
    private final RyaIRI typeId;

    /**
     * {@code true} if the Entity's Type has been explicitly set to the {@link #typeId}
     * value; {@code false} if it is implicit (this Entity only exists because it has
     * properties that happen to match that type's properties).
     */
    private final boolean explicitlyTyped;

    /**
     * The optional {@link Property} values of this {@link TypedEntity}.
     * </p>
     * They are mapped from property name to property object for quick lookup.
     */
    private final ImmutableMap<RyaIRI, Property> optionalFields;

    /**
     * Constructs an instance of {@link TypedEntity}.
     *
     * @param subject - The Subject of the {@link Entity} this view was derived from. (not null)
     * @param dataTypeId - The ID of the {@link Type} that defines the structure of this Entity. (not null)
     * @param explicitlyTyped - {@code true} if the Entity's Type has been explicitly set to the
     *   {@link #typeId} value; {@code false} if it is implicit (this Entity only exists because
     *   it has properties that happen to match that type's properties).
     * @param properties - The optional {@link Property} values of this {@link TypedEntity}. (not null)
     */
    private TypedEntity(final RyaIRI subject,
            final RyaIRI dataTypeId,
            final boolean explicitlyTyped,
            final ImmutableMap<RyaIRI, Property> optionalFields) {
        this.subject = requireNonNull(subject);
        typeId = requireNonNull(dataTypeId);
        this.optionalFields = requireNonNull(optionalFields);
        this.explicitlyTyped = explicitlyTyped;
    }

    /**
     * @return The Subject of the {@link Entity} this view was derived from.
     */
    public RyaIRI getSubject() {
        return subject;
    }

    /**
     * @return The ID of the {@link Type} that defines the structure of this Entity.
     */
    public RyaIRI getTypeId() {
        return typeId;
    }

    /**
     * @return {@code true} if the Entity's Type has been explicitly set to the {@link #typeId}
     *   value; {@code false} if it is implicit (this Entity only exists because it has
     *   properties that happen to match that type's properties).
     */
    public boolean isExplicitlyTyped() {
        return explicitlyTyped;
    }

    /**
     * @return The optional {@link Property} values of this {@link TypedEntity}.
     */
    public ImmutableCollection<Property> getProperties() {
        return optionalFields.values();
    }

    /**
     * Get the value of a specific {@link Property} of this {@link TypedEntity}
     * if the property has been set.
     *
     * @param propertyName - The name of {@link Property} that may be in this Entity. (not null)
     * @return The value of the Property if it has been set.
     */
    public Optional<RyaType> getPropertyValue(final RyaIRI propertyName) {
        requireNonNull(propertyName);

        final Property field = optionalFields.get(propertyName);
        return field == null ?
                Optional.empty() :
                Optional.of( field.getValue() );
    }

    @Override
    public int hashCode() {
        return Objects.hash(subject, typeId, optionalFields);
    }

    @Override
    public boolean equals(final Object o) {
        if(this == o) {
            return true;
        }
        if(o instanceof TypedEntity) {
            final TypedEntity other = (TypedEntity) o;
            return Objects.equals(subject, other.subject) &&
                    Objects.equals(typeId, other.typeId) &&
                    Objects.equals(optionalFields, other.optionalFields);
        }
        return false;
    }

    /**
     * @return An empty instance of {@link Builder}.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Makes a {@link Builder} that is populated with an existing {@link TypedEntity}.
     *
     * @param entity - The initial values of the builder. (not null)
     * @return An instance of {@link Builder} loaded with {@code entity}'s values.
     */
    public static Builder builder(final TypedEntity entity) {
        requireNonNull(entity);

        final Builder builder = builder()
                .setId(entity.getSubject())
                .setTypeId(entity.getTypeId());

        entity.getProperties().forEach(builder::setProperty);

        return builder;
    }

    /**
     * Builds instances of {@link TypedEntity}.
     */
    @DefaultAnnotation(NonNull.class)
    public static class Builder {

        private RyaIRI subject;
        private RyaIRI typeId;
        private boolean explicitlyTyped = false;
        private final Map<RyaIRI, Property> properties = new HashMap<>();

        /**
         * @param subject - The Subject of the {@link Entity} this view was derived from.
         * @return This {@link Builder} so that method invocations may be chained.
         */
        public Builder setId(@Nullable final RyaIRI subject) {
            this.subject = subject;
            return this;
        }

        /**
         * @param typeId -  The ID of the {@link Type} that defines the structure of this Entity.
         * @return This {@link Builder} so that method invocations may be chained.
         */
        public Builder setTypeId(@Nullable final RyaIRI typeId) {
            this.typeId = typeId;
            return this;
        }

        /**
         * @param explicitlyTyped - {@code true} if the Entity's Type has been explicitly
         * set to the {@link #typeId} value; {@code false} if it is implicit (this Entity
         * only exists because it has properties that happen to match that type's properties).
         * @return This {@link Builder} so that method invocations may be chained.
         */
        public Builder setExplicitelyTyped(final boolean explicitlyTyped) {
            this.explicitlyTyped = explicitlyTyped;
            return this;
        }

        /**
         * @param property - A {@link Property} of the {@link TypedEntity}.
         * @return This {@link Builder} so that method invocations may be chained.
         */
        public Builder setProperty(@Nullable final Property property) {
            if(property != null) {
                properties.put(property.getName(), property);
            }
            return this;
        }

        /**
         * @return An instance of {@link TypedEntity} built with this builder's values.
         */
        public TypedEntity build() {
            return new TypedEntity(
                    subject,
                    typeId,
                    explicitlyTyped,
                    ImmutableMap.copyOf(properties));
        }
    }
}