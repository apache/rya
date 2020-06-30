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

import com.google.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;
import jdk.nashorn.internal.ir.annotations.Immutable;
import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.indexing.entity.storage.TypeStorage;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Defines the structure of an {@link TypedEntity}.
 * </p>
 * For example, suppose you want a {@link Type} that defines what properties are
 * available for icecream. It could be modeled like this:
 * <pre>
 *                 Type ID: &lt;urn:icecream>
 *              Properties: &lt;urn:brand>
 *                          &lt;urn:flavor>
 *                          &lt;urn:ingredients>
 *                          &lt;urn:nutritionalInformation>
 * </pre>
 */
@Immutable
@DefaultAnnotation(NonNull.class)
public class Type {

    /**
     * Uniquely identifies the Type within a {@link TypeStorage}.
     */
    private final RyaIRI id;

    /**
     * The names of {@link Property}s that may be part of an {@link TypedEntity} of this type.
     */
    private final ImmutableSet<RyaIRI> propertyNames;

    /**
     * Constructs an instance of {@link Type}.
     *
     * @param id - Uniquely identifies the Type within a {@link TypeStorage}. (not null)
     * @param propertyNames - The names of {@link Property}s that may be part of an {@link TypedEntity} of this type. (not null)
     */
    public Type(final RyaIRI id, final ImmutableSet<RyaIRI> propertyNames) {
        this.id = requireNonNull(id);
        this.propertyNames = requireNonNull(propertyNames);
    }

    /**
     * @return Uniquely identifies the Type within a {@link TypeStorage}.
     */
    public RyaIRI getId() {
        return id;
    }

    /**
     * @return The names of {@link Property}s that may be part of an {@link TypedEntity} of this type.
     */
    public ImmutableSet<RyaIRI> getPropertyNames() {
        return propertyNames;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, propertyNames);
    }

    @Override
    public boolean equals(final Object o) {
        if(this == o) {
            return true;
        }
        if(o instanceof Type) {
            final Type type = (Type) o;
            return Objects.equals(id, type.id) &&
                    Objects.equals(propertyNames, type.propertyNames);
        }
        return false;
    }
}