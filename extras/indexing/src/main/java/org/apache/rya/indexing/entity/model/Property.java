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

import java.util.Objects;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;

import mvm.rya.api.domain.RyaType;
import mvm.rya.api.domain.RyaURI;

/**
 * A value that has been set for an {@link TypedEntity}.
 */
@Immutable
@ParametersAreNonnullByDefault
public class Property {

    private final RyaURI name;
    private final RyaType value;

    /**
     * Constructs an instance of {@link Property}.
     *
     * @param name - Uniquely identifies the {@link Property}. (not null)
     * @param value - The value of the {@link Property}. (not null)
     */
    public Property(final RyaURI name, final RyaType value) {
        this.name = requireNonNull(name);
        this.value = requireNonNull(value);
    }

    /**
     * @return Uniquely identifies the {@link Property}.
     */
    public RyaURI getName() {
        return name;
    }

    /**
     * @return The value of the {@link Property}.
     */
    public RyaType getValue() {
        return value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, value);
    }

    @Override
    public boolean equals(final Object o) {
        if(this == o) {
            return true;
        }
        if(o instanceof Property) {
            final Property field = (Property) o;
            return Objects.equals(name, field.name) &&
                    Objects.equals(value, field.value);
        }
        return false;
    }
}