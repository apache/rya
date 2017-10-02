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
package org.apache.rya.api.model;

import static java.util.Objects.requireNonNull;

import java.util.Objects;

import org.openrdf.query.BindingSet;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Decorates a {@link BindingSet} with a collection of visibilities.
 */
@DefaultAnnotation(NonNull.class)
public class VisibilityBindingSet extends BindingSetDecorator {
    private static final long serialVersionUID = 1L;

    private String visibility;

    /**
     * Creates a new {@link VisibilityBindingSet} that does not have any visibilities
     * associated with it.
     *
     * @param set - Decorates the {@link BindingSet} with no visibilities. (not null)
     */
    public VisibilityBindingSet(final BindingSet set) {
        this(set, "");
    }

    /**
     * Creates a new {@link VisibilityBindingSet}.
     *
     * @param set - The {@link BindingSet} to decorate. (not null)
     * @param visibility - The Visibilities on the {@link BindingSet}. (not null)
     */
    public VisibilityBindingSet(final BindingSet set, final String visibility) {
        super(set);
        this.visibility = requireNonNull(visibility);
    }

    /**
     * @param visibility - The Visibilities on the {@link BindingSet}. (not null)
     */
    public void setVisibility(final String visibility) {
        requireNonNull(visibility);
        this.visibility = visibility;
    }

    /**
     * @return The Visibilities on the {@link BindingSet}.
     */
    public String getVisibility() {
        return visibility;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        } else if(o instanceof VisibilityBindingSet) {
            final VisibilityBindingSet other = (VisibilityBindingSet) o;
            return getBindingSet().equals(other) && visibility.equals(other.getVisibility());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(visibility, super.getBindingSet());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(super.toString());
        sb.append("\n  Visibility: " + getVisibility() + "\n");
        return sb.toString();
    }
}