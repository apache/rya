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
package org.apache.rya.indexing.pcj.storage.accumulo;

import java.util.Iterator;
import java.util.Set;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;

import static java.util.Objects.requireNonNull;

/**
 * Abstracts out the decoration of a {@link BindingSet}.
 */
public abstract class BindingSetDecorator implements BindingSet {
    private static final long serialVersionUID = 1L;

    private final BindingSet set;

    /**
     * Constructs a new {@link BindingSetDecorator}, decorating the provided
     * {@link BindingSet}.
     *
     * @param set - The {@link BindingSet} to be decorated. (not null)
     */
    public BindingSetDecorator(final BindingSet set) {
        this.set = requireNonNull(set);
    }

    /**
     * @return The decorated {@link BindingSet}.
     */
    public BindingSet getBindingSet() {
        return set;
    }

    @Override
    public Iterator<Binding> iterator() {
        return set.iterator();
    }

    @Override
    public Set<String> getBindingNames() {
        return set.getBindingNames();
    }

    @Override
    public Binding getBinding(final String bindingName) {
        return set.getBinding(bindingName);
    }

    @Override
    public boolean hasBinding(final String bindingName) {
        return set.hasBinding(bindingName);
    }

    @Override
    public Value getValue(final String bindingName) {
        return set.getValue(bindingName);
    }

    @Override
    public int size() {
        return set.size();
    }

    @Override
    public boolean equals(final Object o) {
        if(!(o instanceof BindingSetDecorator)) {
            return false;
        }
        final BindingSetDecorator other = (BindingSetDecorator) o;
        return set.equals(other.set);
    }

    @Override
    public int hashCode() {
        return set.hashCode();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("  names: ");
        for (final String name : getBindingNames()) {
            sb.append("\n    [name]: " + name + "  ---  [value]: " + getBinding(name).getValue().toString());
        }
        return sb.toString();
    }
}