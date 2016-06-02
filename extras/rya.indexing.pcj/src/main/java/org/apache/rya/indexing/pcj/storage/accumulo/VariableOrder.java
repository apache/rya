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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.Iterator;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;

import org.openrdf.query.BindingSet;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

/**
 * An ordered list of {@link BindingSet} variable names. These are used to
 * specify the order {@link Binding}s within the set are serialized to Accumulo.
 * This order effects which rows a prefix scan will hit.
 */
@Immutable
@ParametersAreNonnullByDefault
public final class VariableOrder implements Iterable<String> {

    public static final String VAR_ORDER_DELIM = ";";

    private final ImmutableList<String> variableOrder;

    /**
     * Constructs an instance of {@link VariableOrder}.
     *
     * @param varOrder - An ordered array of Binding Set variables. (not null)
     */
    public VariableOrder(final String... varOrder) {
        checkNotNull(varOrder);
        variableOrder = ImmutableList.copyOf(varOrder);
    }

    /**
     * Constructs an instance of {@link VariableOrdeR{.
     *
     * @param varOrder - An ordered collection of Binding Set variables. (not null)
     */
    public VariableOrder(final Collection<String> varOrder) {
        checkNotNull(varOrder);
        variableOrder = ImmutableList.copyOf(varOrder);
    }

    /**
     * Constructs an instance of {@link VariableOrder}.
     *
     * @param varOrderString - The String representation of a VariableOrder. (not null)
     */
    public VariableOrder(final String varOrderString) {
        checkNotNull(varOrderString);
        variableOrder = ImmutableList.copyOf( varOrderString.split(VAR_ORDER_DELIM) );
    }

    /**
     * @return And ordered list of Binding Set variables.
     */
    public ImmutableList<String> getVariableOrders() {
        return variableOrder;
    }

    /**
     * @return The variable order as an ordered array of Strings. This array is mutable.
     */
    public String[] toArray() {
        final String[] array = new String[ variableOrder.size() ];
        return variableOrder.toArray( array );
    }

    @Override
    public String toString() {
        return Joiner.on(VAR_ORDER_DELIM).join(variableOrder);
    }

    @Override
    public int hashCode() {
        return variableOrder.hashCode();
    }

    @Override
    public boolean equals(final Object o) {
        if(this == o) {
            return true;
        } else if(o instanceof VariableOrder) {
            final VariableOrder varOrder = (VariableOrder) o;
            return variableOrder.equals( varOrder.variableOrder );
        }
        return false;
    }

    @Override
    public Iterator<String> iterator() {
        return variableOrder.iterator();
    }
}