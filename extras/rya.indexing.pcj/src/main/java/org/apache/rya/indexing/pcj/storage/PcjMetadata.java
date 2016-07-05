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
package org.apache.rya.indexing.pcj.storage;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.Objects;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;

import com.google.common.collect.ImmutableSet;

/**
 * Metadata that is stored in a PCJ table about the results that are stored within it.
 */
@Immutable
@ParametersAreNonnullByDefault
public final class PcjMetadata {
    private final String sparql;
    private final long cardinality;
    private final ImmutableSet<VariableOrder> varOrders;

    /**
     * Constructs an instance of {@link PcjMetadata}.
     *
     * @param sparql - The SPARQL query this PCJ solves. (not null)
     * @param cardinality  - The number of results the PCJ has. (>= 0)
     * @param varOrders - Strings that describe each of the variable orders
     *   the results are stored in. (not null)
     */
    public PcjMetadata(final String sparql, final long cardinality, final Collection<VariableOrder> varOrders) {
        this.sparql = checkNotNull(sparql);
        this.varOrders = ImmutableSet.copyOf( checkNotNull(varOrders) );

        checkArgument(cardinality >= 0, "Cardinality of a PCJ must be >= 0. Was: " + cardinality);
        this.cardinality = cardinality;
    }

    /**
     * @return The SPARQL query this PCJ solves.
     */
    public String getSparql() {
        return sparql;
    }

    /**
     * @return The number of results the PCJ has.
     */
    public long getCardinality() {
        return cardinality;
    }

    /**
     * @return Strings that describe each of the variable orders the results are stored in.
     */
    public ImmutableSet<VariableOrder> getVarOrders() {
        return varOrders;
    }

    /**
     * Updates the cardinality of a {@link PcjMetadata} by a {@code delta}.
     *
     * @param metadata - The PCJ metadata to update. (not null)
     * @param delta - How much the cardinality of the PCJ has changed.
     * @return A new instance of {@link PcjMetadata} with the new cardinality.
     */
    public static PcjMetadata updateCardinality(final PcjMetadata metadata, final long delta) {
        checkNotNull(metadata);
        return new PcjMetadata(metadata.sparql, metadata.cardinality + delta, metadata.varOrders);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sparql, cardinality, varOrders);
    }

    @Override
    public boolean equals(final Object o) {
        if(this == o) {
            return true;
        } else if(o instanceof PcjMetadata) {
            final PcjMetadata metadata = (PcjMetadata) o;
            return new EqualsBuilder()
                    .append(sparql, metadata.sparql)
                    .append(cardinality, metadata.cardinality)
                    .append(varOrders, metadata.varOrders)
                    .build();
        }
        return false;
    }
}