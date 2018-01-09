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
package org.apache.rya.api.function.aggregation;

import static java.util.Objects.requireNonNull;

import java.io.Serializable;
import java.util.Objects;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Represents all of the metadata require to perform an Aggregation that is part of a SPARQL query.
 * </p>
 * For example, if you have the following in SPARQL:
 * <pre>
 * SELECT (avg(?price) as ?avgPrice) {
 *     ...
 * }
 * </pre>
 * You would construct an instance of this object like so:
 * <pre>
 * new AggregationElement(AggregationType.AVERAGE, "price", "avgPrice");
 * </pre>
 */
@DefaultAnnotation(NonNull.class)
public final class AggregationElement implements Serializable {
    private static final long serialVersionUID = 1L;

    private final AggregationType aggregationType;
    private final String aggregatedBindingName;
    private final String resultBindingName;

    /**
     * Constructs an instance of {@link AggregationElement}.
     *
     * @param aggregationType - Defines how the binding values will be aggregated. (not null)
     * @param aggregatedBindingName - The name of the binding whose values is aggregated. This binding must
     *   appear within the child node's emitted binding sets. (not null)
     * @param resultBindingName - The name of the binding this aggregation's results are written to. This binding
     *   must appeared within the AggregationMetadata's variable order. (not null)
     */
    public AggregationElement(
            final AggregationType aggregationType,
            final String aggregatedBindingName,
            final String resultBindingName) {
        this.aggregationType = requireNonNull(aggregationType);
        this.aggregatedBindingName = requireNonNull(aggregatedBindingName);
        this.resultBindingName = requireNonNull(resultBindingName);
    }

    /**
     * @return Defines how the binding values will be aggregated.
     */
    public AggregationType getAggregationType() {
        return aggregationType;
    }

    /**
     * @return The name of the binding whose values is aggregated. This binding must appear within the child node's emitted binding sets.
     */
    public String getAggregatedBindingName() {
        return aggregatedBindingName;
    }

    /**
     * @return The name of the binding this aggregation's results are written to. This binding must appeared within the AggregationMetadata's variable order.
     */
    public String getResultBindingName() {
        return resultBindingName;
    }

    @Override
    public int hashCode() {
        return Objects.hash(aggregationType, aggregatedBindingName, resultBindingName);
    }

    @Override
    public boolean equals(final Object o ) {
        if(o instanceof AggregationElement) {
            final AggregationElement agg = (AggregationElement) o;
            return Objects.equals(aggregationType, agg.aggregationType) &&
                    Objects.equals(aggregatedBindingName, agg.aggregatedBindingName) &&
                    Objects.equals(resultBindingName, agg.resultBindingName);
        }
        return false;
    }
}