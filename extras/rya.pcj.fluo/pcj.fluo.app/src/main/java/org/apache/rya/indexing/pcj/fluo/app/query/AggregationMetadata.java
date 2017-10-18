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
package org.apache.rya.indexing.pcj.fluo.app.query;

import java.io.Serializable;
import java.util.*;

import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import net.jcip.annotations.Immutable;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.eclipse.rdf4j.query.algebra.*;

import static java.util.Objects.requireNonNull;

/**
 * Metadata that is relevant to Aggregate nodes.
 */
@Immutable
@DefaultAnnotation(NonNull.class)
public class AggregationMetadata extends CommonNodeMetadata {

    /**
     * The different types of Aggregation functions that an aggregate node may perform.
     */
    public enum AggregationType {
        MIN(Min.class),
        MAX(Max.class),
        COUNT(Count.class),
        SUM(Sum.class),
        AVERAGE(Avg.class);

        private final Class<? extends AggregateOperator> operatorClass;

        AggregationType(final Class<? extends AggregateOperator> operatorClass) {
            this.operatorClass = requireNonNull(operatorClass);
        }

        private static final ImmutableMap<Class<? extends AggregateOperator>, AggregationType> byOperatorClass;
        static {
            final ImmutableMap.Builder<Class<? extends AggregateOperator>, AggregationType> builder = ImmutableMap.builder();
            for(final AggregationType type : AggregationType.values()) {
                builder.put(type.operatorClass, type);
            }
            byOperatorClass = builder.build();
        }

        public static Optional<AggregationType> byOperatorClass(final Class<? extends AggregateOperator> operatorClass) {
            return Optional.ofNullable( byOperatorClass.get(operatorClass) );
        }
    }

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
    @Immutable
    @DefaultAnnotation(NonNull.class)
    public static final class AggregationElement implements Serializable {
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

    private final String parentNodeId;
    private final String childNodeId;
    private final Collection<AggregationElement> aggregations;
    private final VariableOrder groupByVariables;

    /**
     * Constructs an instance of {@link AggregationMetadata}.
     *
     * @param nodeId - The ID the Fluo app uses to reference this node. (not null)
     * @param varOrder - The variable order of binding sets that are emitted by this node. This may only contain a
     *   single variable because aggregations are only able to emit the aggregated value. (not null)
     * @param parentNodeId - The Node ID of this node's parent. This is the node that will consume the results of the aggregations. (not null)
     * @param childNodeId - The Node ID of this node's child. This is the node that will feed binding sets into the aggregations. (not null)
     * @param aggregations - The aggregations that will be performed over the BindingSets that are emitted from the child node. (not null)
     * @param groupByVariables - Defines how the data is grouped for the aggregation function. (not null, may be empty if no grouping is required)
     */
    public AggregationMetadata(
            final String nodeId,
            final VariableOrder varOrder,
            final String parentNodeId,
            final String childNodeId,
            final Collection<AggregationElement> aggregations,
            final VariableOrder groupByVariables) {
        super(nodeId, varOrder);
        this.parentNodeId = requireNonNull(parentNodeId);
        this.childNodeId = requireNonNull(childNodeId);
        this.aggregations = requireNonNull(aggregations);
        this.groupByVariables = requireNonNull(groupByVariables);
    }

    /**
     * @return The Node ID of this node's parent. This is the node that will consume the results of the aggregations.
     */
    public String getParentNodeId() {
        return parentNodeId;
    }

    /**
     * @return The Node ID of this node's child. This is the node that will feed binding sets into the aggregations.
     */
    public String getChildNodeId() {
        return childNodeId;
    }

    /**
     * @return The aggregations that will be performed over the BindingSets that are emitted from the child node.
     */
    public Collection<AggregationElement> getAggregations() {
        return aggregations;
    }

    /**
     * @return Defines how the data is grouped for the aggregation function.
     */
    public VariableOrder getGroupByVariableOrder() {
        return groupByVariables;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                super.getNodeId(),
                super.getVariableOrder(),
                parentNodeId,
                childNodeId,
                aggregations,
                groupByVariables);
    }

    @Override
    public boolean equals(final Object o) {
        if(o instanceof AggregationMetadata) {
            final AggregationMetadata metadata = (AggregationMetadata) o;
            return Objects.equals(getNodeId(), metadata.getNodeId()) &&
                    Objects.equals(super.getVariableOrder(), metadata.getVariableOrder()) &&
                    Objects.equals(parentNodeId, metadata.parentNodeId) &&
                    Objects.equals(childNodeId, metadata.childNodeId) &&
                    Objects.equals(aggregations, metadata.aggregations) &&
                    Objects.equals(groupByVariables, metadata.groupByVariables);
        }
        return false;
    }

    @Override
    public String toString() {
        final StringBuilder string = new StringBuilder()
                .append("AggregationMetadata {\n")
                .append("    Node ID: " + super.getNodeId() + "\n")
                .append("    Variable Order: " + super.getVariableOrder() + "\n")
                .append("    Parent Node ID: " + parentNodeId + "\n")
                .append("    Child Node ID: " + childNodeId + "\n");

        // Only print the group by names if they're preesnt.
        if(!groupByVariables.getVariableOrders().isEmpty()) {
            string.append("    GroupBy Variable Order: " + groupByVariables + "\n");
        }

        // Print each of the AggregationElements.
        string.append("    Aggregations: {\n");
        final Iterator<AggregationElement> it = aggregations.iterator();
        while(it.hasNext()) {
            final AggregationElement agg = it.next();
            string.append("        Type: " + agg.getAggregationType() + "\n");
            string.append("        Aggregated Binding Name: " + agg.getAggregatedBindingName() + "\n");
            string.append("        Result Binding Name: " + agg.getResultBindingName() + "\n");

            if(it.hasNext()) {
                string.append("\n");
            }
        }
        string.append("    }\n");
        string.append("}");

        return string.toString();
    }

    /**
     * @param nodeId - The ID the Fluo app uses to reference this node. (not null)
     * @return A new {@link Builder} initialized with the provided nodeId.
     */
    public static Builder builder(final String nodeId) {
        return new Builder(nodeId);
    }

    /**
     * Builds instances of {@link AggregationMetadata}.
     */
    @DefaultAnnotation(NonNull.class)
    public static final class Builder implements CommonNodeMetadata.Builder {

        private final String nodeId;
        private VariableOrder varOrder;
        private String parentNodeId;
        private String childNodeId;
        private final List<AggregationElement> aggregations = new ArrayList<>();
        private VariableOrder groupByVariables = new VariableOrder();

        /**
         * Constructs an instance of {@link Builder}.
         *
         * @param nodeId - This node's Node ID. (not null)
         */
        public Builder(final String nodeId) {
            this.nodeId = requireNonNull(nodeId);
        }

        /**
         * @return This node's Node ID.
         */
        public String getNodeId() {
            return nodeId;
        }

        /**
         * @param varOrder - The variable order of binding sets that are emitted by this node. This may only contain a
         *   single variable because aggregations are only able to emit the aggregated value.
         * @return This builder so that method invocations may be chained.
         */
        public Builder setVarOrder(@Nullable final VariableOrder varOrder) {
            this.varOrder = varOrder;
            return this;
        }
        
        /**
         * @return the variable order of binding sets that are emitted by this node.
         */
        public VariableOrder getVariableOrder() {
            return varOrder;
        }

        /**
         * @param parentNodeId - The Node ID of this node's parent.
         * @return This builder so that method invocations may be chained.
         */
        public Builder setParentNodeId(@Nullable final String parentNodeId) {
            this.parentNodeId = parentNodeId;
            return this;
        }
       
        public String getParentNodeId() {
            return parentNodeId;
        }

        /**
         * @param childNodeId - The Node ID of this node's child.
         * @return This builder so that method invocations may be chained.
         */
        public Builder setChildNodeId(@Nullable final String childNodeId) {
            this.childNodeId = childNodeId;
            return this;
        }
        
        public String getChildNodeId() {
            return childNodeId;
        }

        /**
         * @param aggregation - An aggregation that will be performed over the BindingSets that are emitted from the child node.
         * @return This builder so that method invocations may be chained.
         */
        public Builder addAggregation(@Nullable final AggregationElement aggregation) {
            if(aggregation != null) {
                this.aggregations.add(aggregation);
            }
            return this;
        }

        /**
         * @param groupByBindingNames - Defines how the data is grouped for the aggregation function. (not null, may be
         *   empty if no grouping is required)
         * @return This builder so that method invocations may be chained.
         */
        public Builder setGroupByVariableOrder(@Nullable final VariableOrder groupByVariables) {
            this.groupByVariables = groupByVariables;
            return this;
        }
        
        /**
         * @return variable order that defines how data is grouped for the aggregation function
         */
        public VariableOrder getGroupByVariableOrder() {
            return groupByVariables;
        }

        /**
         * @return An instance of {@link AggregationMetadata} build using this builder's values.
         */
        public AggregationMetadata build() {
            return new AggregationMetadata(nodeId, varOrder, parentNodeId, childNodeId, aggregations, groupByVariables);
        }
    }
}