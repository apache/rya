package org.apache.rya.indexing.pcj.fluo.app.query;

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
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.rya.indexing.pcj.fluo.app.ConstructGraph;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.openrdf.query.BindingSet;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Metadata object used to store metadata for Construct Query Nodes found in
 * SPARQL queries.
 *
 */
public class ConstructQueryMetadata extends CommonNodeMetadata {

    private String childNodeId;
    private ConstructGraph graph;
    private String sparql;

    /**
     * Creates ConstructQueryMetadata object from the provided metadata arguments.
     * @param nodeId - id for the ConstructQueryNode
     * @param childNodeId - id for the child of the ConstructQueryNode
     * @param graph - {@link ConstructGraph} used to project {@link BindingSet}s onto sets of statement representing construct graph
     * @param sparql - SPARQL query containing construct graph
     */
    public ConstructQueryMetadata(String nodeId, String childNodeId, ConstructGraph graph, String sparql) {
        super(nodeId, new VariableOrder("subject", "predicate", "object"));
        Preconditions.checkNotNull(childNodeId);
        Preconditions.checkNotNull(graph);
        Preconditions.checkNotNull(sparql);
        this.childNodeId = childNodeId;
        this.graph = graph;
        this.sparql = sparql;
    }

    /**
     * @return sparql query string representing this construct query
     */
    public String getSparql() {
        return sparql;
    }

    /**
     * @return The node whose results are projected onto the given
     *         {@link ConstructGraph}.
     */
    public String getChildNodeId() {
        return childNodeId;
    }

    /**
     * @return The ConstructGraph used to form statement {@link BindingSet}s for
     *         this Construct Query
     */
    public ConstructGraph getConstructGraph() {
        return graph;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.getNodeId(), super.getVariableOrder(), childNodeId, graph, sparql);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o instanceof ConstructQueryMetadata) {
            ConstructQueryMetadata queryMetadata = (ConstructQueryMetadata) o;
            if (super.equals(queryMetadata)) {
                return new EqualsBuilder().append(childNodeId, queryMetadata.childNodeId).append(graph, queryMetadata.graph)
                        .append(sparql, queryMetadata.sparql).isEquals();
            }
            return false;
        }
        return false;
    }

    @Override
    public String toString() {
        return new StringBuilder().append("Construct Query Metadata {\n").append("    Node ID: " + super.getNodeId() + "\n")
                .append("    SPARQL QUERY: " + sparql + "\n").append("    Variable Order: " + super.getVariableOrder() + "\n")
                .append("    Child Node ID: " + childNodeId + "\n").append("    Construct Graph: " + graph.getProjections() + "\n")
                .append("}").toString();
    }

    /**
     * Creates a new {@link Builder} for this class.
     *
     * @return A new {@link Builder} for this class.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builds instances of {@link QueryMetadata}.
     */
    @DefaultAnnotation(NonNull.class)
    public static final class Builder {


        private String nodeId;
        private ConstructGraph graph;
        private String childNodeId;
        private String sparql;

        /**
         * Set the node Id that identifies this Construct Query Node
         * 
         * @param nodeId
         *            id for this node
         * @return This builder so that method invocations may be chained.
         */
        public Builder setNodeId(String nodeId) {
            this.nodeId = nodeId;
            return this;
        }
        
        /**
         * Set the SPARQL String representing this construct query
         * @param SPARQL string representing this construct query
         */
        public Builder setSparql(String sparql) {
            this.sparql = sparql;
            return this;
        }

        /**
         * Set the ConstructGraph used to form statement {@link BindingSet}s for
         * this Construct Query
         *
         * @param varOrder
         *            - ConstructGraph to project {@link BindingSet}s onto RDF
         *            statements
         * @return This builder so that method invocations may be chained.
         */
        public Builder setConstructGraph(ConstructGraph graph) {
            this.graph = graph;
            return this;
        }

        /**
         * Set the node whose results are projected onto the given
         * {@link ConstructGraph}.
         *
         * @param childNodeId
         *            - The node whose results are projected onto the given
         *            {@link ConstructGraph}.
         * @return This builder so that method invocations may be chained.
         */
        public Builder setChildNodeId(String childNodeId) {
            this.childNodeId = childNodeId;
            return this;
        }

        /**
         * @return An instance of {@link ConstructQueryMetadata} build using
         *         this builder's values.
         */
        public ConstructQueryMetadata build() {
            return new ConstructQueryMetadata(nodeId, childNodeId, graph, sparql);
        }
    }

}
