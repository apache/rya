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

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Serializable;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;
import net.jcip.annotations.Immutable;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;

import com.google.common.base.Objects;

/**
 * Metadata that is common to all nodes that are part of a query.
 */
@Immutable
@DefaultAnnotation(NonNull.class)
public abstract class CommonNodeMetadata {

    private final String nodeId;
    private VariableOrder varOrder;

    /**
     * Constructs an instance of {@link CommonNodeMetadata}.
     *
     * @param nodeId - The ID the Fluo app uses to reference this node. (not null)
     * @param varOrder - The variable order of binding sets that are emitted by this node. (not null)
     */
    public CommonNodeMetadata(
            final String nodeId,
            final VariableOrder varOrder) {
        this.nodeId = checkNotNull(nodeId);
        this.varOrder = checkNotNull(varOrder);
    }

    /**
     * @return The ID the Fluo app uses to reference this node.
     */
    public String getNodeId() {
        return nodeId;
    }

    /**
     * @return The variable order of binding sets that are emitted by this node.
     */
    public VariableOrder getVariableOrder() {
        return varOrder;
    }
    
    /**
     * Sets the VariableOrder for this node. Allows the VariableOrder to be updated.
     * @param varOrder - VariableOrder for this metadata node.
     */
    public void setVariableOrder(VariableOrder varOrder) {
        this.varOrder = varOrder;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(
                nodeId,
                varOrder);
    }

    @Override
    public boolean equals(final Object o) {
        if(this == o) {
            return true;
        }

        if(o instanceof CommonNodeMetadata) {
            final CommonNodeMetadata metadata = (CommonNodeMetadata)o;
            return new EqualsBuilder()
                    .append(nodeId, metadata.nodeId)
                    .append(varOrder, metadata.varOrder)
                    .isEquals();
        }

        return false;
    }

    @Override
    public String toString() {
        return new StringBuilder()
                .append("CommonNodeMetadata { ")
                .append("Node ID: " + nodeId )
                .append("  Variable Order: " + varOrder )
                .append(" }")
                .toString();
    }
    
    /**
     * Base interface for all metadata Builders.  Using this type def
     * allows for the implementation of a Builder visitor for navigating
     * the Builder tree.
     *
     */
    public static interface Builder {
        
        public String getNodeId();
        
        public VariableOrder getVariableOrder();
    }
    
}