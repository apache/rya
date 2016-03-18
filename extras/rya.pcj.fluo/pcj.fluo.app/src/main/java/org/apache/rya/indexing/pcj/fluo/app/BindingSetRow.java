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
package org.apache.rya.indexing.pcj.fluo.app;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.DELIM;
import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.NODEID_BS_DELIM;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;

import io.fluo.api.data.Bytes;

/**
 * The values of an Accumulo Row ID for a row that stores a Binding set for
 * a specific Node ID of a query.
 */
@Immutable
@ParametersAreNonnullByDefault
public class BindingSetRow {
    private final String nodeId;
    private final String[] bindingStrings;

    /**
     * Constructs an instance of {@link BindingSetRow}.
     *
     * @param nodeId - The Node ID of a query node. (not null)
     * @param bindingStrings - A Binding Set that is part of the node's results. (not null)
     */
    public BindingSetRow(final String nodeId, final String[] bindingStrings) {
        this.nodeId = checkNotNull(nodeId);
        this.bindingStrings = checkNotNull(bindingStrings);
    }

    /**
     * @return The Node ID of a query node.
     */
    public String getNodeId() {
        return nodeId;
    }

    /**
     * @return A Binding Set that is part of the node's results. It is formatted
     *   in SPO order and each String requires further interpretation.
     */
    public String[] getBindingStrings() {
        return bindingStrings;
    }

    /**
     * Parses the {@link Bytes} of an Accumulo Row ID into a {@link BindingSetRow}.
     *
     * @param row - The Row ID to parse. (not null).
     * @return A {@link BindingSetRow} holding the parsed values.
     */
    public static BindingSetRow make(final Bytes row) {
        checkNotNull(row);

        // Read the Node ID from the row's bytes.
        final String[] rowArray = row.toString().split(NODEID_BS_DELIM);
        if(rowArray.length != 2) {
            throw new IllegalArgumentException("A row must contain a single NODEID_BS_DELIM.");
        }
        final String nodeId = rowArray[0];

        // Read the row's Binding Set from the bytes.
        final String[] bindingStrings = rowArray[1].split(DELIM);

        return new BindingSetRow(nodeId, bindingStrings);
    }
}