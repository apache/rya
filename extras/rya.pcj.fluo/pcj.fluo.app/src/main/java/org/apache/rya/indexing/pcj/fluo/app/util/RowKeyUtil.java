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
package org.apache.rya.indexing.pcj.fluo.app.util;

import static java.util.Objects.requireNonNull;

import org.apache.fluo.api.data.Bytes;
import org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants;
import org.apache.rya.indexing.pcj.storage.accumulo.BindingSetStringConverter;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.eclipse.rdf4j.query.BindingSet;

import com.google.common.base.Charsets;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * TODO doc that this implements utility functions used to create the Fluo Row Keys used when referencing the binding
 * set results of a query node.
 */
@DefaultAnnotation(NonNull.class)
public class RowKeyUtil {

    private static final BindingSetStringConverter BS_CONVERTER = new BindingSetStringConverter();

    /**
     * Creates the Row Key that will be used by a node within the PCJ Fluo application to represent where a specific
     * result of that node will be placed.
     *
     * @param nodeId - Identifies the Node that the Row Key is for. (not null)
     * @param variableOrder - Specifies which bindings from {@code bindingSet} will be included within the Row Key as
     *   well as the order they will appear. (not null)
     * @param bindingSet - The Binding Set whose values will be used to create the Row Key. (not null)
     * @return A Row Key built using the provided values.
     */
    public static Bytes makeRowKey(final String nodeId, final VariableOrder variableOrder, final BindingSet bindingSet) {
        requireNonNull(nodeId);
        requireNonNull(variableOrder);
        requireNonNull(bindingSet);

        // The Row Key starts with the Node ID of the node the result belongs to.
        String rowId = nodeId + IncrementalUpdateConstants.NODEID_BS_DELIM;

        // Append the String formatted bindings that are included in the Variable Order. The Variable Order also defines
        // the order the binding will be written to the Row Key. If a Binding is missing for one of the Binding Names
        // that appears within the Variable Order, then an empty value will be written for that location within the Row Key.
        rowId += BS_CONVERTER.convert(bindingSet, variableOrder);

        // Format the Row Key as a UTF 8 encoded Bytes object.
        return Bytes.of( rowId.getBytes(Charsets.UTF_8) );
    }
}