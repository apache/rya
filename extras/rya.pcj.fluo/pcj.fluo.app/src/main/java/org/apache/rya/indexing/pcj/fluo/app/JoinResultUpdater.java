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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.log4j.Logger;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryMetadataDAO;
import org.apache.rya.indexing.pcj.fluo.app.query.JoinMetadata;
import org.openrdf.query.BindingSet;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;

import io.fluo.api.client.TransactionBase;
import io.fluo.api.config.ScannerConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.data.Span;
import io.fluo.api.iterator.ColumnIterator;
import io.fluo.api.iterator.RowIterator;
import io.fluo.api.types.Encoder;
import io.fluo.api.types.StringEncoder;
import mvm.rya.indexing.external.tupleSet.PcjTables.VariableOrder;

/**
 * Updates the results of a Join node when one of its children has added a
 * new Binding Set to its results.
 */
@ParametersAreNonnullByDefault
public class JoinResultUpdater {
    private static final Logger log = Logger.getLogger(JoinResultUpdater.class);

    private final FluoQueryMetadataDAO queryDao = new FluoQueryMetadataDAO();
    private final Encoder encoder = new StringEncoder();

    /**
     * Updates the results of a Join node when one of its children has added a
     * new Binding Set to its results.
     *
     * @param tx - The transaction all Fluo queries will use. (not null)
     * @param childId - The Node ID of the child whose results received a new Binding Set. (not null)
     * @param childBindingSet - The Binding Set that was just emitted by child node. (not null)
     * @param joinMetadata - The metadatat for the Join that has been notified. (not null)
     */
    public void updateJoinResults(
            final TransactionBase tx,
            final String childId,
            final BindingSet childBindingSet,
            final JoinMetadata joinMetadata) {
        checkNotNull(tx);
        checkNotNull(childId);
        checkNotNull(joinMetadata);

        // Read the Join metadata from the Fluo table.
        final String[] joinVarOrder = joinMetadata.getVariableOrder().toArray();

        // Transform the Child binding set and varaible order values to be easier to work with.
        final VariableOrder childVarOrder = getVarOrder(tx, childId);
        final String[] childVarOrderArray = childVarOrder.toArray();
        final String childBindingSetString = FluoStringConverter.toBindingSetString(childBindingSet, childVarOrder.toArray());
        final String[] childBindingStrings = FluoStringConverter.toBindingStrings(childBindingSetString);

        // Transform the Sibling binding set and varaible order values to be easier to work with.
        final String leftChildId = joinMetadata.getLeftChildNodeId();
        final String rightChildId = joinMetadata.getRightChildNodeId();
        final String siblingId = leftChildId.equals(childId) ? rightChildId : leftChildId;

        final VariableOrder siblingVarOrder = getVarOrder(tx, siblingId);
        final String[] siblingVarOrderArray = siblingVarOrder.toArray();

        // Create a map that will be used later in this algorithm to create new Join result
        // Binding Sets. It is initialized with all of the values that are in childBindingSet.
        // The common values and any that are added on by the sibling will be overwritten
        // for each sibling scan result.
        final List<String> commonVars = getCommonVars(childVarOrder, siblingVarOrder);
        final Map<String, String> joinBindingSet = Maps.newHashMap();
        for(int i = 0; i < childVarOrderArray.length; i++) {
            joinBindingSet.put(childVarOrderArray[i], childBindingStrings[i]);
        }

        // Create the prefix that will be used to scan for binding sets of the sibling node.
        // This prefix includes the sibling Node ID and the common variable values from
        // childBindingSet.
        String bsPrefix = "";
        for(int i = 0; i < commonVars.size(); i++) {
            if(bsPrefix.length() == 0) {
                bsPrefix = childBindingStrings[i];
            } else {
                bsPrefix += DELIM + childBindingStrings[i];
            }
        }
        bsPrefix = siblingId + NODEID_BS_DELIM + bsPrefix;

        // Scan the sibling node's binding sets for those that have the same
        // common variable values as childBindingSet. These needs to be joined
        // and inserted into the Join's results. It's possible that none of these
        // results will be new Join results if they have already been created in
        // earlier iterations of this algorithm.
        final ScannerConfiguration sc1 = new ScannerConfiguration();
        sc1.setSpan(Span.prefix(bsPrefix));
        setScanColumnFamily(sc1, siblingId);

        try {
            final RowIterator ri = tx.get(sc1);
            while(ri.hasNext()) {
                final ColumnIterator ci = ri.next().getValue();
                while(ci.hasNext()){
                    // Get a sibling binding set.
                    final String siblingBindingSetString = ci.next().getValue().toString();
                    final String[] siblingBindingStrings = FluoStringConverter.toBindingStrings(siblingBindingSetString);

                    // Overwrite the previous sibling's values to create a new join binding set.
                    for (int i = 0; i < siblingBindingStrings.length; i++) {
                        joinBindingSet.put(siblingVarOrderArray[i], siblingBindingStrings[i]);
                    }
                    final String joinBindingSetString = makeBindingSetString(joinVarOrder, joinBindingSet);

                    // Write the join binding set to Fluo.
                    final Bytes row = encoder.encode(joinMetadata.getNodeId() + NODEID_BS_DELIM + joinBindingSetString);
                    final Column col = FluoQueryColumns.JOIN_BINDING_SET;
                    final Bytes value = encoder.encode(joinBindingSetString);
                    tx.set(row, col, value);
                }
            }
        } catch (final Exception e) {
            log.error("Error while scanning sibling binding sets to create new join results.", e);
        }
    }

    /**
     * Fetch the {@link VariableOrder} of a query node.
     *
     * @param tx - The transaction that will be used to read the variable order. (not null)
     * @param nodeId - The ID of the node to fetch. (not null)
     * @return The {@link VariableOrder} of the node.
     */
    private VariableOrder getVarOrder(final TransactionBase tx, final String nodeId) {
        checkNotNull(tx);
        checkNotNull(nodeId);

        final NodeType nodeType = NodeType.fromNodeId(nodeId).get();
        switch(nodeType) {
            case STATEMENT_PATTERN:
                return queryDao.readStatementPatternMetadata(tx, nodeId).getVariableOrder();

            case FILTER:
                return queryDao.readFilterMetadata(tx, nodeId).getVariableOrder();

            case JOIN:
                return queryDao.readJoinMetadata(tx, nodeId).getVariableOrder();

            default:
                throw new IllegalArgumentException("Could not figure out the variable order for node with ID: " + nodeId);
        }
    }

    /**
     * Assuming that the common variables between two children are already
     * shifted to the left, find the common variables between them.
     * <p>
     * Refer to {@link FluoQueryInitializer} to see why this assumption is being made.
     *
     * @param vars1 - The first child's variable order. (not null)
     * @param vars2 - The second child's variable order. (not null)
     * @return An ordered List of the common variables between the two children.
     */
    public List<String> getCommonVars(final VariableOrder vars1, final VariableOrder vars2) {
        checkNotNull(vars1);
        checkNotNull(vars2);

        final List<String> commonVars = new ArrayList<>();

        // Only need to iteratre through the shorted order's length.
        final Iterator<String> vars1It = vars1.iterator();
        final Iterator<String> vars2It = vars2.iterator();
        while(vars1It.hasNext() && vars2It.hasNext()) {
            final String var1 = vars1It.next();
            final String var2 = vars2It.next();

            if(var1.equals(var2)) {
                commonVars.add(var1);
            } else {
                // Because the common variables are left shifted, we can break once
                // we encounter a pair that does not match.
                break;
            }
        }

        return commonVars;
    }

//    /**
//     * Assuming that the common variables between two children are already
//     * shifted to the left, find the common variables between them.
//     * <p>
//     * Refer to {@link FluoQueryInitializer} to see why this assumption is being made.
//     *
//     * @param vars1 - The first child's variable order. (not null)
//     * @param vars2 - The second child's variable order. (not null)
//     * @return An ordered List of the common variables between the two children.
//     */
//    private List<String> getCommonVars(final String[] vars1, final String[] vars2) {
//        checkNotNull(vars1);
//        checkNotNull(vars2);
//
//        final List<String> commonVars = new ArrayList<>();
//
//        // Only need to iteratre through the shorted order's length.
//        final int shortestLen = Math.min(vars1.length, vars2.length);
//        for(int i = 0; i < shortestLen; i++) {
//            final String var1 = vars1[i];
//            final String var2 = vars2[i];
//
//            if(var1.equals(var2)) {
//                commonVars.add(var1);
//            } else {
//                // Because the common variables are left shifted, we can break once
//                // we encounter a pair that does not match.
//                break;
//            }
//        }
//
//        return commonVars;
//    }

    /**
     * Update a {@link ScannerConfiguration} to use the sibling node's binding
     * set column for its scan. The column that will be used is determined by the
     * node's {@link NodeType}.
     *
     * @param sc - The scan configuration that will be updated. (not null)
     * @param siblingId - The Node ID of the sibling. (not null)
     */
    private static void setScanColumnFamily(final ScannerConfiguration sc, final String siblingId) {
        checkNotNull(sc);
        checkNotNull(siblingId);

        // Determine which type of binding set the sibling is.
        final Optional<NodeType> siblingType = NodeType.fromNodeId(siblingId);
        if(!siblingType.isPresent()) {
            throw new IllegalStateException("The child's sibling is not of a recognized type.");
        }

        // Set the column to join with.
        Column column;
        switch(siblingType.get()) {
            case STATEMENT_PATTERN:
                column = FluoQueryColumns.STATEMENT_PATTERN_BINDING_SET;
                break;
            case FILTER:
                column = FluoQueryColumns.FILTER_BINDING_SET;
                break;
            case JOIN:
                column = FluoQueryColumns.JOIN_BINDING_SET;
                break;
            default:
                throw new IllegalArgumentException("The child node's sibling is not of type StatementPattern, Join, or Filter.");
        }
        sc.fetchColumn(column.getFamily(), column.getQualifier());
    }

    /**
     * Create a Binding Set String from a variable order and a map of bindings.
     *
     * @param varOrder - The resulting binding set's variable order. (not null)
     * @param bindingSetValues - A map holding the variables and their values that will be
     *   included in the resulting binding set.
     * @return A binding set string build from the map using the prescribed variable order.
     */
    private static String makeBindingSetString(final String[] varOrder, final Map<String, String> bindingSetValues) {
        checkNotNull(varOrder);
        checkNotNull(bindingSetValues);

        String bindingSetString = "";

        for (final String joinVar : varOrder) {
            if (bindingSetString.length() == 0) {
                bindingSetString = bindingSetValues.get(joinVar);
            } else {
                bindingSetString = bindingSetString + DELIM + bindingSetValues.get(joinVar);
            }
        }

        return bindingSetString;
    }
}

