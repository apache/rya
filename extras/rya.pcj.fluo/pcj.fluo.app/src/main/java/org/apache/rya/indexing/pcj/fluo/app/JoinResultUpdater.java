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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.client.scanner.ColumnScanner;
import org.apache.fluo.api.client.scanner.RowScanner;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.ColumnValue;
import org.apache.fluo.api.data.RowColumn;
import org.apache.fluo.api.data.Span;
import org.apache.log4j.Logger;
import org.apache.rya.accumulo.utils.VisibilitySimplifier;
import org.apache.rya.indexing.pcj.fluo.app.batch.AbstractBatchBindingSetUpdater;
import org.apache.rya.indexing.pcj.fluo.app.batch.BatchInformation.Task;
import org.apache.rya.indexing.pcj.fluo.app.batch.BatchInformationDAO;
import org.apache.rya.indexing.pcj.fluo.app.batch.JoinBatchInformation;
import org.apache.rya.indexing.pcj.fluo.app.query.CommonNodeMetadataImpl;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryMetadataDAO;
import org.apache.rya.indexing.pcj.fluo.app.query.JoinMetadata;
import org.apache.rya.indexing.pcj.fluo.app.util.AggregationStateManager;
import org.apache.rya.indexing.pcj.fluo.app.util.RowKeyUtil;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSetSerDe;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSetStringConverter;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.impl.MapBindingSet;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Updates the results of a Join node when one of its children has added a new Binding Set to its results.
 */
@DefaultAnnotation(NonNull.class)
public class JoinResultUpdater {

    private static final Logger log = Logger.getLogger(JoinResultUpdater.class);

    private static final VisibilityBindingSetSerDe BS_SERDE = new VisibilityBindingSetSerDe();
    private static final VisibilityBindingSetStringConverter VIS_BS_CONVERTER = new VisibilityBindingSetStringConverter();

    private final FluoQueryMetadataDAO queryDao = new FluoQueryMetadataDAO();

    /**
     * Updates the results of a Join node when one of its children has added a new Binding Set to its results.
     *
     * @param tx - The transaction all Fluo queries will use. (not null)
     * @param childNodeId - The Node ID of the child whose results received a new Binding Set. (not null)
     * @param childBindingSet - The Binding Set that was just emitted by child node. (not null)
     * @param joinMetadata - The metadata for the Join that has been notified. (not null)
     * @throws Exception The update could not be successfully performed.
     */
    public void updateJoinResults(final TransactionBase tx, final String childNodeId, final VisibilityBindingSet childBindingSet,
            final JoinMetadata joinMetadata) throws Exception {
        checkNotNull(tx);
        checkNotNull(childNodeId);
        checkNotNull(childBindingSet);
        checkNotNull(joinMetadata);

        Optional<CommonNodeMetadataImpl> stateMeta = Optional.fromNullable(joinMetadata.getStateMetadata().orElse(null));
        Optional<AggregationStateManager> stateManager = Optional.absent();
        if(stateMeta.isPresent()) {
            stateManager = Optional.of(new AggregationStateManager(tx, stateMeta.get()));
        }
        
        if (!stateManager.isPresent() || stateManager.get().checkAggregationState(childBindingSet)) {
            log.trace("Transaction ID: " + tx.getStartTimestamp() + "\n" + "Join Node ID: " + joinMetadata.getNodeId() + "\n"
                    + "Child Node ID: " + childNodeId + "\n" + "Child Binding Set:\n" + childBindingSet + "\n");

            // Figure out which join algorithm we are going to use.
            final IterativeJoin joinAlgorithm;
            switch (joinMetadata.getJoinType()) {
            case NATURAL_JOIN:
                joinAlgorithm = new NaturalJoin();
                break;
            case LEFT_OUTER_JOIN:
                joinAlgorithm = new LeftOuterJoin();
                break;
            default:
                throw new RuntimeException("Unsupported JoinType: " + joinMetadata.getJoinType());
            }

            // Figure out which side of the join the new binding set appeared on.
            final Side emittingSide;
            final String siblingId;

            if (childNodeId.equals(joinMetadata.getLeftChildNodeId())) {
                emittingSide = Side.LEFT;
                siblingId = joinMetadata.getRightChildNodeId();
            } else {
                emittingSide = Side.RIGHT;
                siblingId = joinMetadata.getLeftChildNodeId();
            }

            // Iterates over the sibling node's BindingSets that join with the new binding set.
            Set<VisibilityBindingSet> siblingBindingSets = new HashSet<>();
            Span siblingSpan = getSpan(tx, childNodeId, childBindingSet, siblingId);
            Column siblingColumn = getScanColumnFamily(siblingId);
            Optional<RowColumn> rowColumn = fillSiblingBatch(tx, siblingSpan, siblingColumn, siblingBindingSets,
                    joinMetadata.getJoinBatchSize());

            // Iterates over the resulting BindingSets from the join.
            final Iterator<VisibilityBindingSet> newJoinResults;
            if (emittingSide == Side.LEFT) {
                newJoinResults = joinAlgorithm.newLeftResult(childBindingSet, siblingBindingSets.iterator(), stateManager);
            } else {
                newJoinResults = joinAlgorithm.newRightResult(siblingBindingSets.iterator(), childBindingSet, stateManager);
            }

            // Insert the new join binding sets to the Fluo table.
            final VariableOrder joinVarOrder = joinMetadata.getVariableOrder();
            while (newJoinResults.hasNext()) {
                final VisibilityBindingSet newJoinResult = newJoinResults.next();

                // Create the Row Key for the emitted binding set. It does not contain visibilities.
                final Bytes resultRow = RowKeyUtil.makeRowKey(joinMetadata.getNodeId(), joinVarOrder, newJoinResult);

                // Only insert the join Binding Set if it is new or BindingSet contains values not used in resultRow.
                if (tx.get(resultRow, FluoQueryColumns.JOIN_BINDING_SET) == null
                        || joinVarOrder.getVariableOrders().size() < newJoinResult.size()) {
                    // Create the Node Value. It does contain visibilities.
                    final Bytes nodeValueBytes = BS_SERDE.serialize(newJoinResult);

                    log.trace("Transaction ID: " + tx.getStartTimestamp() + "\n" + "New Join Result:\n" + newJoinResult + "\n");

                    tx.set(resultRow, FluoQueryColumns.JOIN_BINDING_SET, nodeValueBytes);
                }
            }

            // if batch limit met, there are additional entries to process
            // update the span and register updated batch job
            if (rowColumn.isPresent()) {
                Span newSpan = AbstractBatchBindingSetUpdater.getNewSpan(rowColumn.get(), siblingSpan);
                JoinBatchInformation joinBatch = JoinBatchInformation.builder().setBatchSize(joinMetadata.getJoinBatchSize())
                        .setBs(childBindingSet).setColumn(siblingColumn).setJoinType(joinMetadata.getJoinType()).setSide(emittingSide)
                        .setSpan(newSpan).setTask(Task.Add).build();
                BatchInformationDAO.addBatch(tx, joinMetadata.getNodeId(), joinBatch);
            }
        }
    }

    /**
     * The different sides a new binding set may appear on.
     */
    public static enum Side {
        LEFT, RIGHT;
    }

    /**
     * Fetches batch to be processed by scanning over the Span specified by the {@link JoinBatchInformation}. The number
     * of results is less than or equal to the batch size specified by the JoinBatchInformation.
     * 
     * @param tx - Fluo transaction in which batch operation is performed
     * @param siblingSpan - span of sibling to retrieve elements to join with
     * @param bsSet- set that batch results are added to
     * @return Set - containing results of sibling scan.
     * @throws Exception
     */
    private Optional<RowColumn> fillSiblingBatch(TransactionBase tx, Span siblingSpan, Column siblingColumn,
            Set<VisibilityBindingSet> bsSet, int batchSize) throws Exception {

        RowScanner rs = tx.scanner().over(siblingSpan).fetch(siblingColumn).byRow().build();
        Iterator<ColumnScanner> colScannerIter = rs.iterator();

        boolean batchLimitMet = false;
        Bytes row = siblingSpan.getStart().getRow();
        while (colScannerIter.hasNext() && !batchLimitMet) {
            ColumnScanner colScanner = colScannerIter.next();
            row = colScanner.getRow();
            Iterator<ColumnValue> iter = colScanner.iterator();
            while (iter.hasNext() && !batchLimitMet) {
                bsSet.add(BS_SERDE.deserialize(iter.next().getValue()));
                // check if batch size has been met and set flag if it has been met
                if (bsSet.size() >= batchSize) {
                    batchLimitMet = true;
                }
            }
        }

        if (batchLimitMet) {
            return Optional.of(new RowColumn(row, siblingColumn));
        } else {
            return Optional.absent();
        }
    }

    /**
     * Creates a Span for the sibling node to retrieve BindingSets to join with
     * 
     * @param tx
     * @param childId - Id of the node that was updated
     * @param childBindingSet - BindingSet update
     * @param siblingId - Id of the sibling node whose BindingSets will be retrieved and joined with the update
     * @return Span to retrieve sibling node's BindingSets to form join results
     */
    private Span getSpan(TransactionBase tx, final String childId, final BindingSet childBindingSet, final String siblingId) {
        // Get the common variable orders. These are used to build the prefix.
        final VariableOrder siblingVarOrder = getVarOrder(tx, siblingId);
        final List<String> commonVars = getCommonVars(childBindingSet, siblingVarOrder);

        String childBindingSetString = VIS_BS_CONVERTER.convert(childBindingSet, new VariableOrder(commonVars));
        //remove the visibility (which gets appended to the end of the string by VisibilityBindingSetConverter)
        String siblingScanPrefix = childBindingSetString.split("\u0001")[0];     
        
        siblingScanPrefix = siblingId + NODEID_BS_DELIM + siblingScanPrefix;

        return Span.prefix(siblingScanPrefix);
    }

    /**
     * Retrieves variables common to the indicated VariableOrder and the indicated BindingSet. This is necessary to
     * determine which values to extract from the BindingSet to form a scan of the results that will be joined with the
     * BindingSet.
     * 
     * @param bs - BindingSet whose values will be used to form scan
     * @param varOrder - VariableOrder of node whose results will be join with BindingSet
     * @return - List of common variables
     */
    private List<String> getCommonVars(BindingSet bs, VariableOrder varOrder) {
        List<String> vars = new ArrayList<>();

        for (String var : varOrder.getVariableOrders()) {
            if (bs.hasBinding(var)) {
                vars.add(var);
            } else {
                return vars;
            }
        }

        return vars;
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
        switch (nodeType) {
        case STATEMENT_PATTERN:
            return removeBinIdFromVarOrder(queryDao.readStatementPatternMetadata(tx, nodeId).getVariableOrder());
        case FILTER:
            return removeBinIdFromVarOrder(queryDao.readFilterMetadata(tx, nodeId).getVariableOrder());
        case JOIN:
            return removeBinIdFromVarOrder(queryDao.readJoinMetadata(tx, nodeId).getVariableOrder());
        case PROJECTION:
            return removeBinIdFromVarOrder(queryDao.readProjectionMetadata(tx, nodeId).getVariableOrder());
        default:
            throw new IllegalArgumentException("Could not figure out the variable order for node with ID: " + nodeId);
        }
    }

    private VariableOrder removeBinIdFromVarOrder(VariableOrder varOrder) {
        List<String> varOrderList = varOrder.getVariableOrders();
        if (varOrderList.get(0).equals(IncrementalUpdateConstants.PERIODIC_BIN_ID)) {
            List<String> updatedVarOrderList = Lists.newArrayList(varOrderList);
            updatedVarOrderList.remove(0);
            return new VariableOrder(updatedVarOrderList);
        } else {
            return varOrder;
        }
    }

    /**
     * Return the sibling node's binding set column to use for a scan. The column that will be used is determined by the
     * node's {@link NodeType}.
     *
     * @param siblingId - The Node ID of the sibling. (not null)
     */
    private static Column getScanColumnFamily(final String siblingId) {
        checkNotNull(siblingId);

        // Determine which type of binding set the sibling is.
        final Optional<NodeType> siblingType = NodeType.fromNodeId(siblingId);
        if (!siblingType.isPresent()) {
            throw new IllegalStateException("The child's sibling is not of a recognized type.");
        }

        // Set the column to join with.
        Column column;
        switch (siblingType.get()) {
        case STATEMENT_PATTERN:
            column = FluoQueryColumns.STATEMENT_PATTERN_BINDING_SET;
            break;
        case FILTER:
            column = FluoQueryColumns.FILTER_BINDING_SET;
            break;
        case JOIN:
            column = FluoQueryColumns.JOIN_BINDING_SET;
            break;
        case PROJECTION:
            column = FluoQueryColumns.PROJECTION_BINDING_SET;
            break;
        default:
            throw new IllegalArgumentException("The child node's sibling is not of type StatementPattern, Join, Left Join, or Filter.");
        }

        return column;
    }

    /**
     * Defines each of the cases that may generate new join results when iteratively computing a query's join node.
     */
    public static interface IterativeJoin {

        /**
         * Invoked when a new {@link VisibilityBindingSet} is emitted from the left child node of the join. The Fluo
         * table is scanned for results on the right side that will be joined with the new result.
         *
         * @param newLeftResult - A new VisibilityBindingSet that has been emitted from the left child node.
         * @param rightResults - The right child node's binding sets that will be joined with the new left result. (not
         *            null)
         * @param stateManager - manages lookups and verification of aggregation state
         * @return The new BindingSet results for the join.
         */
        public Iterator<VisibilityBindingSet> newLeftResult(VisibilityBindingSet newLeftResult,
                Iterator<VisibilityBindingSet> rightResults, Optional<AggregationStateManager> stateManager);

        /**
         * Invoked when a new {@link VisibilityBindingSet} is emitted from the right child node of the join. The Fluo
         * table is scanned for results on the left side that will be joined with the new result.
         *
         * @param leftResults - The left child node's binding sets that will be joined with the new right result.
         * @param newRightResult - A new BindingSet that has been emitted from the right child node.
         * @param stateManager - manages lookups and verification of aggregation state
         * @return The new BindingSet results for the join.
         */
        public Iterator<VisibilityBindingSet> newRightResult(Iterator<VisibilityBindingSet> leftResults,
                VisibilityBindingSet newRightResult, Optional<AggregationStateManager> stateManager);
    }

    /**
     * Implements an {@link IterativeJoin} that uses the Natural Join algorithm defined by Relational Algebra.
     * <p>
     * This is how you combine {@code BindnigSet}s that may have common Binding names. When two Binding Sets are joined,
     * any bindings that appear in both binding sets are only included once.
     */
    public static final class NaturalJoin implements IterativeJoin {
        @Override
        public Iterator<VisibilityBindingSet> newLeftResult(final VisibilityBindingSet newLeftResult,
                final Iterator<VisibilityBindingSet> rightResults, Optional<AggregationStateManager> stateManager) {
            checkNotNull(newLeftResult);
            checkNotNull(rightResults);

            // Both sides are required, so if there are no right results, then do not emit anything.
            return new LazyJoiningIterator(Side.LEFT, newLeftResult, rightResults, stateManager);
        }

        @Override
        public Iterator<VisibilityBindingSet> newRightResult(final Iterator<VisibilityBindingSet> leftResults,
                final VisibilityBindingSet newRightResult, Optional<AggregationStateManager> stateManager) {
            checkNotNull(leftResults);
            checkNotNull(newRightResult);

            // Both sides are required, so if there are no left reuslts, then do not emit anything.
            return new LazyJoiningIterator(Side.RIGHT, newRightResult, leftResults, stateManager);
        }
    }

    /**
     * Implements an {@link IterativeJoin} that uses the Left Outer Join algorithm defined by Relational Algebra.
     * <p>
     * This is how you add optional information to a {@link BindingSet}. Left binding sets are emitted even if they do
     * not join with anything on the right. However, right binding sets must be joined with a left binding set.
     */
    public static final class LeftOuterJoin implements IterativeJoin {
        @Override
        public Iterator<VisibilityBindingSet> newLeftResult(final VisibilityBindingSet newLeftResult,
                final Iterator<VisibilityBindingSet> rightResults, Optional<AggregationStateManager> stateManager) {
            checkNotNull(newLeftResult);
            checkNotNull(rightResults);

            // If the required portion does not join with any optional portions,
            // then emit a BindingSet that matches the new left result.
            if (!rightResults.hasNext()) {
                return Lists.<VisibilityBindingSet> newArrayList(newLeftResult).iterator();
            }

            // Otherwise, return an iterator that holds the new required result
            // joined with the right results.
            return new LazyJoiningIterator(Side.LEFT, newLeftResult, rightResults, stateManager);
        }

        @Override
        public Iterator<VisibilityBindingSet> newRightResult(final Iterator<VisibilityBindingSet> leftResults,
                final VisibilityBindingSet newRightResult, Optional<AggregationStateManager> stateManager) {
            checkNotNull(leftResults);
            checkNotNull(newRightResult);

            // The right result is optional, so if it does not join with anything
            // on the left, then do not emit anything.
            return new LazyJoiningIterator(Side.RIGHT, newRightResult, leftResults, stateManager);
        }
    }

    /**
     * Joins a {@link BindingSet} (which is new to the left or right side of a join) to all binding sets on the other
     * side that join with it.
     * <p>
     * This is done lazily so that you don't have to load all of the BindingSets into memory at once.
     */
    private static final class LazyJoiningIterator implements Iterator<VisibilityBindingSet> {

        private final Side newResultSide;
        private final VisibilityBindingSet newResult;
        private final Iterator<VisibilityBindingSet> joinedResults;
        private final Optional<AggregationStateManager> stateManager;
        private Optional<VisibilityBindingSet> next = null;

        /**
         * Constructs an instance of {@link LazyJoiningIterator}.
         *
         * @param newResultSide - Indicates which side of the join the {@code newResult} arrived on. (not null)
         * @param newResult - A binding set that will be joined with some other binding sets. (not null)
         * @param joinedResults - The binding sets that will be joined with {@code newResult}. (not null)
         * @param aggregationState - Optional BindingSet specifying aggregation state that all join candidates must
         *            satisfy
         */
        public LazyJoiningIterator(final Side newResultSide, final VisibilityBindingSet newResult,
                final Iterator<VisibilityBindingSet> joinedResults, Optional<AggregationStateManager> stateManager) {
            this.newResultSide = checkNotNull(newResultSide);
            this.newResult = checkNotNull(newResult);
            this.joinedResults = checkNotNull(joinedResults);
            this.stateManager = checkNotNull(stateManager);
        }

        @Override
        public boolean hasNext() {
            // next == null implies hasNext not called
            if (next == null) {
                next = Optional.absent();
                while (!next.isPresent() && joinedResults.hasNext()) {
                    VisibilityBindingSet candidate = joinedResults.next();
                    // check if candidate satisfies aggregation state (already checked that newResult satisfies
                    // aggregation state).
                    if (!stateManager.isPresent() || stateManager.get().checkAggregationState(candidate)) {
                        // Check that Bindings in join line up -- this is for when two BindingSets
                        // are joined by an Aggregation Result that isn't included in the VariableOrder.
                        if (isJoinValid(candidate, newResult)) {
                            next = Optional.of(candidate);
                        }
                    }
                }
            }

            return next.isPresent();
        }

        private boolean isJoinValid(VisibilityBindingSet bs1, VisibilityBindingSet bs2) {
            Set<String> commonVars = Sets.intersection(bs1.getBindingNames(), bs2.getBindingNames());
            for (String var : commonVars) {
                if (!bs1.getBinding(var).equals(bs2.getBinding(var))) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public VisibilityBindingSet next() {
            // check to see if BindingSet built by hasNext call
            if (next != null) {
                // if next not present, then there are no more results to iterate through
                if (next.isPresent()) {
                    VisibilityBindingSet bs = next.get();
                    // set next to null to indicate hasNext not called
                    next = null;
                    return createNewBindingSet(newResult, bs);
                } else {
                    throw new NoSuchElementException();
                }
            } else {
                // call hasNext to build BindingSet if not called
                hasNext();
                return next();
            }
        }

        private VisibilityBindingSet createNewBindingSet(VisibilityBindingSet newResult, VisibilityBindingSet next) {
            final MapBindingSet bs = new MapBindingSet();

            for (final Binding binding : newResult) {
                bs.addBinding(binding);
            }

            for (final Binding binding : next) {
                bs.addBinding(binding);
            }

            // We want to make sure the visibilities are always written the same way,
            // so figure out which are on the left side and which are on the right side.
            final String leftVisi;
            final String rightVisi;
            if (newResultSide == Side.LEFT) {
                leftVisi = newResult.getVisibility();
                rightVisi = next.getVisibility();
            } else {
                leftVisi = next.getVisibility();
                rightVisi = newResult.getVisibility();
            }
            final String visibility = VisibilitySimplifier.unionAndSimplify(leftVisi, rightVisi);

            return new VisibilityBindingSet(bs, visibility);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove() is unsupported.");
        }
    }

}