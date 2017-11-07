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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
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
import org.apache.rya.api.function.join.IterativeJoin;
import org.apache.rya.api.function.join.LazyJoiningIterator.Side;
import org.apache.rya.api.function.join.LeftOuterJoin;
import org.apache.rya.api.function.join.NaturalJoin;
import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.indexing.pcj.fluo.app.batch.AbstractBatchBindingSetUpdater;
import org.apache.rya.indexing.pcj.fluo.app.batch.BatchInformation.Task;
import org.apache.rya.indexing.pcj.fluo.app.batch.BatchInformationDAO;
import org.apache.rya.indexing.pcj.fluo.app.batch.JoinBatchInformation;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryMetadataCache;
import org.apache.rya.indexing.pcj.fluo.app.query.JoinMetadata;
import org.apache.rya.indexing.pcj.fluo.app.query.MetadataCacheSupplier;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSetSerDe;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Updates the results of a Join node when one of its children has added a
 * new Binding Set to its results.
 */
@DefaultAnnotation(NonNull.class)
public class JoinResultUpdater extends AbstractNodeUpdater {

    private static final Logger log = Logger.getLogger(JoinResultUpdater.class);
    private static final VisibilityBindingSetSerDe BS_SERDE = new VisibilityBindingSetSerDe();
    private final FluoQueryMetadataCache queryDao = MetadataCacheSupplier.getOrCreateCache();

    /**
     * Updates the results of a Join node when one of its children has added a
     * new Binding Set to its results.
     *
     * @param tx - The transaction all Fluo queries will use. (not null)
     * @param childNodeId - The Node ID of the child whose results received a new Binding Set. (not null)
     * @param childBindingSet - The Binding Set that was just emitted by child node. (not null)
     * @param joinMetadata - The metadata for the Join that has been notified. (not null)
     * @throws Exception The update could not be successfully performed.
     */
    public void updateJoinResults(
            final TransactionBase tx,
            final String childNodeId,
            final VisibilityBindingSet childBindingSet,
            final JoinMetadata joinMetadata) throws Exception {
        checkNotNull(tx);
        checkNotNull(childNodeId);
        checkNotNull(childBindingSet);
        checkNotNull(joinMetadata);

        log.trace(
                "Transaction ID: " + tx.getStartTimestamp() + "\n" +
                        "Join Node ID: " + joinMetadata.getNodeId() + "\n" +
                        "Child Node ID: " + childNodeId + "\n" +
                        "Child Binding Set:\n" + childBindingSet + "\n");

        // Figure out which join algorithm we are going to use.
        final IterativeJoin joinAlgorithm;
        switch(joinMetadata.getJoinType()) {
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

        if(childNodeId.equals(joinMetadata.getLeftChildNodeId())) {
            emittingSide = Side.LEFT;
            siblingId = joinMetadata.getRightChildNodeId();
        } else {
            emittingSide = Side.RIGHT;
            siblingId = joinMetadata.getLeftChildNodeId();
        }

        // Iterates over the sibling node's BindingSets that join with the new binding set.
        final Set<VisibilityBindingSet> siblingBindingSets = new HashSet<>();
        final Span siblingSpan = getSpan(tx, childNodeId, childBindingSet, siblingId);
        final Column siblingColumn = getScanColumnFamily(siblingId);
        final Optional<RowColumn> rowColumn = fillSiblingBatch(tx, siblingSpan, siblingColumn, siblingBindingSets, joinMetadata.getJoinBatchSize());

        // Iterates over the resulting BindingSets from the join.
        final Iterator<VisibilityBindingSet> newJoinResults;
        if(emittingSide == Side.LEFT) {
            newJoinResults = joinAlgorithm.newLeftResult(childBindingSet, siblingBindingSets.iterator());
        } else {
            newJoinResults = joinAlgorithm.newRightResult(siblingBindingSets.iterator(), childBindingSet);
        }

        // Insert the new join binding sets to the Fluo table.
        final VariableOrder joinVarOrder = joinMetadata.getVariableOrder();
        while(newJoinResults.hasNext()) {
            final VisibilityBindingSet newJoinResult = newJoinResults.next();

            // Create the Row Key for the emitted binding set. It does not contain visibilities.
            final Bytes resultRow = makeRowKey(joinMetadata.getNodeId(), joinVarOrder, newJoinResult);

            // Only insert the join Binding Set if it is new or BindingSet contains values not used in resultRow.
            if(tx.get(resultRow, FluoQueryColumns.JOIN_BINDING_SET) == null || joinVarOrder.getVariableOrders().size() < newJoinResult.size()) {
                // Create the Node Value. It does contain visibilities.
                final Bytes nodeValueBytes = BS_SERDE.serialize(newJoinResult);

                log.trace(
                        "Transaction ID: " + tx.getStartTimestamp() + "\n" +
                                "New Join Result:\n" + newJoinResult + "\n");

                tx.set(resultRow, FluoQueryColumns.JOIN_BINDING_SET, nodeValueBytes);
            }
        }

        // if batch limit met, there are additional entries to process
        // update the span and register updated batch job
        if (rowColumn.isPresent()) {
            final Span newSpan = AbstractBatchBindingSetUpdater.getNewSpan(rowColumn.get(), siblingSpan);
            final JoinBatchInformation joinBatch = JoinBatchInformation.builder()
                    .setBatchSize(joinMetadata.getJoinBatchSize())
                    .setBs(childBindingSet)
                    .setColumn(siblingColumn)
                    .setJoinType(joinMetadata.getJoinType())
                    .setSide(emittingSide)
                    .setSpan(newSpan)
                    .setTask(Task.Add)
                    .build();
            BatchInformationDAO.addBatch(tx, joinMetadata.getNodeId(), joinBatch);
        }
    }

    /**
     * Fetches batch to be processed by scanning over the Span specified by the
     * {@link JoinBatchInformation}. The number of results is less than or equal
     * to the batch size specified by the JoinBatchInformation.
     *
     * @param tx - Fluo transaction in which batch operation is performed
     * @param siblingSpan - span of sibling to retrieve elements to join with
     * @param bsSet- set that batch results are added to
     * @return Set - containing results of sibling scan.
     * @throws Exception
     */
    private Optional<RowColumn> fillSiblingBatch(final TransactionBase tx, final Span siblingSpan, final Column siblingColumn, final Set<VisibilityBindingSet> bsSet, final int batchSize) throws Exception {

        final RowScanner rs = tx.scanner().over(siblingSpan).fetch(siblingColumn).byRow().build();
        final Iterator<ColumnScanner> colScannerIter = rs.iterator();

        boolean batchLimitMet = false;
        Bytes row = siblingSpan.getStart().getRow();
        while (colScannerIter.hasNext() && !batchLimitMet) {
            final ColumnScanner colScanner = colScannerIter.next();
            row = colScanner.getRow();
            final Iterator<ColumnValue> iter = colScanner.iterator();
            while (iter.hasNext() && !batchLimitMet) {
                bsSet.add(BS_SERDE.deserialize(iter.next().getValue()));
                //check if batch size has been met and set flag if it has been met
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
     * @param tx
     * @param childId - Id of the node that was updated
     * @param childBindingSet - BindingSet update
     * @param siblingId - Id of the sibling node whose BindingSets will be retrieved and joined with the update
     * @return Span to retrieve sibling node's BindingSets to form join results
     */
    private Span getSpan(TransactionBase tx, final String childId, final VisibilityBindingSet childBindingSet, final String siblingId) {
        // Get the common variable orders. These are used to build the prefix.
        final VariableOrder childVarOrder = getVarOrder(tx, childId);
        final VariableOrder siblingVarOrder = getVarOrder(tx, siblingId);
        final List<String> commonVars = getCommonVars(childVarOrder, siblingVarOrder);

        Bytes siblingScanPrefix = null;
        if(!commonVars.isEmpty()) {
            siblingScanPrefix = makeRowKey(siblingId, new VariableOrder(commonVars), childBindingSet);
        } else {
            siblingScanPrefix = makeRowKey(siblingId, siblingVarOrder, childBindingSet);
        }

        return Span.prefix(siblingScanPrefix);
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
        if(varOrderList.get(0).equals(IncrementalUpdateConstants.PERIODIC_BIN_ID)) {
            final List<String> updatedVarOrderList = Lists.newArrayList(varOrderList);
            updatedVarOrderList.remove(0);
            return new VariableOrder(updatedVarOrderList);
        } else {
            return varOrder;
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

        // Only need to iterate through the shorted order's length.
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

    /**
     * Return the sibling node's binding set column to use for a scan. The column
     * that will be used is determined by the node's {@link NodeType}.
     *
     * @param siblingId - The Node ID of the sibling. (not null)
     */
    private static Column getScanColumnFamily(final String siblingId) {
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
            case PROJECTION:
                column = FluoQueryColumns.PROJECTION_BINDING_SET;
                break;
            default:
                throw new IllegalArgumentException("The child node's sibling is not of type StatementPattern, Join, Left Join, or Filter.");
        }

        return column;
    }
}