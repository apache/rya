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
import java.util.Set;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryMetadataDAO;
import org.apache.rya.indexing.pcj.fluo.app.query.JoinMetadata;
import org.apache.rya.indexing.pcj.storage.accumulo.BindingSetConverter.BindingSetConversionException;
import org.apache.rya.indexing.pcj.storage.accumulo.BindingSetStringConverter;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSetStringConverter;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.impl.MapBindingSet;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.client.scanner.ColumnScanner;
import org.apache.fluo.api.client.scanner.RowScanner;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.ColumnValue;
import org.apache.fluo.api.data.Span;

/**
 * Updates the results of a Join node when one of its children has added a
 * new Binding Set to its results.
 */
@ParametersAreNonnullByDefault
public class JoinResultUpdater {

    private static final BindingSetStringConverter idConverter = new BindingSetStringConverter();
    private static final VisibilityBindingSetStringConverter valueConverter = new VisibilityBindingSetStringConverter();

    private final FluoQueryMetadataDAO queryDao = new FluoQueryMetadataDAO();
   
    /**
     * Updates the results of a Join node when one of its children has added a
     * new Binding Set to its results.
     *
     * @param tx - The transaction all Fluo queries will use. (not null)
     * @param childId - The Node ID of the child whose results received a new Binding Set. (not null)
     * @param childBindingSet - The Binding Set that was just emitted by child node. (not null)
     * @param joinMetadata - The metadata for the Join that has been notified. (not null)
     * @throws BindingSetConversionException
     */
    public void updateJoinResults(
            final TransactionBase tx,
            final String childId,
            final VisibilityBindingSet childBindingSet,
            final JoinMetadata joinMetadata) throws BindingSetConversionException {
        checkNotNull(tx);
        checkNotNull(childId);
        checkNotNull(childBindingSet);
        checkNotNull(joinMetadata);

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

        if(childId.equals(joinMetadata.getLeftChildNodeId())) {
            emittingSide = Side.LEFT;
            siblingId = joinMetadata.getRightChildNodeId();
        } else {
            emittingSide = Side.RIGHT;
            siblingId = joinMetadata.getLeftChildNodeId();
        }

        // Iterates over the sibling node's BindingSets that join with the new binding set.
        final FluoTableIterator siblingBindingSets = makeSiblingScanIterator(childId, childBindingSet, siblingId, tx);

        // Iterates over the resulting BindingSets from the join.
        final Iterator<VisibilityBindingSet> newJoinResults;
        if(emittingSide == Side.LEFT) {
            newJoinResults = joinAlgorithm.newLeftResult(childBindingSet, siblingBindingSets);
        } else {
            newJoinResults = joinAlgorithm.newRightResult(siblingBindingSets, childBindingSet);
        }

        // Insert the new join binding sets to the Fluo table.
        final VariableOrder joinVarOrder = joinMetadata.getVariableOrder();
        while(newJoinResults.hasNext()) {
            final BindingSet newJoinResult = newJoinResults.next();
            final String joinBindingSetStringId = idConverter.convert(newJoinResult, joinVarOrder);
            final String joinBindingSetStringValue = valueConverter.convert(newJoinResult, joinVarOrder);

            final String row = joinMetadata.getNodeId() + NODEID_BS_DELIM + joinBindingSetStringId;
            final Column col = FluoQueryColumns.JOIN_BINDING_SET;
            final String value = joinBindingSetStringValue;
            tx.set(row, col, value);
        }
    }

    /**
     * The different sides a new binding set may appear on.
     */
    public static enum Side {
        LEFT, RIGHT;
    }

    private FluoTableIterator makeSiblingScanIterator(final String childId, final BindingSet childBindingSet, final String siblingId, final TransactionBase tx) throws BindingSetConversionException {
        // Get the common variable orders. These are used to build the prefix.
        final VariableOrder childVarOrder = getVarOrder(tx, childId);
        final VariableOrder siblingVarOrder = getVarOrder(tx, siblingId);
        final List<String> commonVars = getCommonVars(childVarOrder, siblingVarOrder);

        // Get the Binding strings
        final String childBindingSetString = valueConverter.convert(childBindingSet, childVarOrder);
        final String[] childBindingStrings = FluoStringConverter.toBindingStrings(childBindingSetString);

        // Create the prefix that will be used to scan for binding sets of the sibling node.
        // This prefix includes the sibling Node ID and the common variable values from
        // childBindingSet.
        String siblingScanPrefix = "";
        for(int i = 0; i < commonVars.size(); i++) {
            if(siblingScanPrefix.length() == 0) {
                siblingScanPrefix = childBindingStrings[i];
            } else {
                siblingScanPrefix += DELIM + childBindingStrings[i];
            }
        }
        siblingScanPrefix = siblingId + NODEID_BS_DELIM + siblingScanPrefix;

        // Scan the sibling node's binding sets for those that have the same
        // common variable values as childBindingSet. These needs to be joined
        // and inserted into the Join's results. It's possible that none of these
        // results will be new Join results if they have already been created in
        // earlier iterations of this algorithm.

        final RowScanner rs = tx.scanner().over(Span.prefix(siblingScanPrefix)).fetch(getScanColumnFamily(siblingId)).byRow().build();
        return new FluoTableIterator(rs, siblingVarOrder);
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
            default:
                throw new IllegalArgumentException("The child node's sibling is not of type StatementPattern, Join, Left Join, or Filter.");
        }
        
        return column;
    }

    /**
     * Defines each of the cases that may generate new join results when
     * iteratively computing a query's join node.
     */
    public static interface IterativeJoin {

        /**
         * Invoked when a new {@link VisibilityBindingSet} is emitted from the left child
         * node of the join. The Fluo table is scanned for results on the right
         * side that will be joined with the new result.
         *
         * @param newLeftResult - A new VisibilityBindingSet that has been emitted from
         *   the left child node.
         * @param rightResults - The right child node's binding sets that will
         *   be joined with the new left result. (not null)
         * @return The new BindingSet results for the join.
         */
        public Iterator<VisibilityBindingSet> newLeftResult(VisibilityBindingSet newLeftResult, Iterator<VisibilityBindingSet> rightResults);

        /**
         * Invoked when a new {@link VisibilityBindingSet} is emitted from the right child
         * node of the join. The Fluo table is scanned for results on the left
         * side that will be joined with the new result.
         *
         * @param leftResults - The left child node's binding sets that will be
         *   joined with the new right result.
         * @param newRightResult - A new BindingSet that has been emitted from
         *   the right child node.
         * @return The new BindingSet results for the join.
         */
        public Iterator<VisibilityBindingSet> newRightResult(Iterator<VisibilityBindingSet> leftResults, VisibilityBindingSet newRightResult);
    }

    /**
     * Implements an {@link IterativeJoin} that uses the Natural Join algorithm
     * defined by Relational Algebra.
     * <p>
     * This is how you combine {@code BindnigSet}s that may have common Binding
     * names. When two Binding Sets are joined, any bindings that appear in both
     * binding sets are only included once.
     */
    public static final class NaturalJoin implements IterativeJoin {
        @Override
        public Iterator<VisibilityBindingSet> newLeftResult(final VisibilityBindingSet newLeftResult, final Iterator<VisibilityBindingSet> rightResults) {
            checkNotNull(newLeftResult);
            checkNotNull(rightResults);

            // Both sides are required, so if there are no right results, then do not emit anything.
            return new LazyJoiningIterator(Side.LEFT, newLeftResult, rightResults);
        }

        @Override
        public Iterator<VisibilityBindingSet> newRightResult(final Iterator<VisibilityBindingSet> leftResults, final VisibilityBindingSet newRightResult) {
            checkNotNull(leftResults);
            checkNotNull(newRightResult);

            // Both sides are required, so if there are no left reuslts, then do not emit anything.
            return new LazyJoiningIterator(Side.RIGHT, newRightResult, leftResults);
        }
    }

    /**
     * Implements an {@link IterativeJoin} that uses the Left Outer Join
     * algorithm defined by Relational Algebra.
     * <p>
     * This is how you add optional information to a {@link BindingSet}. Left
     * binding sets are emitted even if they do not join with anything on the right.
     * However, right binding sets must be joined with a left binding set.
     */
    public static final class LeftOuterJoin implements IterativeJoin {
        @Override
        public Iterator<VisibilityBindingSet> newLeftResult(final VisibilityBindingSet newLeftResult, final Iterator<VisibilityBindingSet> rightResults) {
            checkNotNull(newLeftResult);
            checkNotNull(rightResults);

            // If the required portion does not join with any optional portions,
            // then emit a BindingSet that matches the new left result.
            if(!rightResults.hasNext()) {
                return Lists.<VisibilityBindingSet>newArrayList(newLeftResult).iterator();
            }

            // Otherwise, return an iterator that holds the new required result
            // joined with the right results.
            return new LazyJoiningIterator(Side.LEFT, newLeftResult, rightResults);
        }

        @Override
        public Iterator<VisibilityBindingSet> newRightResult(final Iterator<VisibilityBindingSet> leftResults, final VisibilityBindingSet newRightResult) {
            checkNotNull(leftResults);
            checkNotNull(newRightResult);

            // The right result is optional, so if it does not join with anything
            // on the left, then do not emit anything.
            return new LazyJoiningIterator(Side.RIGHT, newRightResult, leftResults);
        }
    }

    /**
     * Joins a {@link BindingSet} (which is new to the left or right side of a join)
     * to all binding sets on the other side that join with it.
     * <p>
     * This is done lazily so that you don't have to load all of the BindingSets
     * into memory at once.
     */
    private static final class LazyJoiningIterator implements Iterator<VisibilityBindingSet> {

        private final Side newResultSide;
        private final VisibilityBindingSet newResult;
        private final Iterator<VisibilityBindingSet> joinedResults;

        /**
         * Constructs an instance of {@link LazyJoiningIterator}.
         *
         * @param newResultSide - Indicates which side of the join the {@code newResult} arrived on. (not null)
         * @param newResult - A binding set that will be joined with some other binding sets. (not null)
         * @param joinedResults - The binding sets that will be joined with {@code newResult}. (not null)
         */
        public LazyJoiningIterator(final Side newResultSide, final VisibilityBindingSet newResult, final Iterator<VisibilityBindingSet> joinedResults) {
            this.newResultSide = checkNotNull(newResultSide);
            this.newResult = checkNotNull(newResult);
            this.joinedResults = checkNotNull(joinedResults);
        }

        @Override
        public boolean hasNext() {
            return joinedResults.hasNext();
        }

        @Override
        public VisibilityBindingSet next() {
            final MapBindingSet bs = new MapBindingSet();

            for(final Binding binding : newResult) {
                bs.addBinding(binding);
            }

            final VisibilityBindingSet joinResult = joinedResults.next();
            for(final Binding binding : joinResult) {
                bs.addBinding(binding);
            }

            // We want to make sure the visibilities are always written the same way,
            // so figure out which are on the left side and which are on the right side.
            final String leftVisi;
            final String rightVisi;
            if(newResultSide == Side.LEFT) {
                leftVisi = newResult.getVisibility();
                rightVisi = joinResult.getVisibility();
            } else {
                leftVisi = joinResult.getVisibility();
                rightVisi = newResult.getVisibility();
            }

            String visibility = "";
            final Joiner join = Joiner.on(")&(");
            if(leftVisi.isEmpty() || rightVisi.isEmpty()) {
                visibility = (leftVisi + rightVisi).trim();
            } else {
                visibility = "(" + join.join(leftVisi, rightVisi) + ")";
            }
            return new VisibilityBindingSet(bs, visibility);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove() is unsupported.");
        }
    }

    /**
     * Iterates over rows that have a Binding Set column and returns the unmarshalled
     * {@link BindingSet}s.
     */
    private static final class FluoTableIterator implements Iterator<VisibilityBindingSet> {

        private static final Set<Column> BINDING_SET_COLUMNS = Sets.newHashSet(
                FluoQueryColumns.STATEMENT_PATTERN_BINDING_SET,
                FluoQueryColumns.JOIN_BINDING_SET,
                FluoQueryColumns.FILTER_BINDING_SET);

        private final Iterator<ColumnScanner> rows;
        private final VariableOrder varOrder;

        /**
         * Constructs an instance of {@link FluoTableIterator}.
         *
         * @param rows - Iterates over RowId values in a Fluo Table. (not null)
         * @param varOrder - The Variable Order of binding sets that will be
         *   read from the Fluo Table. (not null)
         */
        public FluoTableIterator(final RowScanner rows, final VariableOrder varOrder) {
            this.rows = checkNotNull(rows).iterator();
            this.varOrder = checkNotNull(varOrder);
        }

        @Override
        public boolean hasNext() {
            return rows.hasNext();
        }

        @Override
        public VisibilityBindingSet next() {
            final ColumnScanner columns = rows.next();

            for (ColumnValue cv : columns) {
            	 if(BINDING_SET_COLUMNS.contains(cv.getColumn())) {
                     final String bindingSetString = cv.getsValue();
                     return (VisibilityBindingSet) valueConverter.convert(bindingSetString, varOrder);
                 }
			}

            throw new RuntimeException("Row did not containing a Binding Set.");
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove() is unsupported.");
        }
    }
}