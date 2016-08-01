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
package org.apache.rya.export.accumulo;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Date;

import org.apache.log4j.Logger;
import org.apache.rya.export.accumulo.util.TimeUtils;
import org.apache.rya.export.api.ParentMetadataRepository.MergeParentMetadata;
import org.apache.rya.export.api.RyaStatementStore;
import org.apache.rya.export.api.StatementManager;

import com.google.common.base.Optional;

import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.resolver.RyaTripleContext;

/**
 * Defines how 2 {@link RyaStatement}s will merge.
 */
public class AccumuloStatementManager implements StatementManager {
    private static final Logger log = Logger.getLogger(AccumuloStatementManager.class);

    private AccumuloRyaStatementStore parentAccumuloRyaStatementStore;
    private AccumuloParentMetadataRepository accumuloParentMetadataRepository;
    private MergeParentMetadata mergeParentMetadata;
    private RyaTripleContext parentRyaTripleContext;
    private RyaTripleContext childRyaTripleContext;

    /**
     * Creates a new instance of {@link AccumuloStatementManager}.
     * @param parentAccumuloRyaStatementStore the parent
     * {@link AccumuloRyaStatementStore}. (not {@code null})
     * @param parentAccumuloRyaStatementStore the child
     * {@link RyaStatementStore}. (not {@code null})
     * @param accumuloParentMetadataRepository the
     * {@link AccumuloParentMetadataRepository}. (not {@code null})
     */
    public AccumuloStatementManager(AccumuloRyaStatementStore parentAccumuloRyaStatementStore, RyaStatementStore childRyaStatementStore,  AccumuloParentMetadataRepository accumuloParentMetadataRepository) {
        this.parentAccumuloRyaStatementStore = checkNotNull(parentAccumuloRyaStatementStore);
        this.accumuloParentMetadataRepository = checkNotNull(accumuloParentMetadataRepository);
        mergeParentMetadata = accumuloParentMetadataRepository.get();
        parentRyaTripleContext = parentAccumuloRyaStatementStore.getRyaTripleContext();
        childRyaTripleContext = checkNotNull(childRyaStatementStore).getRyaTripleContext();
    }

    /**
     * The result of comparing a child key and parent key which determines what should be done with them.
     */
    protected static enum CompareKeysResult {
        /**
         * Indicates that the child iterator should move to the next key in the child
         * table in order to be compared to the current key in the parent table.
         */
        ADVANCE_CHILD,
        /**
         * Indicates that the child iterator should move to the next key in the child
         * table in order to be compared to the current key in the parent table
         * and that the current child key should be added to the parent.
         */
        ADVANCE_CHILD_AND_ADD,
        /**
         * Indicates that the parent iterator should move to the next key in the parent table
         * in order to be compared to the current key in the child table.
         */
        ADVANCE_PARENT,
        /**
         * Indicates that the parent iterator should move to the next key in the parent table
         * in order to be compared to the current key in the child table
         * and that the current parent key should be deleted from the parent.
         */
        ADVANCE_PARENT_AND_DELETE,
        /**
         * Indicates that the child iterator should move to the next key in the child table
         * and the parent iterator should move to the next key in the parent table.
         */
        ADVANCE_BOTH,
        /**
         * Indicates that there are no more keys to compare in the child and parent tables.
         */
        FINISHED;
    }

    @Override
    public Optional<RyaStatement> merge(final Optional<RyaStatement> parent, Optional<RyaStatement> child) {
        Date copyToolInputTime = mergeParentMetadata.getCopyToolInputTime();
        boolean usesStartTime = mergeParentMetadata.isUsesStartTime();
        Date startTime = mergeParentMetadata.getTimestamp();

        log.trace("parent = " + parent);
        log.trace("child = " + child);

        return Optional.absent();
//        if (!parent.isPresent() && !child.isPresent()) {
//            // Reached the end of the parent and child table.
//            return null;
//        } else if (!parent.isPresent()) {
//            // Reached the end of the parent table so add the remaining child keys if they meet the time criteria.
//            Date t2 = normalizeDate(new Date(child.get().getTimestamp()), false);
//            // Move on to next comparison (do nothing) or add this child key to parent
//            boolean doNothing = usesStartTime && t2.before(startTime);
//            return doNothing ? CompareKeysResult.ADVANCE_CHILD : CompareKeysResult.ADVANCE_CHILD_AND_ADD;
//        } else if (!child.isPresent()) {
//            // Reached the end of the child table so delete the remaining parent keys if they meet the time criteria.
//            Date t1 = normalizeDate(new Date(parent.get().getTimestamp()), true);
//            // Move on to next comparison (do nothing) or delete this key from parent
//            boolean doNothing = usesStartTime && (copyToolInputTime != null && (t1.before(copyToolInputTime) || (t1.after(copyToolInputTime) && t1.after(startTime))) || (copyToolInputTime == null && t1.after(startTime)));
//            return doNothing ? CompareKeysResult.ADVANCE_PARENT : CompareKeysResult.ADVANCE_PARENT_AND_DELETE;
//        } else {
//            // There are 2 keys to compare
//            Map<TABLE_LAYOUT, TripleRow> map1 = parentRyaTripleContext.serializeTriple(parent.get());
//            Text row1 = new Text(map1.get(TABLE_LAYOUT.SPO).getRow());
//            Map<TABLE_LAYOUT, TripleRow> map2 = childRyaTripleContext.serializeTriple(child.get());
//            Text row2 = new Text(map2.get(TABLE_LAYOUT.SPO).getRow());
//            Date t1 = normalizeDate(new Date(parent.get().getTimestamp()), true);
//            Date t2 = normalizeDate(new Date(child.get().getTimestamp()), false);
//
//            if (row1.compareTo(row2) < 0) {
//                // Parent key sort order was before the child key sort order
//                // so it doesn't exist in the child table.
//                // What does this mean?  Was it added by the parent after the child was cloned? (Meaning we should leave it)
//                // Or did the child delete it after it was cloned? (Meaning we should delete it)
//                boolean doNothing = usesStartTime && (copyToolInputTime != null && (t1.before(copyToolInputTime) || (t1.after(copyToolInputTime) && t1.after(startTime))) || (copyToolInputTime == null && t1.after(startTime)));
//                return doNothing ? CompareKeysResult.ADVANCE_PARENT : CompareKeysResult.ADVANCE_PARENT_AND_DELETE;
//            } else if (row1.compareTo(row2) > 0) {
//                // Parent key sort order was after the child key sort order
//                // so it doesn't exist in the parent table.
//                // What does this mean?  Was it deleted by the parent after the child was cloned? (Meaning we should leave it)
//                // Or did the child add it after it was cloned? (Meaning we should add it)
//                boolean doNothing = usesStartTime && t2.before(startTime);
//                return doNothing ? CompareKeysResult.ADVANCE_CHILD : CompareKeysResult.ADVANCE_CHILD_AND_ADD;
//            } else {
//                // Rows are the same. So just check if column visibility needs to be updated and
//                // move on to the next parent and child keys.
//                return CompareKeysResult.ADVANCE_BOTH;
//            }
//        }
    }

    /**
     * Adjusts the date of a key's timestamp to account for the instance's machine local time offset.
     * @param date the timestamp {@link Date} to adjust.
     * @param isParentTable {@code true} if the timestamp is from a key in one of the parent instance's tables.
     * {@code false} if it's from the child instance.
     * @return the normalized {@link Date} or the same date if nothing needed to be adjusted.
     */
    private Date normalizeDate(Date date, boolean isParentTable) {
        Date normalizedDate = date;
        if (mergeParentMetadata.isUseTimeSync()) {
            if (isParentTable) {
                normalizedDate = new Date(date.getTime() - mergeParentMetadata.getParentTimeOffset());
            } else {
                // If the timestamp is before the time the child table was copied from
                // the parent then the timestamp originated from the parent machine
                if (TimeUtils.dateBeforeInclusive(date, mergeParentMetadata.getCopyToolInputTime())) {
                    normalizedDate = new Date(date.getTime() - mergeParentMetadata.getParentTimeOffset());
                } else {
                    // Timestamps after the copy time originated from the child machine.
                    normalizedDate = new Date(date.getTime() - mergeParentMetadata.getChildTimeOffset());
                }
            }
        }
        return normalizedDate;
    }

}
