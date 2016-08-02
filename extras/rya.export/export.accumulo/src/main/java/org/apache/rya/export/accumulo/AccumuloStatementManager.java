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
import java.util.Map;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.rya.export.accumulo.conf.AccumuloExportConstants;
import org.apache.rya.export.accumulo.util.TimeUtils;
import org.apache.rya.export.api.MergerException;
import org.apache.rya.export.api.ParentMetadataRepository.MergeParentMetadata;
import org.apache.rya.export.api.RyaStatementStore;
import org.apache.rya.export.api.StatementManager;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;

import mvm.rya.accumulo.AccumuloRdfConstants;
import mvm.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.resolver.RyaTripleContext;
import mvm.rya.api.resolver.triple.TripleRow;
import mvm.rya.api.resolver.triple.TripleRowResolverException;

/**
 * Defines how 2 {@link RyaStatement}s will merge.
 */
public class AccumuloStatementManager implements StatementManager {
    private static final Logger log = Logger.getLogger(AccumuloStatementManager.class);

    private final AccumuloRyaStatementStore parentAccumuloRyaStatementStore;
    private final AccumuloParentMetadataRepository accumuloParentMetadataRepository;
    private final MergeParentMetadata mergeParentMetadata;
    private final RyaTripleContext parentRyaTripleContext;
    private final RyaTripleContext childRyaTripleContext;

    private boolean usesStartTime = false;
    private boolean useTimeSync = false;
    private boolean useMergeFileInput = false;
    private Long childTimeOffset = null;

    /**
     * Creates a new instance of {@link AccumuloStatementManager}.
     * @param parentAccumuloRyaStatementStore the parent
     * {@link AccumuloRyaStatementStore}. (not {@code null})
     * @param parentAccumuloRyaStatementStore the child
     * {@link RyaStatementStore}. (not {@code null})
     * @param accumuloParentMetadataRepository the
     * {@link AccumuloParentMetadataRepository}. (not {@code null})
     */
    public AccumuloStatementManager(final AccumuloRyaStatementStore parentAccumuloRyaStatementStore, final RyaStatementStore childRyaStatementStore, final AccumuloParentMetadataRepository accumuloParentMetadataRepository) {
        this.parentAccumuloRyaStatementStore = checkNotNull(parentAccumuloRyaStatementStore);
        this.accumuloParentMetadataRepository = checkNotNull(accumuloParentMetadataRepository);
        mergeParentMetadata = accumuloParentMetadataRepository.get();
        parentRyaTripleContext = parentAccumuloRyaStatementStore.getRyaTripleContext();
        childRyaTripleContext = checkNotNull(childRyaStatementStore).getRyaTripleContext();

        usesStartTime = mergeParentMetadata.getTimestamp() != null;
        useTimeSync = parentAccumuloRyaStatementStore.getConfiguration().getBoolean(AccumuloExportConstants.USE_NTP_SERVER_PROP, false);
        useMergeFileInput = parentAccumuloRyaStatementStore.getConfiguration().getBoolean(AccumuloExportConstants.USE_MERGE_FILE_INPUT, false);
        childTimeOffset = Long.valueOf(parentAccumuloRyaStatementStore.getConfiguration().get(AccumuloExportConstants.CHILD_TIME_OFFSET_PROP, null));
    }

    @Override
    public Optional<RyaStatement> merge(final Optional<RyaStatement> parent, final Optional<RyaStatement> child) throws MergerException {
        final Date copyToolInputTime = mergeParentMetadata.getCopyToolInputTime();
        final Date startTime = mergeParentMetadata.getTimestamp();

        log.trace("parent = " + parent);
        log.trace("child = " + child);

        if (!parent.isPresent() && !child.isPresent()) {
            // Reached the end of the parent and child table.
            return Optional.absent();
        } else if (!parent.isPresent()) {
            // Reached the end of the parent table so add the remaining child keys if they meet the time criteria.
            final Date t2 = normalizeDate(new Date(child.get().getTimestamp()), false);
            // Move on to next comparison (do nothing) or add this child key to parent
            final boolean doNothing = usesStartTime && t2.before(startTime);
            if (!doNothing) {
                parentAccumuloRyaStatementStore.addStatement(child.get());
            }
        } else if (!child.isPresent()) {
            // Reached the end of the child table so delete the remaining parent keys if they meet the time criteria.
            final Date t1 = normalizeDate(new Date(parent.get().getTimestamp()), true);
            // Move on to next comparison (do nothing) or delete this key from parent
            final boolean doNothing = usesStartTime && (copyToolInputTime != null && (t1.before(copyToolInputTime) || (t1.after(copyToolInputTime) && t1.after(startTime))) || (copyToolInputTime == null && t1.after(startTime)));
            if (!doNothing) {
                parentAccumuloRyaStatementStore.removeStatement(parent.get());
            }
        } else {
            // There are 2 keys to compare
            final Text row1 = getRow(parent.get(), parentRyaTripleContext);
            final Text row2 = getRow(child.get(), childRyaTripleContext);
            final Date t1 = normalizeDate(new Date(parent.get().getTimestamp()), true);
            final Date t2 = normalizeDate(new Date(child.get().getTimestamp()), false);

            if (row1.compareTo(row2) < 0) {
                // Parent key sort order was before the child key sort order
                // so it doesn't exist in the child table.
                // What does this mean?  Was it added by the parent after the child was cloned? (Meaning we should leave it)
                // Or did the child delete it after it was cloned? (Meaning we should delete it)
                final boolean doNothing = usesStartTime && (copyToolInputTime != null && (t1.before(copyToolInputTime) || (t1.after(copyToolInputTime) && t1.after(startTime))) || (copyToolInputTime == null && t1.after(startTime)));
                if (!doNothing) {
                    parentAccumuloRyaStatementStore.removeStatement(parent.get());
                }
            } else if (row1.compareTo(row2) > 0) {
                // Parent key sort order was after the child key sort order
                // so it doesn't exist in the parent table.
                // What does this mean?  Was it deleted by the parent after the child was cloned? (Meaning we should leave it)
                // Or did the child add it after it was cloned? (Meaning we should add it)
                final boolean doNothing = usesStartTime && t2.before(startTime);
                if (!doNothing) {
                    parentAccumuloRyaStatementStore.addStatement(child.get());
                }
            } else {
                // Rows are the same. So just check if column visibility needs to be updated and
                // move on to the next parent and child keys.
                return checkColumnVisibilities(parent.get(), child.get());
            }
        }

        return Optional.absent();
    }

    private Optional<RyaStatement> checkColumnVisibilities(final RyaStatement parentRyaStatement, final RyaStatement childRyaStatement) throws MergerException {
        final ColumnVisibility cv1 = new ColumnVisibility(parentRyaStatement.getColumnVisibility() != null ? parentRyaStatement.getColumnVisibility() : AccumuloRdfConstants.EMPTY_CV.getExpression());
        final ColumnVisibility cv2 = new ColumnVisibility(childRyaStatement.getColumnVisibility() != null ? childRyaStatement.getColumnVisibility() : AccumuloRdfConstants.EMPTY_CV.getExpression());

        // Update new column visibility now if necessary
        if (!cv1.equals(cv2) && !cv2.equals(AccumuloRdfConstants.EMPTY_CV)) {
            final ColumnVisibility newCv = combineColumnVisibilities(cv1, cv2);
            final RyaStatement newCvRyaStatement = updateRyaStatementColumnVisibility(parentRyaStatement, newCv);

            parentAccumuloRyaStatementStore.removeStatement(parentRyaStatement);
            parentAccumuloRyaStatementStore.addStatement(newCvRyaStatement);
            return Optional.fromNullable(newCvRyaStatement);
        }

        return Optional.fromNullable(parentRyaStatement);
    }

    /**
     * Combines 2 {@link ColumnVisibility ColumnVisibilities} by OR'ing them together.
     * @param cv1 the first (parent) {@link ColumnVisibility}.
     * @param cv2 the second (child) {@link ColumnVisibility}.
     * @return the newly combined {@link ColumnVisibility}.
     */
    private static ColumnVisibility combineColumnVisibilities(final ColumnVisibility cv1, final ColumnVisibility cv2) {
        // OR the 2 column visibilities together if they're different
        String columnVisibilityExpression;
        if (cv1.equals(AccumuloRdfConstants.EMPTY_CV)) {
            columnVisibilityExpression = new String(cv2.getExpression(), Charsets.UTF_8);
        } else {
            columnVisibilityExpression = "(" + new String(cv1.getExpression(), Charsets.UTF_8) + ")|("
                    + new String(cv2.getExpression(), Charsets.UTF_8) + ")";
        }
        ColumnVisibility newCv = new ColumnVisibility(new Text(columnVisibilityExpression));
        newCv = new ColumnVisibility(newCv.flatten());
        return newCv;
    }

    private static RyaStatement updateRyaStatementColumnVisibility(final RyaStatement ryaStatement, final ColumnVisibility newCv) {
        final RyaStatement newCvRyaStatement = new RyaStatement(ryaStatement.getSubject(), ryaStatement.getPredicate(), ryaStatement.getObject(), ryaStatement.getContext(), ryaStatement.getQualifer(), newCv.getExpression(), ryaStatement.getValue(), ryaStatement.getTimestamp());
        return newCvRyaStatement;
    }

    private static Text getRow(final RyaStatement ryaStatement, final RyaTripleContext ryaTripleContext) throws MergerException {
        Text row = null;
        try {
            final Map<TABLE_LAYOUT, TripleRow> map = ryaTripleContext.serializeTriple(ryaStatement);
            row = new Text(map.get(TABLE_LAYOUT.SPO).getRow());
        } catch (final TripleRowResolverException e) {
            throw new MergerException();
        }
        return row;
    }

    /**
     * Adjusts the date of a key's timestamp to account for the instance's machine local time offset.
     * @param date the timestamp {@link Date} to adjust.
     * @param isParentTable {@code true} if the timestamp is from a key in one of the parent instance's tables.
     * {@code false} if it's from the child instance.
     * @return the normalized {@link Date} or the same date if nothing needed to be adjusted.
     */
    private Date normalizeDate(final Date date, final boolean isParentTable) {
        Date normalizedDate = date;
        if (useTimeSync) {
            if (isParentTable) {
                normalizedDate = new Date(date.getTime() - mergeParentMetadata.getParentTimeOffset());
            } else {
                // If the timestamp is before the time the child table was copied from
                // the parent then the timestamp originated from the parent machine
                if (TimeUtils.dateBeforeInclusive(date, mergeParentMetadata.getCopyToolInputTime())) {
                    normalizedDate = new Date(date.getTime() - mergeParentMetadata.getParentTimeOffset());
                } else {
                    // Timestamps after the copy time originated from the child machine.
                    normalizedDate = new Date(date.getTime() - childTimeOffset);
                }
            }
        }
        return normalizedDate;
    }

}
