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

import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.rya.export.api.Merger;
import org.apache.rya.export.api.MergerException;
import org.apache.rya.export.api.parent.MergeParentMetadata;
import org.apache.rya.export.api.parent.ParentMetadataDoesNotExistException;
import org.apache.rya.export.api.store.RyaStatementStore;

import com.google.common.base.Optional;

import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.persist.RyaDAOException;

/**
 * Handles Merging a parent Accumulo instance with another DB child instance.
 */
public class AccumuloMerger implements Merger {
    private static final Logger log = Logger.getLogger(AccumuloMerger.class);

    private final AccumuloRyaStatementStore accumuloParentRyaStatementStore;
    private final RyaStatementStore childRyaStatementStore;
    private final AccumuloStatementManager accumuloStatementManager;

    /**
     * Creates a new instance of {@link AccumuloMerger}.
     * @param accumuloParentRyaStatementStore the Accumulo parent
     * {@link RyaStatementStore}. (not {@code null})
     * @param childRyaStatementStore the child {@link RyaStatementStore}.
     * (not {@code null})
     * @param accumuloParentMetadataRepository the
     * {@link AccumuloParentMetadataRepository}. (not {@code null})
     */
    public AccumuloMerger(final AccumuloRyaStatementStore accumuloParentRyaStatementStore, final RyaStatementStore childRyaStatementStore, final AccumuloParentMetadataRepository accumuloParentMetadataRepository) {
        this.accumuloParentRyaStatementStore = checkNotNull(accumuloParentRyaStatementStore);
        this.childRyaStatementStore = checkNotNull(childRyaStatementStore);

        MergeParentMetadata mergeParentMetadata = null;
        try {
            mergeParentMetadata = accumuloParentMetadataRepository.get();
        } catch (final ParentMetadataDoesNotExistException e) {
            log.error("Error getting merge parent metadata from the repository.", e);
        }

        accumuloStatementManager = new AccumuloStatementManager(accumuloParentRyaStatementStore, childRyaStatementStore, mergeParentMetadata);
    }

    /**
     * @return the parent {@link Configuration}.
     */
    public Configuration getConfig() {
        return accumuloParentRyaStatementStore.getRyaDAO().getConf();
    }

    /**
     * @return the parent {@link AccumuloRyaStatementStore}.
     */
    public AccumuloRyaStatementStore getParentRyaStatementStore() {
        return accumuloParentRyaStatementStore;
    }

    /**
     * @return the child {@link RyaStatementStore}.
     */
    public RyaStatementStore getChildRyaStatementStore() {
       return childRyaStatementStore;
    }

    /**
     * @return the {@link AccumuloStatementManager}.
     */
    public AccumuloStatementManager getAccumuloStatementManager() {
        return accumuloStatementManager;
    }

    @Override
    public void runJob() {
        try {
            // Go through the child statements first.
            final Iterator<RyaStatement> childRyaStatementIterator = childRyaStatementStore.fetchStatements();

            RyaStatement childRyaStatement = nextRyaStatement(childRyaStatementIterator);
            while (childRyaStatement != null) {
                final RyaStatement parentRyaStatementResult = accumuloParentRyaStatementStore.findStatement(childRyaStatement);

                accumuloStatementManager.merge(Optional.fromNullable(parentRyaStatementResult), Optional.fromNullable(childRyaStatement));

                childRyaStatement = nextRyaStatement(childRyaStatementIterator);
            }

            // Now go through the parent statements.
            // But, now since we covered all the child statements above, we only
            // care about the statements that are in the parent and don't exist
            // in the child (which the above loop would miss).
            // We need to determine if the statement is only in the parent
            // because the child deleted it after being copied from the parent
            // (meaning the parent should delete it too) or that the parent
            // added it after the copy (meaning the parent should still keep
            // that statement)
            final Iterator<RyaStatement> parentRyaStatementIterator = accumuloParentRyaStatementStore.fetchStatements();

            RyaStatement parentRyaStatement = nextRyaStatement(parentRyaStatementIterator);
            while (parentRyaStatement != null) {
                final boolean doesChildRyaStatementExist = childRyaStatementStore.containsStatement(parentRyaStatement);

                // Only care about the parent statements not found in the child.
                if (!doesChildRyaStatementExist) {
                    accumuloStatementManager.merge(Optional.fromNullable(parentRyaStatement), Optional.absent());
                }

                parentRyaStatement = nextRyaStatement(parentRyaStatementIterator);
            }
        } catch (RyaDAOException | MergerException e) {
            log.error("Error encountered while merging", e);
        }
    }

    private static RyaStatement nextRyaStatement(final Iterator<RyaStatement> iterator) {
        RyaStatement ryaStatement = null;
        if (iterator.hasNext()) {
            ryaStatement = iterator.next();
        }
        return ryaStatement;
    }
}