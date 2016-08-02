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
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.rya.export.api.Merger;
import org.apache.rya.export.api.MergerException;
import org.apache.rya.export.api.ParentMetadataRepository.MergeParentMetadata;
import org.apache.rya.export.api.RyaStatementStore;

import com.google.common.base.Optional;

import info.aduna.iteration.CloseableIteration;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.persist.RyaDAOException;
import mvm.rya.indexing.accumulo.ConfigUtils;

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
     */
    public AccumuloMerger(final AccumuloRyaStatementStore accumuloParentRyaStatementStore, final RyaStatementStore childRyaStatementStore) {
        this.accumuloParentRyaStatementStore = checkNotNull(accumuloParentRyaStatementStore);
        this.childRyaStatementStore = checkNotNull(childRyaStatementStore);
        final AccumuloParentMetadataRepository accumuloParentMetadataRepository = new AccumuloParentMetadataRepository();
        final String instanceName = getConfig().get(ConfigUtils.CLOUDBASE_INSTANCE);
        final MergeParentMetadata mergeParentMetadata = new MergeParentMetadata(instanceName, new Date());
        accumuloParentMetadataRepository.set(mergeParentMetadata);
        accumuloStatementManager = new AccumuloStatementManager(accumuloParentRyaStatementStore, childRyaStatementStore, accumuloParentMetadataRepository);
    }

    /**
     * @return the parent {@link Configuration}.
     */
    public Configuration getConfig() {
        return accumuloParentRyaStatementStore.getConfiguration();
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
                final CloseableIteration<RyaStatement, RyaDAOException> parentIter = accumuloParentRyaStatementStore.findStatement(childRyaStatement);

                RyaStatement parentRyaStatementResult = null;
                if (parentIter.hasNext()) {
                    parentRyaStatementResult = parentIter.next();
                }

                final Optional<RyaStatement> result = accumuloStatementManager.merge(Optional.fromNullable(parentRyaStatementResult), Optional.fromNullable(childRyaStatement));
                if (result.isPresent()) {
                    //accumuloParentRyaStatementStore.addStatement(result.get());
                }

                childRyaStatement = nextRyaStatement(childRyaStatementIterator);
            }

            // Now go through the parent statements.
            final Iterator<RyaStatement> parentRyaStatementIterator = accumuloParentRyaStatementStore.fetchStatements();

            RyaStatement parentRyaStatement = nextRyaStatement(parentRyaStatementIterator);
            while (parentRyaStatement != null) {
                final CloseableIteration<RyaStatement, RyaDAOException> childIter = childRyaStatementStore.findStatement(parentRyaStatement);

                RyaStatement childRyaStatementResult = null;
                if (childIter.hasNext()) {
                    childRyaStatementResult = childIter.next();
                }

                final Optional<RyaStatement> result = accumuloStatementManager.merge(Optional.fromNullable(parentRyaStatement), Optional.fromNullable(childRyaStatementResult));
                if (result.isPresent()) {
                    //accumuloParentRyaStatementStore.addStatement(result.get());
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