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

import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.rya.export.api.Merger;
import org.apache.rya.export.api.MergerException;
import org.apache.rya.export.api.ParentMetadataRepository.MergeParentMetadata;
import org.apache.rya.export.api.RyaStatementStore;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;

import info.aduna.iteration.CloseableIteration;
import mvm.rya.accumulo.AccumuloRdfConstants;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.persist.RyaDAOException;
import mvm.rya.indexing.accumulo.ConfigUtils;

/**
 * Handles Merging a parent Accumulo instance with a child Accumulo instance.
 */
public class AccumuloMerger implements Merger {
    private static final Logger log = Logger.getLogger(AccumuloMerger.class);

    private AccumuloRyaStatementStore accumuloParentRyaStatementStore;
    private RyaStatementStore childRyaStatementStore;
    private AccumuloStatementManager accumuloStatementManager;

    /**
     * Creates a new instance of {@link AccumuloMerger}.
     * @param accumuloParentRyaStatementStore the Accumulo parent
     * {@link RyaStatementStore}. (not {@code null})
     * @param childRyaStatementStore the child {@link RyaStatementStore}.
     * (not {@code null})
     */
    public AccumuloMerger(AccumuloRyaStatementStore accumuloParentRyaStatementStore, RyaStatementStore childRyaStatementStore) {
        this.accumuloParentRyaStatementStore = checkNotNull(accumuloParentRyaStatementStore);
        this.childRyaStatementStore = checkNotNull(childRyaStatementStore);
        AccumuloParentMetadataRepository accumuloParentMetadataRepository = new AccumuloParentMetadataRepository();
        String instanceName = getConfig().get(ConfigUtils.CLOUDBASE_INSTANCE);
        MergeParentMetadata mergeParentMetadata = new MergeParentMetadata(instanceName, new Date());
        accumuloParentMetadataRepository.set(mergeParentMetadata);
        accumuloStatementManager = new AccumuloStatementManager(accumuloParentRyaStatementStore, childRyaStatementStore, accumuloParentMetadataRepository);
    }

    /**
     * @return the {@link Configuration}.
     */
    public Configuration getConfig() {
        return accumuloParentRyaStatementStore.getConfiguration();
    }

    /**
     * @return the {@link AccumuloRyaStatementStore}.
     */
    public AccumuloRyaStatementStore getAccumuloRyaStatementStore() {
        return accumuloParentRyaStatementStore;
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
            accumuloParentRyaStatementStore.init();
        } catch (MergerException e) {
            log.error("Error while running merge job", e);
        }

        try {
            //Iterator<RyaStatement> parentRyaStatementIterator = accumuloParentRyaStatementStore.fetchStatements();
            Iterator<RyaStatement> childRyaStatementIterator = childRyaStatementStore.fetchStatements();

            //RyaStatement parentRyaStatement = nextRyaStatement(parentRyaStatementIterator);
            RyaStatement childRyaStatement = nextRyaStatement(childRyaStatementIterator);
            while (childRyaStatement != null) {

                CloseableIteration<RyaStatement, RyaDAOException> parentIter = accumuloParentRyaStatementStore.findStatement(childRyaStatement);

                RyaStatement parentRyaStatement = null;
                if (parentIter.hasNext()) {
                    parentRyaStatement = parentIter.next();
                }

                Optional<RyaStatement> result = accumuloStatementManager.merge(Optional.of(parentRyaStatement), Optional.of(childRyaStatement));
                if (result.isPresent()) {
                    //accumuloParentRyaStatementStore.addStatement(result.get());
                }
            }
        } catch (RyaDAOException | MergerException e) {
            log.error("Error encountered while merging", e);
        }

///////////
//            CompareKeysResult compareKeysResult = null;
//            // Iteratively compare parent keys to child keys until finished
//            while (compareKeysResult != CompareKeysResult.FINISHED) {
//                compareKeysResult = compareKeys(parentRyaStatement, childRyaStatement);
//
//                // Based on how the keys compare add or delete keys and advance the child or parent iterators forward
//                switch (compareKeysResult) {
//                    case ADVANCE_CHILD:
//                        childRyaStatement = nextRyaStatement(childRyaStatementIterator);
//                        break;
//                    case ADVANCE_PARENT:
//                        parentRyaStatement = nextRyaStatement(parentRyaStatementIterator);
//                        break;
//                    case ADVANCE_CHILD_AND_ADD:
//                        RyaStatement tempChildRyaStatement = childRyaStatement;
//                        childRyaStatement = nextRyaStatement(childRyaStatementIterator);
//                        //addKey(tempChildRyaStatement, context);
//                        accumuloParentRyaStatementStore.addStatement(tempChildRyaStatement);
//                        break;
//                    case ADVANCE_PARENT_AND_DELETE:
//                        RyaStatement tempParentRyaStatement = parentRyaStatement;
//                        parentRyaStatement = nextRyaStatement(parentRyaStatementIterator);
//                        //deleteKey(tempParentRyaStatement, context);
//                        accumuloParentRyaStatementStore.removeStatement(tempParentRyaStatement);
//                        break;
//                    case ADVANCE_BOTH:
//                        ColumnVisibility cv1 = new ColumnVisibility(parentRyaStatement.getColumnVisibility());
//                        ColumnVisibility cv2 = new ColumnVisibility(childRyaStatement.getColumnVisibility());
//
//                        // Update new column visibility now if necessary
//                        if (!cv1.equals(cv2) && !cv2.equals(AccumuloRdfConstants.EMPTY_CV)) {
//                            ColumnVisibility newCv = combineColumnVisibilities(cv1, cv2);
//                            RyaStatement newCvRyaStatement = updateRyaStatementColumnVisibility(parentRyaStatement, newCv);
//
//                            //deleteKey(parentRyaStatement, context);
//                            //addKey(newCvRyaStatement, context);
//                            accumuloParentRyaStatementStore.removeStatement(parentRyaStatement);
//                            accumuloParentRyaStatementStore.addStatement(newCvRyaStatement);
//                        }
//
//                        parentRyaStatement = nextRyaStatement(childRyaStatementIterator);
//                        childRyaStatement = nextRyaStatement(childRyaStatementIterator);
//                        break;
//                    case FINISHED:
//                        log.info("Finished scanning parent and child tables");
//                        break;
//                    default:
//                        log.error("Unknown result: " + compareKeysResult);
//                        break;
//                }
//            }
//        } catch (MutationsRejectedException | TripleRowResolverException e) {
//            log.error("Error encountered while merging", e);
//        }
    }

    private static RyaStatement nextRyaStatement(Iterator<RyaStatement> iterator) {
        RyaStatement ryaStatement = null;
        if (iterator.hasNext()) {
            ryaStatement = iterator.next();
        }
        return ryaStatement;
    }

    /**
     * Combines 2 {@link ColumnVisibility ColumnVisibilities} by OR'ing them together.
     * @param cv1 the first (parent) {@link ColumnVisibility}.
     * @param cv2 the second (child) {@link ColumnVisibility}.
     * @return the newly combined {@link ColumnVisibility}.
     */
    public static ColumnVisibility combineColumnVisibilities(ColumnVisibility cv1, ColumnVisibility cv2) {
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

    private static RyaStatement updateRyaStatementColumnVisibility(RyaStatement ryaStatement, ColumnVisibility newCv) {
        RyaStatement newCvRyaStatement = new RyaStatement(ryaStatement.getSubject(), ryaStatement.getPredicate(), ryaStatement.getObject(), ryaStatement.getContext(), ryaStatement.getQualifer(), newCv.getExpression(), ryaStatement.getValue(), ryaStatement.getTimestamp());
        return newCvRyaStatement;
    }
}