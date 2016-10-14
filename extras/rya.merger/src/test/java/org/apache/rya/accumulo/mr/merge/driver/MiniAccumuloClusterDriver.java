/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.accumulo.mr.merge.driver;

import static org.apache.rya.accumulo.mr.merge.util.TestUtils.LAST_MONTH;
import static org.apache.rya.accumulo.mr.merge.util.TestUtils.TODAY;
import static org.apache.rya.accumulo.mr.merge.util.TestUtils.createRyaStatement;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.log4j.Logger;

import org.apache.rya.accumulo.mr.merge.util.AccumuloRyaUtils;
import org.apache.rya.accumulo.mr.merge.util.TestUtils;
import org.apache.rya.api.RdfCloudTripleStoreConstants;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.persist.RyaDAOException;

/**
 * Runs a {@link MiniAccumuloCluster}.
 */
public class MiniAccumuloClusterDriver extends AccumuloDualInstanceDriver {
    private static final Logger log = Logger.getLogger(MiniAccumuloClusterDriver.class);

    /**
     * {@code true} to configure the cluster for merging.  {@code false} to configure
     * the cluster for copying.
     */
    private static final boolean IS_MERGE_SETUP = true;

    private static boolean keepRunning = true;

    /**
     * Creates a new instance of {@link MiniAccumuloClusterDriver}.
     */
    public MiniAccumuloClusterDriver() {
        super(false, !IS_MERGE_SETUP, !IS_MERGE_SETUP, IS_MERGE_SETUP, IS_MERGE_SETUP);
    }

    private void writeStatements() throws RyaDAOException, IOException {
        // This statement was in both parent/child instances a month ago and is before the start time of yesterday
        // but it was left alone.  It should remain in the parent after merging.
        final RyaStatement ryaStatementOutOfTimeRange = createRyaStatement("coach", "called", "timeout", LAST_MONTH);

        // This statement was in both parent/child instances a month ago but after the start time of yesterday
        // the parent deleted it and the child still has it.  It should stay deleted in the parent after merging.
        final RyaStatement ryaStatementParentDeletedAfter = createRyaStatement("parent", "deleted", "after", LAST_MONTH);

        // This statement was added by the parent after the start time of yesterday and doesn't exist in the child.
        // It should stay in the parent after merging.
        final RyaStatement ryaStatementParentAddedAfter = createRyaStatement("parent", "added", "after", TODAY);

        // This statement was in both parent/child instances a month ago but after the start time of yesterday
        // the child deleted it and the parent still has it.  It should be deleted from the parent after merging.
        final RyaStatement ryaStatementChildDeletedAfter = createRyaStatement("child", "deleted", "after", LAST_MONTH);

        // This statement was added by the child after the start time of yesterday and doesn't exist in the parent.
        // It should be added to the parent after merging.
        final RyaStatement ryaStatementChildAddedAfter = createRyaStatement("child", "added", "after", TODAY);

        // This statement was modified by the child after the start of yesterday (The timestamp changes after updating)
        // It should be updated in the parent to match the child.
        final RyaStatement ryaStatementUpdatedByChild = createRyaStatement("bob", "catches", "ball", LAST_MONTH);

        final RyaStatement ryaStatementUntouchedByChild = createRyaStatement("bill", "talks to", "john", LAST_MONTH);

        final RyaStatement ryaStatementDeletedByChild = createRyaStatement("susan", "eats", "burgers", LAST_MONTH);

        final RyaStatement ryaStatementAddedByChild = createRyaStatement("ronnie", "plays", "guitar", TODAY);

        // This statement was modified by the child to change the column visibility.
        // The parent should combine the child's visibility with its visibility.
        final RyaStatement ryaStatementVisibilityDifferent = createRyaStatement("I", "see", "you", LAST_MONTH);
        ryaStatementVisibilityDifferent.setColumnVisibility(PARENT_COLUMN_VISIBILITY.getExpression());

        final List<RyaStatement> parentStatements = new ArrayList<>();
        final List<RyaStatement> childStatements = new ArrayList<>();

        // Setup initial parent instance with 7 rows
        // This is the state of the parent data (as it is today) before merging occurs which will use the specified start time of yesterday.
        parentStatements.add(ryaStatementOutOfTimeRange);      // Merging should keep statement
        parentStatements.add(ryaStatementUpdatedByChild);      // Merging should update statement
        parentStatements.add(ryaStatementUntouchedByChild);    // Merging should keep statement
        parentStatements.add(ryaStatementDeletedByChild);      // Merging should delete statement
        parentStatements.add(ryaStatementVisibilityDifferent); // Merging should update statement
        parentStatements.add(ryaStatementParentAddedAfter);    // Merging should keep statement
        parentStatements.add(ryaStatementChildDeletedAfter);   // Merging should delete statement

        addParentRyaStatements(parentStatements);

        // Simulate the child coming back with a modified data set before the merging occurs.
        // (1 updated row, 1 row left alone because it was unchanged, 1 row outside time range,
        // 1 row deleted, 1 new row added, 1 modified visibility, 1 deleted by child, 1 added by child).
        // There should be 5 rows in the child instance (4 which will be scanned over from the start time).
        ryaStatementUpdatedByChild.setObject(TestUtils.createRyaUri("football"));
        ryaStatementUpdatedByChild.setTimestamp(TODAY.getTime());
        ryaStatementVisibilityDifferent.setColumnVisibility(CHILD_COLUMN_VISIBILITY.getExpression());
        childStatements.add(ryaStatementOutOfTimeRange);
        childStatements.add(ryaStatementUpdatedByChild);
        childStatements.add(ryaStatementUntouchedByChild);
        childStatements.add(ryaStatementAddedByChild);         // Merging should add statement
        childStatements.add(ryaStatementVisibilityDifferent);
        childStatements.add(ryaStatementParentDeletedAfter);
        childStatements.add(ryaStatementChildAddedAfter);      // Merging should add statement

        if (IS_MERGE_SETUP) {
            addChildRyaStatements(childStatements);
        }


        AccumuloRyaUtils.printTable(PARENT_TABLE_PREFIX + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX, getParentConfig());
        AccumuloRyaUtils.printTable(CHILD_TABLE_PREFIX + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX, getChildConfig());
    }

    public static void main(final String args[]) {
        log.info("Setting up MiniAccumulo Cluster");

        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(final Thread thread, final Throwable throwable) {
                log.fatal("Uncaught exception in " + thread.getName(), throwable);
            }
        });

        final MiniAccumuloClusterDriver macDriver = new MiniAccumuloClusterDriver();
        try {
            macDriver.setUp();
            log.info("Populating clusters");
            macDriver.writeStatements();
            log.info("MiniAccumuloClusters running and populated");
        } catch (final Exception e) {
            log.error("Error setting up and writing statements", e);
            keepRunning = false;
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                log.info("Shutting down...");
                try {
                    macDriver.tearDown();
                } catch (final Exception e) {
                    log.error("Error while shutting down", e);
                } finally {
                    keepRunning = false;
                    log.info("Done shutting down");
                }
            }
        });

        while(keepRunning) {
            try {
                Thread.sleep(2000);
            } catch (final InterruptedException e) {
                log.error("Interrupted exception while running MiniAccumuloClusterDriver", e);
                keepRunning = false;
            }
        }
    }
}