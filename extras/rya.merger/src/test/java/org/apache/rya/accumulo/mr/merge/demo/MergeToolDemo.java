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
package org.apache.rya.accumulo.mr.merge.demo;

import static org.apache.rya.accumulo.mr.merge.util.TestUtils.createRyaStatement;
import static org.apache.rya.accumulo.mr.merge.util.ToolConfigUtils.makeArgument;

import java.util.Date;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.log4j.Logger;

import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.accumulo.mr.MRUtils;
import org.apache.rya.accumulo.mr.merge.CopyTool;
import org.apache.rya.accumulo.mr.merge.MergeTool;
import org.apache.rya.accumulo.mr.merge.demo.util.DemoUtilities;
import org.apache.rya.accumulo.mr.merge.demo.util.DemoUtilities.LoggingDetail;
import org.apache.rya.accumulo.mr.merge.driver.AccumuloDualInstanceDriver;
import org.apache.rya.accumulo.mr.merge.util.AccumuloRyaUtils;
import org.apache.rya.accumulo.mr.merge.util.TestUtils;
import org.apache.rya.accumulo.mr.merge.util.TimeUtils;
import org.apache.rya.api.RdfCloudTripleStoreConstants;
import org.apache.rya.api.domain.RyaStatement;

/**
 * Tests for {@link MergeTool}.
 */
public class MergeToolDemo {
    private static final Logger log = Logger.getLogger(MergeToolDemo.class);

    private static final boolean IS_MOCK = true;
    private static final boolean USE_TIME_SYNC = false;
    private static final boolean USE_MERGE_FILE_INPUT = false;
    private static final boolean IS_PROMPTING_ENABLED = true;
    private static final boolean IS_START_TIME_DIALOG_ENABLED = true;
    private static final LoggingDetail LOGGING_DETAIL = LoggingDetail.LIGHT;

    private static final String CHILD_SUFFIX = MergeTool.CHILD_SUFFIX;

    private static final String PARENT_PASSWORD = AccumuloDualInstanceDriver.PARENT_PASSWORD;
    private static final String PARENT_INSTANCE = AccumuloDualInstanceDriver.PARENT_INSTANCE;
    private static final String PARENT_TABLE_PREFIX = AccumuloDualInstanceDriver.PARENT_TABLE_PREFIX;
    private static final String PARENT_AUTH = AccumuloDualInstanceDriver.PARENT_AUTH;
    private static final ColumnVisibility PARENT_COLUMN_VISIBILITY = new ColumnVisibility(PARENT_AUTH);
    private static final String PARENT_TOMCAT_URL = "http://rya-example-box:8080";

    private static final String CHILD_PASSWORD = AccumuloDualInstanceDriver.CHILD_PASSWORD;
    private static final String CHILD_INSTANCE = AccumuloDualInstanceDriver.CHILD_INSTANCE;
    private static final String CHILD_TABLE_PREFIX = AccumuloDualInstanceDriver.CHILD_TABLE_PREFIX;
    private static final String CHILD_AUTH = AccumuloDualInstanceDriver.CHILD_AUTH;
    private static final ColumnVisibility CHILD_COLUMN_VISIBILITY = new ColumnVisibility(CHILD_AUTH);
    private static final String CHILD_TOMCAT_URL = "http://localhost:8080";


    private static final long MERGE_FILE_TIME = 1463691894954L;
    private static final Date TODAY = USE_MERGE_FILE_INPUT ? new Date(MERGE_FILE_TIME) : new Date(System.currentTimeMillis());
    private static final Date YESTERDAY = TestUtils.dayBefore(TODAY);
    private static final Date LAST_MONTH = TestUtils.monthBefore(TODAY);

    private AccumuloRyaDAO parentDao;
    private AccumuloRyaDAO childDao;

    private AccumuloRdfConfiguration parentConfig;
    private AccumuloRdfConfiguration childConfig;

    private AccumuloDualInstanceDriver accumuloDualInstanceDriver;

    public static void main(final String args[]) {
        DemoUtilities.setupLogging(LOGGING_DETAIL);
        log.info("Setting up Merge Tool Demo");

        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(final Thread thread, final Throwable throwable) {
                log.fatal("Uncaught exception in " + thread.getName(), throwable);
            }
        });

        final MergeToolDemo mergeToolDemo = new MergeToolDemo();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                log.info("Shutting down...");
                try {
                    mergeToolDemo.tearDown();
                } catch (final Exception e) {
                    log.error("Error while shutting down", e);
                } finally {
                    log.info("Done shutting down");
                }
            }
        });

        try {
            mergeToolDemo.setUp();
            mergeToolDemo.testMergeTool();
        } catch (final Exception e) {
            log.error("Error while testing merge tool", e);
        } finally {
            try {
                mergeToolDemo.tearDown();
            } catch (final Exception e) {
                log.error("Error shutting down merge tool.", e);
            }
        }

        System.exit(0);
    }

    public void setUp() throws Exception {
        accumuloDualInstanceDriver = new AccumuloDualInstanceDriver(IS_MOCK, false, false, true, !USE_MERGE_FILE_INPUT);
        accumuloDualInstanceDriver.setUpInstances();

        accumuloDualInstanceDriver.setUpTables();

        accumuloDualInstanceDriver.setUpDaos();

        accumuloDualInstanceDriver.setUpConfigs();

        parentConfig = accumuloDualInstanceDriver.getParentConfig();
        childConfig = accumuloDualInstanceDriver.getChildConfig();
        parentDao = accumuloDualInstanceDriver.getParentDao();
        childDao = accumuloDualInstanceDriver.getChildDao();
    }

    public void tearDown() throws Exception {
        log.info("Tearing down...");
        accumuloDualInstanceDriver.tearDown();
    }

    private void mergeToolRun(final Date startDate) {
        MergeTool.setupAndRun(new String[] {
                makeArgument(MRUtils.AC_MOCK_PROP, Boolean.toString(IS_MOCK)),
                makeArgument(MRUtils.AC_INSTANCE_PROP, PARENT_INSTANCE),
                makeArgument(MRUtils.AC_USERNAME_PROP, accumuloDualInstanceDriver.getParentUser()),
                makeArgument(MRUtils.AC_PWD_PROP, PARENT_PASSWORD),
                makeArgument(MRUtils.TABLE_PREFIX_PROPERTY, PARENT_TABLE_PREFIX),
                makeArgument(MRUtils.AC_AUTH_PROP, PARENT_AUTH),
                makeArgument(MRUtils.AC_ZK_PROP, accumuloDualInstanceDriver.getParentZooKeepers()),
                makeArgument(CopyTool.PARENT_TOMCAT_URL_PROP, PARENT_TOMCAT_URL),
                makeArgument(MRUtils.AC_MOCK_PROP + CHILD_SUFFIX, Boolean.toString(IS_MOCK)),
                makeArgument(MRUtils.AC_INSTANCE_PROP + CHILD_SUFFIX, CHILD_INSTANCE),
                makeArgument(MRUtils.AC_USERNAME_PROP + CHILD_SUFFIX, accumuloDualInstanceDriver.getChildUser()),
                makeArgument(MRUtils.AC_PWD_PROP + CHILD_SUFFIX, CHILD_PASSWORD),
                makeArgument(MRUtils.TABLE_PREFIX_PROPERTY + CHILD_SUFFIX, CHILD_TABLE_PREFIX),
                makeArgument(MRUtils.AC_AUTH_PROP + CHILD_SUFFIX, CHILD_AUTH),
                makeArgument(MRUtils.AC_ZK_PROP + CHILD_SUFFIX, accumuloDualInstanceDriver.getChildZooKeepers()),
                makeArgument(CopyTool.CHILD_TOMCAT_URL_PROP, CHILD_TOMCAT_URL),
                makeArgument(CopyTool.NTP_SERVER_HOST_PROP, TimeUtils.DEFAULT_TIME_SERVER_HOST),
                makeArgument(CopyTool.USE_NTP_SERVER_PROP, Boolean.toString(USE_TIME_SYNC)),
                makeArgument(MergeTool.USE_MERGE_FILE_INPUT, Boolean.toString(USE_MERGE_FILE_INPUT)),
                makeArgument(MergeTool.MERGE_FILE_INPUT_PATH, "resources/test/merge_tool_file_input/"),
                makeArgument(MergeTool.START_TIME_PROP, MergeTool.getStartTimeString(startDate, IS_START_TIME_DIALOG_ENABLED))
        });

        log.info("Finished running tool.");
    }

    public void testMergeTool() throws Exception {
        log.info("");
        final StringBuilder sb = new StringBuilder();
        sb.append("Cases to check\n");
        sb.append("\n");
        sb.append("| **case** | **Parent** | **Child** | **assume that** | **merge modification** | **in parent after** |\n");
        sb.append("|----------|------------|-----------|-----------------|------------------------|---------------------|\n");
        sb.append("| 1        | older      | missing   | child deleted   | delete from parent     | no                  |\n");
        sb.append("| 2        | newer      | missing   | parent added    | do nothing             | yes                 |\n");
        sb.append("| 3        | missing    | older     | parent deleted  | do nothing             | no                  |\n");
        sb.append("| 4        | missing    | newer     | child added     | add to parent          | yes                 |\n");
        sb.append("| 5x       | older      | older     | same key        | do nothing             | yes                 |\n");
        sb.append("| 6x       | newer      | newer     | same key        | do nothing             | yes                 |\n");
        sb.append("| 7*       | older      | newer     | child updated   | do cases 1 and 4       | yes                 |\n");
        sb.append("| 8*       | newer      | older     | parent updated  | do cases 2 and 3       | yes                 |\n");
        sb.append("\n");
        sb.append("x - The two cases having the same key and timestamp are already merged.\n");
        sb.append("* - The last two cases are really two distinct keys, since the timestamp is part of the Accumulo key. They are handled as two comparisons. They are different versions of the same key.");
        sb.append("\n");
        log.info(sb.toString());

        log.info("Additional Case 9 to combine column visibilities of same rows.\n");

        log.info("Initial state of parent and child SPO tables...");

        DemoUtilities.promptEnterKey(IS_PROMPTING_ENABLED);

        // This statement was in both parent/child instances a month ago but after the start time of yesterday
        // the child deleted it and the parent still has it.  It should be deleted from the parent after merging.
        final RyaStatement ryaStatementCase1 = createRyaStatement("c1", "parent older", "child missing", LAST_MONTH);

        // This statement was added by the parent after the start time of yesterday and doesn't exist in the child.
        // It should stay in the parent after merging.
        final RyaStatement ryaStatementCase2 = createRyaStatement("c2", "parent newer", "child missing", TODAY);

        // This statement was in both parent/child instances a month ago but after the start time of yesterday
        // the parent deleted it and the child still has it.  It should stay deleted in the parent after merging.
        final RyaStatement ryaStatementCase3 = createRyaStatement("c3", "parent missing", "child older", LAST_MONTH);

        // This statement was added by the child after the start time of yesterday and doesn't exist in the parent.
        // It should be added to the parent after merging.
        final RyaStatement ryaStatementCase4 = createRyaStatement("c4", "parent missing", "child newer", TODAY);

        // This statement was in both parent/child instances a month ago and is before the start time of yesterday
        // but it was left alone.  It should remain in the parent after merging.
        final RyaStatement ryaStatementCase5 = createRyaStatement("c5", "parent older", "child older", LAST_MONTH);

        // This statement was added by the parent and child after the start time of yesterday.
        // It should remain in the parent after merging.
        final RyaStatement ryaStatementCase6 = createRyaStatement("c6", "parent newer", "child newer", TODAY);

        // This statement was modified by the child after the start time of yesterday.
        // It should deleted and then re-added to the parent with the new child data after merging.
        final RyaStatement ryaStatementCase7 = createRyaStatement("c7", "parent older", "child newer", LAST_MONTH);

        // This statement was modified by the parent after the start time of yesterday.
        // It should deleted and then re-added to the parent with the new child data after merging.
        final RyaStatement ryaStatementCase8 = createRyaStatement("c8", "parent newer", "child older", LAST_MONTH);

        // This statement was modified by the child to change the column visibility.
        // The parent should combine the child's visibility with its visibility.
        final RyaStatement ryaStatementVisibilityDifferent = createRyaStatement("c9", "I see", "you", LAST_MONTH);
        ryaStatementVisibilityDifferent.setColumnVisibility(PARENT_COLUMN_VISIBILITY.getExpression());

        // Change statements that were updated by the parent or child after the start time.
        final RyaStatement ryaStatementCase7Updated = TestUtils.copyRyaStatement(ryaStatementCase7);
        ryaStatementCase7Updated.setSubject(TestUtils.createRyaUri("c7 BOB"));
        ryaStatementCase7Updated.setTimestamp(TODAY.getTime());

        final RyaStatement ryaStatementCase8Updated = TestUtils.copyRyaStatement(ryaStatementCase8);
        ryaStatementCase8Updated.setSubject(TestUtils.createRyaUri("c8 SUSAN"));
        ryaStatementCase8Updated.setTimestamp(TODAY.getTime());

        final RyaStatement ryaStatementVisibilityDifferentUpdated = TestUtils.copyRyaStatement(ryaStatementVisibilityDifferent);
        ryaStatementVisibilityDifferentUpdated.setColumnVisibility(CHILD_COLUMN_VISIBILITY.getExpression());

        // Setup initial parent instance with 7 rows
        // This is the state of the parent data (as it is today) before merging occurs which will use the specified start time of yesterday.
        parentDao.add(ryaStatementVisibilityDifferent); // Merging should update statement
        parentDao.add(ryaStatementCase1);        // Merging should delete statement
        parentDao.add(ryaStatementCase2);        // Merging should keep statement
        parentDao.add(ryaStatementCase5);        // Merging should keep statement
        parentDao.add(ryaStatementCase6);        // Merging should keep statement
        parentDao.add(ryaStatementCase7);        // Merging should update statement
        parentDao.add(ryaStatementCase8Updated); // Merging should keep statement

        ryaStatementVisibilityDifferent.setColumnVisibility(CHILD_COLUMN_VISIBILITY.getExpression());

        if (!USE_MERGE_FILE_INPUT) {
            // Simulate the child coming back with a modified data set before the merging occurs.
            // There should be 7 rows in the child instance.
            childDao.add(ryaStatementVisibilityDifferentUpdated);
            childDao.add(ryaStatementCase3);
            childDao.add(ryaStatementCase4);
            childDao.add(ryaStatementCase5);
            childDao.add(ryaStatementCase6);         // Merging should add statement
            childDao.add(ryaStatementCase7Updated);
            childDao.add(ryaStatementCase8);         // Merging should add statement

            // Create 5 hour 5 minute  5 second and 5 millisecond offset metadata key
            final Long timeOffset = TimeUnit.HOURS.toMillis(5) + TimeUnit.MINUTES.toMillis(5) + TimeUnit.SECONDS.toMillis(5) + TimeUnit.MILLISECONDS.toMillis(5);
            log.info("Creating parent time offset of: " + TimeUtils.getDurationBreakdown(timeOffset));
            AccumuloRyaUtils.setTimeOffset(timeOffset, childDao);
        }

        log.info("7 rows in parent before merging.");

        AccumuloRyaUtils.printTablePretty(PARENT_TABLE_PREFIX + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX, parentConfig);
        if (!USE_MERGE_FILE_INPUT) {
            AccumuloRyaUtils.printTablePretty(CHILD_TABLE_PREFIX + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX, childConfig);
        }

        DemoUtilities.promptEnterKey(IS_PROMPTING_ENABLED);

        log.info("Starting merge tool. Merging all data after the specified start time: " + YESTERDAY);

        mergeToolRun(YESTERDAY);


//        for (String tableSuffix : AccumuloDualInstanceDriver.TABLE_NAME_SUFFIXES) {
//            AccumuloRyaUtils.printTablePretty(PARENT_TABLE_PREFIX + tableSuffix, parentConfig);
//        }

        AccumuloRyaUtils.printTablePretty(PARENT_TABLE_PREFIX + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX, parentConfig);


        final Scanner scanner = AccumuloRyaUtils.getScanner(PARENT_TABLE_PREFIX + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX, parentConfig);
        final Iterator<Entry<Key, Value>> iterator = scanner.iterator();
        int count = 0;
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        // Make sure we have all of them in the parent.
        log.info("Total rows in parent: " + count);

        log.info("Demo done");
    }
}
