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

import static org.apache.rya.accumulo.mr.merge.util.TestUtils.LAST_MONTH;
import static org.apache.rya.accumulo.mr.merge.util.TestUtils.TODAY;
import static org.apache.rya.accumulo.mr.merge.util.TestUtils.YESTERDAY;
import static org.apache.rya.accumulo.mr.merge.util.TestUtils.createRyaStatement;
import static org.apache.rya.accumulo.mr.merge.util.ToolConfigUtils.makeArgument;

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.rfile.bcfile.Compression.Algorithm;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.accumulo.mr.MRUtils;
import org.apache.rya.accumulo.mr.merge.CopyTool;
import org.apache.rya.accumulo.mr.merge.MergeTool;
import org.apache.rya.accumulo.mr.merge.common.InstanceType;
import org.apache.rya.accumulo.mr.merge.demo.util.DemoUtilities;
import org.apache.rya.accumulo.mr.merge.demo.util.DemoUtilities.LoggingDetail;
import org.apache.rya.accumulo.mr.merge.driver.AccumuloDualInstanceDriver;
import org.apache.rya.accumulo.mr.merge.util.AccumuloRyaUtils;
import org.apache.rya.accumulo.mr.merge.util.TimeUtils;
import org.apache.rya.api.RdfCloudTripleStoreConstants;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.indexing.accumulo.ConfigUtils;

/**
 * Tests for {@link CopyTool}.
 */
public class CopyToolDemo {
    private static final Logger log = Logger.getLogger(CopyToolDemo.class);

    private static final boolean IS_MOCK = true;
    private static final boolean USE_TIME_SYNC = false;
    private static final boolean USE_COPY_FILE_OUTPUT = false;
    private static final boolean USE_COPY_FILE_IMPORT = false;
    private static final boolean IS_PROMPTING_ENABLED = true;
    private static final boolean IS_START_TIME_DIALOG_ENABLED = true;
    private static final LoggingDetail LOGGING_DETAIL = LoggingDetail.LIGHT;

    private static final String CHILD_SUFFIX = MergeTool.CHILD_SUFFIX;

    private static final String PARENT_PASSWORD = AccumuloDualInstanceDriver.PARENT_PASSWORD;
    private static final String PARENT_INSTANCE = AccumuloDualInstanceDriver.PARENT_INSTANCE;
    private static final String PARENT_TABLE_PREFIX = AccumuloDualInstanceDriver.PARENT_TABLE_PREFIX;
    private static final String PARENT_AUTH = AccumuloDualInstanceDriver.PARENT_AUTH;
    private static final String PARENT_TOMCAT_URL = "http://rya-example-box:8080";

    private static final String CHILD_PASSWORD = AccumuloDualInstanceDriver.CHILD_PASSWORD;
    private static final String CHILD_INSTANCE = AccumuloDualInstanceDriver.CHILD_INSTANCE;
    private static final String CHILD_TABLE_PREFIX = AccumuloDualInstanceDriver.CHILD_TABLE_PREFIX;
    private static final String CHILD_TOMCAT_URL = "http://localhost:8080";

    private AccumuloRyaDAO parentDao;

    private AccumuloRdfConfiguration parentConfig;
    private AccumuloRdfConfiguration childConfig;

    private AccumuloDualInstanceDriver accumuloDualInstanceDriver;
    private CopyTool copyTool = null;

    public static void main(final String args[]) {
        DemoUtilities.setupLogging(LOGGING_DETAIL);
        log.info("Setting up Copy Tool Demo");

        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(final Thread thread, final Throwable throwable) {
                log.fatal("Uncaught exception in " + thread.getName(), throwable);
            }
        });

        final CopyToolDemo copyToolDemo = new CopyToolDemo();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                log.info("Shutting down...");
                try {
                    copyToolDemo.tearDown();
                } catch (final Exception e) {
                    log.error("Error while shutting down", e);
                } finally {
                    log.info("Done shutting down");
                }
            }
        });

        try {
            copyToolDemo.setUp();
            copyToolDemo.testCopyTool();
        } catch (final Exception e) {
            log.error("Error while testing copy tool", e);
        } finally {
            try {
                copyToolDemo.tearDown();
            } catch (final Exception e) {
                log.error("Error shutting down copy tool", e);
            }
        }

        System.exit(0);
    }

    public void setUp() throws Exception {
        accumuloDualInstanceDriver = new AccumuloDualInstanceDriver(IS_MOCK, true, true, false, false);
        accumuloDualInstanceDriver.setUpInstances();

        accumuloDualInstanceDriver.setUpTables();

        accumuloDualInstanceDriver.setUpDaos();

        accumuloDualInstanceDriver.setUpConfigs();

        parentConfig = accumuloDualInstanceDriver.getParentConfig();
        childConfig = accumuloDualInstanceDriver.getChildConfig();
        parentDao = accumuloDualInstanceDriver.getParentDao();
    }

    public void tearDown() throws Exception {
        log.info("Tearing down...");
        accumuloDualInstanceDriver.tearDown();
        if (copyTool != null) {
            copyTool.shutdown();
        }
    }

    private void copyToolRun(final Date startDate) throws AccumuloException, AccumuloSecurityException {
        copyTool = new CopyTool();
        copyTool.setupAndRun(new String[] {
                makeArgument(MRUtils.AC_MOCK_PROP, Boolean.toString(IS_MOCK)),
                makeArgument(MRUtils.AC_INSTANCE_PROP, PARENT_INSTANCE),
                makeArgument(MRUtils.AC_USERNAME_PROP, accumuloDualInstanceDriver.getParentUser()),
                makeArgument(MRUtils.AC_PWD_PROP, PARENT_PASSWORD),
                makeArgument(MRUtils.TABLE_PREFIX_PROPERTY, PARENT_TABLE_PREFIX),
                makeArgument(MRUtils.AC_AUTH_PROP, accumuloDualInstanceDriver.getParentAuths().toString()),
                makeArgument(MRUtils.AC_ZK_PROP, accumuloDualInstanceDriver.getParentZooKeepers()),
                makeArgument(CopyTool.PARENT_TOMCAT_URL_PROP, PARENT_TOMCAT_URL),
                makeArgument(MRUtils.AC_MOCK_PROP + CHILD_SUFFIX, Boolean.toString(IS_MOCK)),
                makeArgument(MRUtils.AC_INSTANCE_PROP + CHILD_SUFFIX, CHILD_INSTANCE),
                makeArgument(MRUtils.AC_USERNAME_PROP + CHILD_SUFFIX, accumuloDualInstanceDriver.getChildUser()),
                makeArgument(MRUtils.AC_PWD_PROP + CHILD_SUFFIX, CHILD_PASSWORD),
                makeArgument(MRUtils.TABLE_PREFIX_PROPERTY + CHILD_SUFFIX, CHILD_TABLE_PREFIX),
                makeArgument(MRUtils.AC_AUTH_PROP + CHILD_SUFFIX, accumuloDualInstanceDriver.getChildAuths() != null ? accumuloDualInstanceDriver.getChildAuths().toString() : null),
                makeArgument(MRUtils.AC_ZK_PROP + CHILD_SUFFIX, accumuloDualInstanceDriver.getChildZooKeepers() != null ? accumuloDualInstanceDriver.getChildZooKeepers() : "localhost"),
                makeArgument(CopyTool.CHILD_TOMCAT_URL_PROP, CHILD_TOMCAT_URL),
                makeArgument(CopyTool.COPY_TABLE_LIST_PROP, !USE_COPY_FILE_IMPORT ? PARENT_TABLE_PREFIX + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX : ""),
                makeArgument(CopyTool.CREATE_CHILD_INSTANCE_TYPE_PROP, (IS_MOCK ? InstanceType.MOCK : InstanceType.MINI).toString()),
                makeArgument(CopyTool.NTP_SERVER_HOST_PROP, TimeUtils.DEFAULT_TIME_SERVER_HOST),
                makeArgument(CopyTool.USE_NTP_SERVER_PROP, Boolean.toString(USE_TIME_SYNC)),
                makeArgument(CopyTool.USE_COPY_FILE_OUTPUT, Boolean.toString(USE_COPY_FILE_OUTPUT)),
                makeArgument(CopyTool.COPY_FILE_OUTPUT_PATH, "/test/copy_tool_file_output/"),
                makeArgument(CopyTool.COPY_FILE_OUTPUT_COMPRESSION_TYPE, Algorithm.GZ.getName()),
                makeArgument(CopyTool.USE_COPY_FILE_OUTPUT_DIRECTORY_CLEAR, Boolean.toString(true)),
                makeArgument(CopyTool.COPY_FILE_IMPORT_DIRECTORY, "resources/test/copy_tool_file_output/"),
                makeArgument(CopyTool.USE_COPY_FILE_IMPORT, Boolean.toString(USE_COPY_FILE_IMPORT)),
                //makeArgument(CopyTool.COPY_TABLE_LIST_PROP, Joiner.on(",").join(accumuloDualInstanceDriver.getParentTableList())),
                makeArgument(MergeTool.START_TIME_PROP, MergeTool.getStartTimeString(startDate, IS_START_TIME_DIALOG_ENABLED))
        });

        final Configuration toolConfig = copyTool.getConf();
        final String zooKeepers = toolConfig.get(MRUtils.AC_ZK_PROP + CHILD_SUFFIX);
        MergeTool.setDuplicateKeysForProperty(childConfig, MRUtils.AC_ZK_PROP, zooKeepers);

        if (USE_COPY_FILE_OUTPUT) {
            // Set up the child tables now to test importing the files back into the child instance
            final String childTableName = CHILD_TABLE_PREFIX + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX;
            try {
                copyTool.createTableIfNeeded(childTableName);
                copyTool.importFilesToChildTable(childTableName);
            } catch (final Exception e) {
                log.error("Failed to import files into child instance.", e);
            }
        }

        log.info("Finished running tool.");
    }

    public void testCopyTool() throws Exception {
        log.info("");
        log.info("Setting up initial state of parent before copying to child...");
        log.info("Adding data to parent...");

        final int numRowsNotToCopy = 80;
        final int numRowsToCopy = 20;

        // Create Rya Statement before last month which won't be copied
        final Random random = new Random();

        for (int i = 1; i <= numRowsNotToCopy; i++) {
            final long randTimeBeforeLastMonth = DemoUtilities.randLong(0, LAST_MONTH.getTime());
            final String randVis = random.nextBoolean() ? PARENT_AUTH : "";
            final RyaStatement ryaStatementOutOfTimeRange = createRyaStatement("Nobody", "sees", "me " + i, new Date(randTimeBeforeLastMonth));
            ryaStatementOutOfTimeRange.setColumnVisibility(randVis.getBytes());
            parentDao.add(ryaStatementOutOfTimeRange);
        }

        for (int i = 1; i <= numRowsToCopy; i++) {
            final long randTimeAfterYesterdayAndBeforeToday = DemoUtilities.randLong(YESTERDAY.getTime(), TODAY.getTime());
            final String randVis = random.nextBoolean() ? PARENT_AUTH : "";
            final RyaStatement ryaStatementShouldCopy = createRyaStatement("bob", "copies", "susan " + i, new Date(randTimeAfterYesterdayAndBeforeToday));
            ryaStatementShouldCopy.setColumnVisibility(randVis.getBytes());
            parentDao.add(ryaStatementShouldCopy);
        }

        if (USE_COPY_FILE_OUTPUT) {
            // Set up table splits
            final SortedSet<Text> splits = new TreeSet<>();
            for (char alphabet = 'a'; alphabet <= 'e'; alphabet++) {
                final Text letter = new Text(alphabet + "");
                splits.add(letter);
            }
            parentDao.getConnector().tableOperations().addSplits(PARENT_TABLE_PREFIX + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX, splits);
        }

        log.info("Added " +  (numRowsNotToCopy + numRowsToCopy) + " rows to parent SPO table.");
        log.info("Parent SPO table output below:");
        DemoUtilities.promptEnterKey(IS_PROMPTING_ENABLED);


        AccumuloRyaUtils.printTablePretty(PARENT_TABLE_PREFIX + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX, parentConfig);
        //AccumuloRyaUtils.printTablePretty(CHILD_TABLE_PREFIX + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX, childConfig);

        log.info("");
        log.info("Total Rows in table: " + (numRowsNotToCopy + numRowsToCopy));
        log.info("Number of Rows NOT to copy (out of time range): " + numRowsNotToCopy);
        log.info("Number of Rows to copy (in time range): " + numRowsToCopy);
        log.info("");

        DemoUtilities.promptEnterKey(IS_PROMPTING_ENABLED);

        log.info("Starting copy tool. Copying all data after the specified start time: " + YESTERDAY);
        log.info("");

        copyToolRun(YESTERDAY);


        // Copy Tool made child instance so hook the tables and dao into the driver.
        final String childUser = accumuloDualInstanceDriver.getChildUser();
        final Connector childConnector = ConfigUtils.getConnector(childConfig);
        accumuloDualInstanceDriver.getChildAccumuloInstanceDriver().setConnector(childConnector);

        accumuloDualInstanceDriver.getChildAccumuloInstanceDriver().setUpTables();

        accumuloDualInstanceDriver.getChildAccumuloInstanceDriver().setUpDao();


        // Update child config to include changes made from copy process
        final SecurityOperations childSecOps = accumuloDualInstanceDriver.getChildSecOps();
        final Authorizations newChildAuths = AccumuloRyaUtils.addUserAuths(childUser, childSecOps, PARENT_AUTH);
        childSecOps.changeUserAuthorizations(childUser, newChildAuths);
        final String childAuthString = newChildAuths.toString();
        final List<String> duplicateKeys = MergeTool.DUPLICATE_KEY_MAP.get(MRUtils.AC_AUTH_PROP);
        childConfig.set(MRUtils.AC_AUTH_PROP, childAuthString);
        for (final String key : duplicateKeys) {
            childConfig.set(key, childAuthString);
        }


        //AccumuloRyaUtils.printTablePretty(CHILD_TABLE_PREFIX + RdfCloudTripleStoreConstants.TBL_PO_SUFFIX, childConfig);
        //AccumuloRyaUtils.printTablePretty(CHILD_TABLE_PREFIX + RdfCloudTripleStoreConstants.TBL_OSP_SUFFIX, childConfig);
        AccumuloRyaUtils.printTablePretty(CHILD_TABLE_PREFIX + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX, childConfig);

        final Scanner scanner = AccumuloRyaUtils.getScanner(CHILD_TABLE_PREFIX + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX, childConfig);
        final Iterator<Entry<Key, Value>> iterator = scanner.iterator();
        int count = 0;
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        log.info("");
        log.info("Total rows copied: " + count);
        log.info("");

        log.info("Demo done");
    }
}
