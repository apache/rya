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
package org.apache.rya.accumulo.mr.merge;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.mapreduce.AbstractInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloMultiTableInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase;
import org.apache.accumulo.core.client.mapreduce.InputTableConfig;
import org.apache.accumulo.core.client.mapreduce.lib.partition.KeyRangePartitioner;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.rfile.bcfile.Compression.Algorithm;
import org.apache.accumulo.core.iterators.user.AgeOffFilter;
import org.apache.accumulo.core.iterators.user.TimestampFilter;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;

import com.google.common.base.Joiner;

import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.mr.AccumuloHDFSFileInputFormat;
import org.apache.rya.accumulo.mr.MRUtils;
import org.apache.rya.accumulo.mr.merge.common.InstanceType;
import org.apache.rya.accumulo.mr.merge.gui.DateTimePickerDialog;
import org.apache.rya.accumulo.mr.merge.mappers.AccumuloCopyToolMapper;
import org.apache.rya.accumulo.mr.merge.mappers.AccumuloRyaRuleMapper;
import org.apache.rya.accumulo.mr.merge.mappers.FileCopyToolMapper;
import org.apache.rya.accumulo.mr.merge.mappers.MergeToolMapper;
import org.apache.rya.accumulo.mr.merge.mappers.RowRuleMapper;
import org.apache.rya.accumulo.mr.merge.reducers.MultipleFileReducer;
import org.apache.rya.accumulo.mr.merge.util.AccumuloInstanceDriver;
import org.apache.rya.accumulo.mr.merge.util.AccumuloQueryRuleset;
import org.apache.rya.accumulo.mr.merge.util.AccumuloRyaUtils;
import org.apache.rya.accumulo.mr.merge.util.GroupedRow;
import org.apache.rya.accumulo.mr.merge.util.TimeUtils;
import org.apache.rya.accumulo.mr.merge.util.ToolConfigUtils;
import org.apache.rya.api.RdfCloudTripleStoreConstants;
import org.apache.rya.api.RdfCloudTripleStoreUtils;
import org.apache.rya.api.layout.TablePrefixLayoutStrategy;
import org.apache.rya.indexing.accumulo.ConfigUtils;

/**
 * Handles copying data from a parent instance into a child instance.
 */
public class CopyTool extends AbstractDualInstanceAccumuloMRTool {
    private static final Logger log = Logger.getLogger(CopyTool.class);

    /**
     * Use this property to set the tables that are going to be copied.  The list should
     * be a comma-separated string containing the full table names.  If not set, then all
     * tables will be copied.
     */
    public static final String COPY_TABLE_LIST_PROP = "copy.table.list";

    /**
     * Indicates the type of child instance to create.  {@code null} or empty to not create an
     * instance indicating that it already was created and exists.
     */
    public static final String CREATE_CHILD_INSTANCE_TYPE_PROP = "create.child.instance.type";

    /**
     * The time difference between the parent machine and the time server.
     */
    public static final String PARENT_TIME_OFFSET_PROP = "time.offset";

    /**
     * The time difference between the child machine and the time server.
     */
    public static final String CHILD_TIME_OFFSET_PROP = "time.offset.child";

    /**
     * The host name of the time server to use.
     */
    public static final String NTP_SERVER_HOST_PROP = "ntp.server.host";

    /**
     * The URL of the Apache Tomcat server web page running on the parent machine.
     */
    public static final String PARENT_TOMCAT_URL_PROP = "tomcat.url";

    /**
     * The URL of the Apache Tomcat server web page running on the child machine.
     */
    public static final String CHILD_TOMCAT_URL_PROP = "tomcat.url.child";

    /**
     * The run time of the copy process.
     */
    public static final String COPY_RUN_TIME_PROP = "copy.run.time";

    /**
     * "true" to use the NTP server to handle time synchronization.
     * "false" (or any other value) to not use the NTP server.
     */
    public static final String USE_NTP_SERVER_PROP = "use.ntp.server";

    /**
     * "true" to use file output. "false" to use Accumulo output.
     */
    public static final String USE_COPY_FILE_OUTPUT = "use.copy.file.output";

    /**
     * The file path to output the child data to.
     */
    public static final String COPY_FILE_OUTPUT_PATH = "copy.file.output.path";

    /**
     * The compression type to use for file output.  One of "none", "gz", "lzo", or "snappy".
     */
    public static final String COPY_FILE_OUTPUT_COMPRESSION_TYPE = "copy.file.output.compression.type";

    /**
     * "true" to clear the file output directory before copying. "false" to leave the output directory alone.
     */
    public static final String USE_COPY_FILE_OUTPUT_DIRECTORY_CLEAR = "use.copy.file.output.directory.clear";

    /**
     * The input directory for importing files into accumulo tables.
     */
    public static final String COPY_FILE_IMPORT_DIRECTORY = "copy.file.import.directory";

    /**
     * "true" to read from the input directory. "false" otherwise.
     */
    public static final String USE_COPY_FILE_IMPORT = "use.copy.file.import";

    /**
     * "true" to extract a set of rules from a SPARQL query, and only copy statements relevant to those rules. "false" otherwise.
     * If set, either the query itself or a query file should also be provided.
     */
    public static final String USE_COPY_QUERY_SPARQL = "use.copy.query.sparql";

    /**
     * The text of the query that defines which statements to copy.
     */
    public static final String QUERY_STRING_PROP = "ac.copy.query";

    /**
     * The path to a file containing the query that defines which statements to copy.
     */
    public static final String QUERY_FILE_PROP = "ac.copy.queryfile";

    private static final String DIALOG_TITLE = "Select a Start Time/Date";
    private static final String DIALOG_MESSAGE =
        "<html>Choose the time of the data to copy.<br>Only parent data AFTER the selected time will be copied to the child.</html>";

    private String startTime = null;
    private boolean useCopyFileOutput = false;
    private String baseOutputDir = null;
    private String localBaseOutputDir = null;
    private String compressionType = null;
    private boolean useCopyFileOutputDirectoryClear = false;
    private String tempDir = null;
    private boolean useCopyFileImport = false;
    private boolean useQuery = false;
    private String localCopyFileImportDir = null;
    private String baseImportDir = null;

    private final List<String> tables = new ArrayList<>();

    private AccumuloInstanceDriver childAccumuloInstanceDriver = null;

    /**
     * Sets up and initializes the copy tool's configuration.
     * @throws Exception
     */
    public void setup() throws Exception {
        super.init();

        tempDir = conf.get("hadoop.tmp.dir", null);
        if (tempDir == null) {
            throw new Exception("Invalid hadoop temp directory. \"hadoop.tmp.dir\" could not be found in the configuration.");
        }

        useCopyFileOutput = conf.getBoolean(USE_COPY_FILE_OUTPUT, false);
        baseOutputDir = tempDir + "/copy_tool_file_output/";
        localBaseOutputDir = conf.get(COPY_FILE_OUTPUT_PATH, null);
        compressionType = conf.get(COPY_FILE_OUTPUT_COMPRESSION_TYPE, null);
        useCopyFileOutputDirectoryClear = conf.getBoolean(USE_COPY_FILE_OUTPUT_DIRECTORY_CLEAR, false);
        localCopyFileImportDir = conf.get(COPY_FILE_IMPORT_DIRECTORY, null);
        baseImportDir = tempDir + "/copy_tool_import/";

        startTime = conf.get(MergeTool.START_TIME_PROP, null);

        if (!useCopyFileImport) {
            // Display start time dialog if requested
            if (MergeTool.USE_START_TIME_DIALOG.equals(startTime)) {
                log.info("Select start time from dialog...");
                final DateTimePickerDialog dateTimePickerDialog = new DateTimePickerDialog(DIALOG_TITLE, DIALOG_MESSAGE);
                dateTimePickerDialog.setVisible(true);

                final Date date = dateTimePickerDialog.getSelectedDateTime();
                startTime = MergeTool.START_TIME_FORMATTER.format(date);
                conf.set(MergeTool.START_TIME_PROP, startTime);
                log.info("Will copy all data after " + date);
            } else if (startTime != null) {
                try {
                    final Date date = MergeTool.START_TIME_FORMATTER.parse(startTime);
                    log.info("Will copy all data after " + date);
                } catch (final ParseException e) {
                    throw new Exception("Unable to parse the provided start time: " + startTime, e);
                }
            }

            Date copyRunTime = new Date();
            final boolean useTimeSync = conf.getBoolean(USE_NTP_SERVER_PROP, false);
            if (useTimeSync) {
                final String tomcatUrl = conf.get(PARENT_TOMCAT_URL_PROP, null);
                final String ntpServerHost = conf.get(NTP_SERVER_HOST_PROP, null);
                Long timeOffset = null;
                Date ntpDate = null;
                try {
                    log.info("Comparing parent machine's time to NTP server time...");
                    ntpDate = TimeUtils.getNtpServerDate(ntpServerHost);
                    final Date parentMachineDate = TimeUtils.getMachineDate(tomcatUrl);
                    final boolean isMachineLocal = TimeUtils.isUrlLocalMachine(tomcatUrl);
                    timeOffset = TimeUtils.getTimeDifference(ntpDate, parentMachineDate, isMachineLocal);
                } catch (IOException | ParseException e) {
                    throw new Exception("Unable to get time difference between machine and NTP server.", e);
                }
                if (timeOffset != null) {
                    conf.set(PARENT_TIME_OFFSET_PROP, "" + timeOffset);
                }
                copyRunTime = ntpDate;
            }
            final String copyRunTimeString = MergeTool.START_TIME_FORMATTER.format(copyRunTime);
            if (copyRunTime != null) {
                conf.set(COPY_RUN_TIME_PROP, copyRunTimeString);
            }
        }

        MergeTool.setDuplicateKeys(conf);

        final String copyTableListProperty = conf.get(COPY_TABLE_LIST_PROP);
        if (StringUtils.isNotBlank(copyTableListProperty)) {
            // Copy the tables specified in the config
            final String[] split = copyTableListProperty.split(",");
            tables.addAll(Arrays.asList(split));
        } else if (useCopyFileImport) {
            final File importDir = new File(localCopyFileImportDir);
            final String[] files = importDir.list();
            tables.addAll(Arrays.asList(files));
        } else {
            // By default copy all tables
            tables.add(tablePrefix + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX);
            tables.add(tablePrefix + RdfCloudTripleStoreConstants.TBL_OSP_SUFFIX);
            tables.add(tablePrefix + RdfCloudTripleStoreConstants.TBL_PO_SUFFIX);
            tables.add(tablePrefix + RdfCloudTripleStoreConstants.TBL_NS_SUFFIX);
            tables.add(tablePrefix + RdfCloudTripleStoreConstants.TBL_EVAL_SUFFIX);
            tables.add(tablePrefix + RdfCloudTripleStoreConstants.TBL_STATS_SUFFIX);
            tables.add(tablePrefix + RdfCloudTripleStoreConstants.TBL_SEL_SUFFIX);
            /* TODO: SEE RYA-160
            tables.add(ConfigUtils.getFreeTextDocTablename(conf));
            tables.add(ConfigUtils.getFreeTextTermTablename(conf));
            tables.add(ConfigUtils.getGeoTablename(conf));
            tables.add(ConfigUtils.getTemporalTableName(conf));
            tables.add(ConfigUtils.getEntityTableName(conf));
            */
        }
        if (tables.isEmpty()) {
            log.warn("No list of tables to copy was provided.");
        } else {
            final String tablesToCopy = Joiner.on("\r\n\t").join(tables);
            log.info("Will attempt to copy the following tables/indices from the parent:\r\n\t" + tablesToCopy);
        }
    }

    @Override
    public int run(final String[] strings) throws Exception {
        useCopyFileImport = conf.getBoolean(USE_COPY_FILE_IMPORT, false);
        useQuery = conf.getBoolean(USE_COPY_QUERY_SPARQL, false);

        if (useCopyFileImport) {
            return runImport();
        } else if (useQuery) {
            return runQueryCopy();
        } else {
            return runCopy();
        }
    }

    private int runCopy() throws Exception {
        log.info("Setting up Copy Tool...");

        setup();

        if (!useCopyFileOutput) {
            createChildInstance(conf);
        }

        final AccumuloRdfConfiguration parentAccumuloRdfConfiguration = new AccumuloRdfConfiguration(conf);
        parentAccumuloRdfConfiguration.setTablePrefix(tablePrefix);
        final Connector parentConnector = AccumuloRyaUtils.setupConnector(parentAccumuloRdfConfiguration);
        final TableOperations parentTableOperations = parentConnector.tableOperations();

        for (final String table : tables) {
            // Check if the parent table exists before creating a job on it
            if (parentTableOperations.exists(table)) {
                final String childTable = table.replaceFirst(tablePrefix, childTablePrefix);
                final String jobName = "Copy Tool, copying Parent Table: " + table + ", into Child Table: " + childTable + ", " + System.currentTimeMillis();
                log.info("Initializing job: " + jobName);
                conf.set(MRUtils.JOB_NAME_PROP, jobName);
                conf.set(MergeTool.TABLE_NAME_PROP, table);

                final Job job = Job.getInstance(conf);
                job.setJarByClass(CopyTool.class);

                setupAccumuloInput(job);

                InputFormatBase.setInputTableName(job, table);

                // Set input output of the particular job
                if (useCopyFileOutput) {
                    job.setMapOutputKeyClass(Key.class);
                    job.setMapOutputValueClass(Value.class);
                    job.setOutputKeyClass(Key.class);
                    job.setOutputValueClass(Value.class);
                } else {
                    job.setMapOutputKeyClass(Text.class);
                    job.setMapOutputValueClass(Mutation.class);
                    job.setOutputKeyClass(Text.class);
                    job.setOutputValueClass(Mutation.class);
                }

                setupAccumuloOutput(job, childTable);

                // Set mapper and reducer classes
                if (useCopyFileOutput) {
                    setupSplitsFile(job, parentTableOperations, table, childTable);
                    job.setMapperClass(FileCopyToolMapper.class);
                } else {
                    job.setMapperClass(AccumuloCopyToolMapper.class);
                }
                job.setReducerClass(Reducer.class);

                // Submit the job
                final Date beginTime = new Date();
                log.info("Job for table \"" + table + "\" started: " + beginTime);
                final int exitCode = job.waitForCompletion(true) ? 0 : 1;

                if (exitCode == 0) {
                    if (useCopyFileOutput) {
                        log.info("Moving data from HDFS to the local file system for the table: " + childTable);
                        final Path hdfsPath = getPath(baseOutputDir, childTable);
                        final Path localPath = getPath(localBaseOutputDir, childTable);
                        log.info("HDFS directory: " + hdfsPath.toString());
                        log.info("Local directory: " + localPath.toString());
                        copyHdfsToLocal(hdfsPath, localPath);
                    }

                    final Date endTime = new Date();
                    log.info("Job for table \"" + table + "\" finished: " + endTime);
                    log.info("The job took " + (endTime.getTime() - beginTime.getTime()) / 1000 + " seconds.");
                } else {
                    log.error("Job for table \"" + table + "\" Failed!!!");
                    return exitCode;
                }
            } else {
                log.warn("The table \"" + table + "\" was NOT found in the parent instance and cannot be copied.");
            }
        }

        return 0;
    }

    private int runImport() throws Exception {
        log.info("Setting up Copy Tool for importing...");

        setup();

        createChildInstance(conf);

        for (final String childTable : tables) {
            final String jobName = "Copy Tool, importing Exported Parent Table files from: " + getPath(localCopyFileImportDir, childTable).toString() + ", into Child Table: " + childTable + ", " + System.currentTimeMillis();
            log.info("Initializing job: " + jobName);
            conf.set(MRUtils.JOB_NAME_PROP, jobName);

            // Submit the job
            final Date beginTime = new Date();
            log.info("Job for table \"" + childTable + "\" started: " + beginTime);

            createTableIfNeeded(childTable);
            importFilesToChildTable(childTable);

            final Date endTime = new Date();
            log.info("Job for table \"" + childTable + "\" finished: " + endTime);
            log.info("The job took " + (endTime.getTime() - beginTime.getTime()) / 1000 + " seconds.");
        }

        return 0;
    }

    private int runQueryCopy() throws Exception {
        log.info("Setting up Copy Tool with a query-based ruleset...");
        setup();
        if (!useCopyFileOutput) {
            createChildInstance(conf);
        }

        // Set up the configuration
        final AccumuloRdfConfiguration aconf = new AccumuloRdfConfiguration(conf);
        aconf.setBoolean(ConfigUtils.USE_MOCK_INSTANCE, mock);
        aconf.setTablePrefix(tablePrefix);
        aconf.setFlush(false);
        ConfigUtils.setIndexers(aconf);

        // Since we're copying at the statement-level, ignore any given list of tables and determine
        // which tables we might need to create based on which indexers are desired.
        final TablePrefixLayoutStrategy prefixStrategy = new TablePrefixLayoutStrategy(tablePrefix);
        tables.clear();
        // Always include core tables
        tables.add(prefixStrategy.getSpo());
        tables.add(prefixStrategy.getOsp());
        tables.add(prefixStrategy.getPo());
        // Copy namespaces if they exist
        tables.add(prefixStrategy.getNs());
        // Add tables associated with any configured indexers
        /* TODO: SEE RYA-160
        if (aconf.getBoolean(ConfigUtils.USE_FREETEXT, false)) {
            tables.add(ConfigUtils.getFreeTextDocTablename(conf));
            tables.add(ConfigUtils.getFreeTextTermTablename(conf));
        }
        if (aconf.getBoolean(ConfigUtils.USE_GEO, false)) {
            tables.add(ConfigUtils.getGeoTablename(conf));
        }
        if (aconf.getBoolean(ConfigUtils.USE_TEMPORAL, false)) {
            tables.add(ConfigUtils.getTemporalTableName(conf));
        }
        if (aconf.getBoolean(ConfigUtils.USE_ENTITY, false)) {
            tables.add(ConfigUtils.getEntityTableName(conf));
        }
        */
        // Ignore anything else, e.g. statistics -- must be recalculated for the child if desired

        // Extract the ruleset, and copy the namespace table directly
        final AccumuloQueryRuleset ruleset = new AccumuloQueryRuleset(aconf);
        ruleset.addTable(prefixStrategy.getNs());
        for (final String line : ruleset.toString().split("\n")) {
            log.info(line);
        }

        // Create a Job and configure its input and output
        final Job job = Job.getInstance(aconf);
        job.setJarByClass(this.getClass());
        setupMultiTableInputFormat(job, ruleset);
        setupAccumuloOutput(job, "");

        if (useCopyFileOutput) {
            // Configure job for file output
            job.setJobName("Ruleset-based export to file: " + tablePrefix + " -> " + localBaseOutputDir);
            // Map (row) to (table+key, key+value)
            job.setMapperClass(RowRuleMapper.class);
            job.setMapOutputKeyClass(GroupedRow.class);
            job.setMapOutputValueClass(GroupedRow.class);
            // Group according to table and and sort according to key
            job.setGroupingComparatorClass(GroupedRow.GroupComparator.class);
            job.setSortComparatorClass(GroupedRow.SortComparator.class);
            // Reduce ([table+row], rows): output each row to the file for that table, in sorted order
            job.setReducerClass(MultipleFileReducer.class);
            job.setOutputKeyClass(Key.class);
            job.setOutputValueClass(Value.class);
        }
        else {
            // Configure job for table output
            job.setJobName("Ruleset-based copy: " + tablePrefix + " -> " + childTablePrefix);
            // Map (row): convert to statement, insert to child (for namespace table, output row directly)
            job.setMapperClass(AccumuloRyaRuleMapper.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Mutation.class);
            job.setNumReduceTasks(0);
            // Create the child tables, so mappers don't try to do this in parallel
            for (final String parentTable : tables) {
                final String childTable = parentTable.replaceFirst(tablePrefix, childTablePrefix);
                createTableIfNeeded(childTable);
            }
        }

        // Run the job and copy files to local filesystem if needed
        final Date beginTime = new Date();
        log.info("Job started: " + beginTime);
        final boolean success = job.waitForCompletion(true);
        if (success) {
            if (useCopyFileOutput) {
                log.info("Moving data from HDFS to the local file system");
                final Path baseOutputPath = new Path(baseOutputDir);
                for (final FileStatus status : FileSystem.get(conf).listStatus(baseOutputPath)) {
                    if (status.isDirectory()) {
                        final String tableName = status.getPath().getName();
                        final Path hdfsPath = getPath(baseOutputDir, tableName);
                        final Path localPath = getPath(localBaseOutputDir, tableName);
                        log.info("HDFS directory: " + hdfsPath.toString());
                        log.info("Local directory: " + localPath.toString());
                        copyHdfsToLocal(hdfsPath, localPath);
                    }
                }
            }
            final Date endTime = new Date();
            log.info("Job finished: " + endTime);
            log.info("The job took " + (endTime.getTime() - beginTime.getTime()) / 1000 + " seconds.");
            return 0;
        } else {
            log.error("Job failed!!!");
            return 1;
        }
    }

    /**
     * Creates the child table if it doesn't already exist.
     * @param childTableName the name of the child table.
     * @throws IOException
     */
    public void createTableIfNeeded(final String childTableName) throws IOException {
        try {
            final Configuration childConfig = MergeToolMapper.getChildConfig(conf);
            final AccumuloRdfConfiguration childAccumuloRdfConfiguration = new AccumuloRdfConfiguration(childConfig);
            childAccumuloRdfConfiguration.setTablePrefix(childTablePrefix);
            final Connector childConnector = AccumuloRyaUtils.setupConnector(childAccumuloRdfConfiguration);
            if (!childConnector.tableOperations().exists(childTableName)) {
                log.info("Creating table: " + childTableName);
                childConnector.tableOperations().create(childTableName);
                log.info("Created table: " + childTableName);
                log.info("Granting authorizations to table: " + childTableName);
                childConnector.securityOperations().grantTablePermission(childUserName, childTableName, TablePermission.WRITE);
                log.info("Granted authorizations to table: " + childTableName);
            }
        } catch (TableExistsException | AccumuloException | AccumuloSecurityException e) {
            throw new IOException(e);
        }
    }

    private void setupSplitsFile(final Job job, final TableOperations parentTableOperations, final String parentTableName, final String childTableName) throws Exception {
        final FileSystem fs = FileSystem.get(conf);
        fs.setPermission(getPath(baseOutputDir, childTableName), new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
        final Path splitsPath = getPath(baseOutputDir, childTableName, "splits.txt");
        final Collection<Text> splits = parentTableOperations.listSplits(parentTableName, 100);
        log.info("Creating splits file at: " + splitsPath);
        try (PrintStream out = new PrintStream(new BufferedOutputStream(fs.create(splitsPath)))) {
            for (final Text split : splits) {
                final String encoded = new String(Base64.encodeBase64(TextUtil.getBytes(split)));
                out.println(encoded);
            }
        }
        fs.setPermission(splitsPath, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));

        final String userDir = System.getProperty("user.dir");
        // The splits file has a symlink created in the user directory for some reason.
        // It might be better to copy the entire file for Windows but it doesn't seem to matter if
        // the user directory symlink is broken.
        java.nio.file.Files.deleteIfExists(new File(userDir, "splits.txt").toPath());
        //Files.copy(new File(splitsPath.toString()), new File(userDir, "splits.txt"));
        job.setPartitionerClass(KeyRangePartitioner.class);
        KeyRangePartitioner.setSplitFile(job, splitsPath.toString());
        job.setNumReduceTasks(splits.size() + 1);
    }

    /**
     * Converts a path string, or a sequence of strings that when joined form a path string,
     * to a {@link org.apache.hadoop.fs.Path}.
     * @param first The path string or initial part of the path string.
     * @param more Additional strings to be joined to form the path string.
     * @return the resulting {@link org.apache.hadoop.fs.Path}.
     */
    public static Path getPath(final String first, final String... more) {
        final java.nio.file.Path path = Paths.get(first, more);
        final String stringPath = FilenameUtils.separatorsToUnix(path.toAbsolutePath().toString());
        final Path hadoopPath = new Path(stringPath);
        return hadoopPath;
    }

    /**
     * Imports the files that hold the table data into the child instance.
     * @param childTableName the name of the child table to import.
     * @throws Exception
     */
    public void importFilesToChildTable(final String childTableName) throws Exception {
        final Configuration childConfig = MergeToolMapper.getChildConfig(conf);
        final AccumuloRdfConfiguration childAccumuloRdfConfiguration = new AccumuloRdfConfiguration(childConfig);
        childAccumuloRdfConfiguration.setTablePrefix(childTablePrefix);
        final Connector childConnector = AccumuloRyaUtils.setupConnector(childAccumuloRdfConfiguration);
        final TableOperations childTableOperations = childConnector.tableOperations();

        final Path localWorkDir = getPath(localCopyFileImportDir, childTableName);
        final Path hdfsBaseWorkDir = getPath(baseImportDir, childTableName);

        final FileSystem fs = FileSystem.get(conf);
        if (fs.exists(hdfsBaseWorkDir)) {
            fs.delete(hdfsBaseWorkDir, true);
        }

        log.info("Importing from the local directory: " + localWorkDir);
        log.info("Importing to the HDFS directory: " + hdfsBaseWorkDir);
        copyLocalToHdfs(localWorkDir, hdfsBaseWorkDir);

        final Path files = getPath(hdfsBaseWorkDir.toString(), "files");
        final Path failures = getPath(hdfsBaseWorkDir.toString(), "failures");

        // With HDFS permissions on, we need to make sure the Accumulo user can read/move the files
        final FsShell shell = new FsShell(conf);
        shell.run(new String[] {"-chmod", "777", hdfsBaseWorkDir.toString()});
        if (fs.exists(failures)) {
            fs.delete(failures, true);
        }
        fs.mkdirs(failures);

        childTableOperations.importDirectory(childTableName, files.toString(), failures.toString(), false);
    }

    /**
     * Copies the file from the local file system into the HDFS.
     * @param localInputPath the local system input {@link Path}.
     * @param hdfsOutputPath the HDFS output {@link Path}.
     * @throws IOException
     */
    public void copyLocalToHdfs(final Path localInputPath, final Path hdfsOutputPath) throws IOException {
        copyLocalToHdfs(localInputPath, hdfsOutputPath, conf);
    }

    /**
     * Copies the file from the local file system into the HDFS.
     * @param localInputPath the local system input {@link Path}.
     * @param hdfsOutputPath the HDFS output {@link Path}.
     * @param configuration the {@link Configuration} to use.
     * @throws IOException
     */
    public static void copyLocalToHdfs(final Path localInputPath, final Path hdfsOutputPath, final Configuration configuration) throws IOException {
        final FileSystem fs = FileSystem.get(configuration);
        fs.copyFromLocalFile(localInputPath, hdfsOutputPath);
    }

    /**
     * Copies the file from HDFS into the local file system.
     * @param hdfsInputPath the HDFS input {@link Path}.
     * @param localOutputPath the local system output {@link Path}.
     * @throws IOException
     */
    public void copyHdfsToLocal(final Path hdfsInputPath, final Path localOutputPath) throws IOException {
        copyHdfsToLocal(hdfsInputPath, localOutputPath, conf);
    }

    /**
     * Copies the file from HDFS into the local file system.
     * @param hdfsInputPath the HDFS input {@link Path}.
     * @param localOutputPath the local system output {@link Path}.
     * @param configuration the {@link Configuration} to use.
     * @throws IOException
     */
    public static void copyHdfsToLocal(final Path hdfsInputPath, final Path localOutputPath, final Configuration configuration) throws IOException {
        final FileSystem fs = FileSystem.get(configuration);
        fs.copyToLocalFile(hdfsInputPath, localOutputPath);
    }

    @Override
    protected void setupAccumuloInput(final Job job) throws AccumuloSecurityException {
        if (useCopyFileImport) {
            try {
                FileInputFormat.setInputPaths(job, localCopyFileImportDir);
            } catch (final IOException e) {
                log.error("Failed to set copy file import directory", e);
            }
        } else {
            // set up accumulo input
            if (!hdfsInput) {
                job.setInputFormatClass(AccumuloInputFormat.class);
            } else {
                job.setInputFormatClass(AccumuloHDFSFileInputFormat.class);
            }
            AbstractInputFormat.setConnectorInfo(job, userName, new PasswordToken(pwd));
            InputFormatBase.setInputTableName(job, RdfCloudTripleStoreUtils.layoutPrefixToTable(rdfTableLayout, tablePrefix));
            AbstractInputFormat.setScanAuthorizations(job, authorizations);
            if (!mock) {
                AbstractInputFormat.setZooKeeperInstance(job, new ClientConfiguration().withInstance(instance).withZkHosts(zk));
            } else {
                AbstractInputFormat.setMockInstance(job, instance);
            }
            if (ttl != null) {
                final IteratorSetting setting = new IteratorSetting(1, "fi", AgeOffFilter.class);
                AgeOffFilter.setTTL(setting, Long.valueOf(ttl));
                InputFormatBase.addIterator(job, setting);
            }
            if (startTime != null) {
                final IteratorSetting setting = getStartTimeSetting(startTime);
                InputFormatBase.addIterator(job, setting);
            }
            for (final IteratorSetting iteratorSetting : AccumuloRyaUtils.COMMON_REG_EX_FILTER_SETTINGS) {
                InputFormatBase.addIterator(job, iteratorSetting);
            }
        }
    }

    /**
     * Set up job to use AccumuloMultiTableInput format, using the tables/ranges given by a ruleset.
     * @param job The Job to configure
     * @param rules The ruleset mapping a query to the appropriate tables and ranges
     */
    protected void setupMultiTableInputFormat(final Job job, final AccumuloQueryRuleset rules) throws AccumuloSecurityException {
        AbstractInputFormat.setConnectorInfo(job, userName, new PasswordToken(pwd));
        AbstractInputFormat.setScanAuthorizations(job, authorizations);
        if (!mock) {
            AbstractInputFormat.setZooKeeperInstance(job, new ClientConfiguration().withInstance(instance).withZkHosts(zk));
        } else {
            AbstractInputFormat.setMockInstance(job, instance);
        }
        final Map<String, InputTableConfig> configs = rules.getInputConfigs();
        // Add any relevant iterator settings
        final List<IteratorSetting> additionalSettings = new LinkedList<>(AccumuloRyaUtils.COMMON_REG_EX_FILTER_SETTINGS);
        if (ttl != null) {
            final IteratorSetting ttlSetting = new IteratorSetting(1, "fi", AgeOffFilter.class);
            AgeOffFilter.setTTL(ttlSetting, Long.valueOf(ttl));
            additionalSettings.add(ttlSetting);
        }
        if (startTime != null) {
            final IteratorSetting startTimeSetting = getStartTimeSetting(startTime);
            additionalSettings.add(startTimeSetting);
        }
        for (final Map.Entry<String, InputTableConfig> entry : configs.entrySet()) {
            final List<IteratorSetting> iterators = entry.getValue().getIterators();
            iterators.addAll(additionalSettings);
            entry.getValue().setIterators(iterators);
        }
        // Set the input format
        AccumuloMultiTableInputFormat.setInputTableConfigs(job, configs);
        job.setInputFormatClass(AccumuloMultiTableInputFormat.class);
    }

    @Override
    protected void setupAccumuloOutput(final Job job, final String outputTable) throws AccumuloSecurityException {
        AccumuloOutputFormat.setConnectorInfo(job, childUserName, new PasswordToken(childPwd));
        AccumuloOutputFormat.setCreateTables(job, true);
        AccumuloOutputFormat.setDefaultTableName(job, outputTable);
        if (!childMock) {
            AccumuloOutputFormat.setZooKeeperInstance(job, new ClientConfiguration().withInstance(childInstance).withZkHosts(childZk));
        } else {
            AccumuloOutputFormat.setMockInstance(job, childInstance);
        }
        if (useCopyFileOutput) {
            log.info("Using file output format mode.");
            if (StringUtils.isNotBlank(baseOutputDir)) {
                Path baseOutputPath;
                Path filesOutputPath;
                if (StringUtils.isNotBlank(outputTable)) {
                    filesOutputPath = getPath(baseOutputDir, outputTable, "files");
                    baseOutputPath = filesOutputPath.getParent();
                    job.setOutputFormatClass(AccumuloFileOutputFormat.class);
                }
                else {
                    // If table name is not given, configure output for one level higher:
                    // it's up to the job to handle subdirectories. Make sure the parent
                    // exists.
                    filesOutputPath = getPath(baseOutputDir);
                    baseOutputPath = filesOutputPath;
                    LazyOutputFormat.setOutputFormatClass(job, AccumuloFileOutputFormat.class);
                    MultipleOutputs.setCountersEnabled(job, true);
                }
                log.info("File output destination: " + filesOutputPath);
                if (useCopyFileOutputDirectoryClear) {
                    try {
                        clearOutputDir(baseOutputPath);
                    } catch (final IOException e) {
                        log.error("Error clearing out output path.", e);
                    }
                }
                try {
                    final FileSystem fs = FileSystem.get(conf);
                    fs.mkdirs(filesOutputPath.getParent());
                    fs.setPermission(filesOutputPath.getParent(), new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
                } catch (final IOException e) {
                    log.error("Failed to set permission for output path.", e);
                }
                FileOutputFormat.setOutputPath(job, filesOutputPath);

                if (StringUtils.isNotBlank(compressionType)) {
                    if (isValidCompressionType(compressionType)) {
                        log.info("File compression type: " + compressionType);
                        AccumuloFileOutputFormat.setCompressionType(job, compressionType);
                    } else {
                        log.warn("Invalid compression type: " + compressionType);
                    }
                }
            }
        } else {
            log.info("Using accumulo output format mode.");
            job.setOutputFormatClass(AccumuloOutputFormat.class);
        }
    }

    /**
     * Sets up and runs the copy tool with the provided args.
     * @param args the arguments list.
     * @return the execution result.
     */
    public int setupAndRun(final String[] args) {
        int returnCode = -1;
        try {
            final Configuration conf = new Configuration();
            final Set<String> toolArgs = ToolConfigUtils.getUserArguments(conf, args);
            if (!toolArgs.isEmpty()) {
                final String parameters = Joiner.on("\r\n\t").join(toolArgs);
                log.info("Running Copy Tool with the following parameters...\r\n\t" + parameters);
            }

            returnCode = ToolRunner.run(conf, this, args);
        } catch (final Exception e) {
            log.error("Error running copy tool", e);
        }
        return returnCode;
    }

    public static void main(final String[] args) {
        final String log4jConfiguration = System.getProperties().getProperty("log4j.configuration");
        if (StringUtils.isNotBlank(log4jConfiguration)) {
            final String parsedConfiguration = StringUtils.removeStart(log4jConfiguration, "file:");
            final File configFile = new File(parsedConfiguration);
            if (configFile.exists()) {
                DOMConfigurator.configure(parsedConfiguration);
            } else {
                BasicConfigurator.configure();
            }
        }
        log.info("Starting Copy Tool");

        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(final Thread thread, final Throwable throwable) {
                log.error("Uncaught exception in " + thread.getName(), throwable);
            }
        });

        final CopyTool copyTool = new CopyTool();
        final int returnCode = copyTool.setupAndRun(args);

        log.info("Finished running Copy Tool");

        System.exit(returnCode);
    }

    /**
     * Creates an {@link IteratorSetting} with a time stamp filter that starts with the specified data.
     * @param startTimeString the start time of the filter.
     * @return the {@link IteratorSetting}.
     */
    public static IteratorSetting getStartTimeSetting(final String startTimeString) {
        Date date = null;
        try {
            date = MergeTool.START_TIME_FORMATTER.parse(startTimeString);
        } catch (final ParseException e) {
            throw new IllegalArgumentException("Couldn't parse " + startTimeString, e);
        }
        return getStartTimeSetting(date);
    }

    /**
     * Creates an {@link IteratorSetting} with a time stamp filter that starts with the specified data.
     * @param date the start {@link Date} of the filter.
     * @return the {@link IteratorSetting}.
     */
    public static IteratorSetting getStartTimeSetting(final Date date) {
        return getStartTimeSetting(date.getTime());
    }

    /**
     * Creates an {@link IteratorSetting} with a time stamp filter that starts with the specified data.
     * @param time the start time of the filter.
     * @return the {@link IteratorSetting}.
     */
    public static IteratorSetting getStartTimeSetting(final long time) {
        final IteratorSetting setting = new IteratorSetting(1, "startTimeIterator", TimestampFilter.class);
        TimestampFilter.setStart(setting, time, true);
        TimestampFilter.setEnd(setting, Long.MAX_VALUE, true);
        return setting;
    }

    /**
     * Checks to see if the specified compression type is valid. The compression must be defined in
     * {@link Algorithm} to be valid.
     * @param compressionType the compression type to check.
     * @return {@code true} if the compression type is one of "none", "gz", "lzo", or "snappy".
     * {@code false} otherwise.
     */
    private static boolean isValidCompressionType(final String compressionType) {
        for (final Algorithm algorithm : Algorithm.values()) {
            if (algorithm.getName().equals(compressionType)) {
                return true;
            }
        }
        return false;
    }

    private void clearOutputDir(final Path path) throws IOException {
        final FileSystem fs = FileSystem.get(conf);
        fs.delete(path, true);
    }

    private Instance createChildInstance(final Configuration config) throws Exception {
        Instance instance = null;
        String instanceTypeProp = config.get(CREATE_CHILD_INSTANCE_TYPE_PROP);
        final String childAuth = config.get(MRUtils.AC_AUTH_PROP + MergeTool.CHILD_SUFFIX);

        // Default to distribution cluster if not specified
        if (StringUtils.isBlank(instanceTypeProp)) {
            instanceTypeProp = InstanceType.DISTRIBUTION.toString();
        }

        final InstanceType instanceType = InstanceType.fromName(instanceTypeProp);
        switch (instanceType) {
            case DISTRIBUTION:
                if (childInstance == null) {
                    throw new IllegalArgumentException("Must specify instance name for distributed mode");
                } else if (childZk == null) {
                    throw new IllegalArgumentException("Must specify ZooKeeper hosts for distributed mode");
                }
                instance = new ZooKeeperInstance(childInstance, childZk);
                break;

            case MINI:
                childAccumuloInstanceDriver = new AccumuloInstanceDriver("Child", false, true, false, false, childUserName, childPwd, childInstance, childTablePrefix, childAuth);
                childAccumuloInstanceDriver.setUpInstance();
                childAccumuloInstanceDriver.setUpTables();
                childZk = childAccumuloInstanceDriver.getZooKeepers();
                MergeTool.setDuplicateKeysForProperty(config, MRUtils.AC_ZK_PROP+ MergeTool.CHILD_SUFFIX, childZk);
                instance = new ZooKeeperInstance(childInstance, childZk);
                break;

            case MOCK:
                instance = new MockInstance(childInstance);
                break;

            default:
                throw new AccumuloException("Unexpected instance type: " + instanceType);
        }

        return instance;
    }

    /**
     * @return the child {@link AccumuloInstanceDriver} or {@code null}.
     */
    public AccumuloInstanceDriver getChildAccumuloInstanceDriver() {
        return childAccumuloInstanceDriver;
    }

    /**
     * Shuts down the child {@link AccumuloInstanceDriver} in the {@link CopyTool} if it exists.
     * @throws Exception
     */
    public void shutdown() throws Exception {
        if (childAccumuloInstanceDriver != null) {
            childAccumuloInstanceDriver.tearDown();
        }
    }
}