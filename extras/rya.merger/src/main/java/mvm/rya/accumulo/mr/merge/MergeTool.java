package mvm.rya.accumulo.mr.merge;

/*
 * #%L
 * mvm.rya.accumulo.mr.merge
 * %%
 * Copyright (C) 2014 Rya
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.iterators.user.AgeOffFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.accumulo.mr.AccumuloHDFSFileInputFormat;
import mvm.rya.accumulo.mr.MRUtils;
import mvm.rya.accumulo.mr.merge.gui.DateTimePickerDialog;
import mvm.rya.accumulo.mr.merge.mappers.MergeToolMapper;
import mvm.rya.accumulo.mr.merge.util.AccumuloRyaUtils;
import mvm.rya.accumulo.mr.merge.util.TimeUtils;
import mvm.rya.accumulo.mr.merge.util.ToolConfigUtils;
import mvm.rya.api.RdfCloudTripleStoreConfiguration;
import mvm.rya.api.RdfCloudTripleStoreConstants;
import mvm.rya.api.RdfCloudTripleStoreUtils;
import mvm.rya.indexing.accumulo.ConfigUtils;

/**
 * Handles merging a child accumulo instance's data back into its parent's
 * instance.
 */
public class MergeTool extends AbstractDualInstanceAccumuloMRTool {
    private static final Logger log = Logger.getLogger(MergeTool.class);

    public static final SimpleDateFormat START_TIME_FORMATTER = new SimpleDateFormat("yyyyMMddHHmmssSSSz");

    /**
     * Appended to certain config property names to indicate that the property is for the child instance.
     */
    public static final String CHILD_SUFFIX = ".child";

    /**
     * Suffix added to a child table when it is temporarily being imported into the parent instance when
     * being read from file and before the tables are merged together.
     */
    public static final String TEMP_SUFFIX = "_temp_child";

    /**
     * The time of the data to be included in the copy/merge process.
     */
    public static final String START_TIME_PROP = "tool.start.time";

    /**
     * The name of the table to process for the map reduce job.
     */
    public static final String TABLE_NAME_PROP = "tool.table.name";

    /**
     * "true" to use file input. "false" to use Accumulo output.
     */
    public static final String USE_MERGE_FILE_INPUT = "use.merge.file.input";

    /**
     * The file path to the child data input to merge in.
     */
    public static final String MERGE_FILE_INPUT_PATH = "merge.file.input.path";

    /**
     * A value used for the {@link #START_TIME_PROP} property to indicate that a dialog
     * should be displayed to select the time.
     */
    public static final String USE_START_TIME_DIALOG = "dialog";

    private static final String DIALOG_TITLE = "Select a Start Time/Date";
    private static final String DIALOG_MESSAGE =
        "<html>Choose the time of the data to merge.<br>Only data modified AFTER the selected time will be merged.</html>";

    private String startTime = null;
    private String tempDir = null;
    private boolean useMergeFileInput = false;
    private String localMergeFileImportDir = null;
    private String baseImportDir = null;

    private String tempChildAuths = null;

    private final List<String> tables = new ArrayList<>();

    /**
     * Map of keys that are supposed to use the same values.
     */
    public static final ImmutableMap<String, List<String>> DUPLICATE_KEY_MAP = ImmutableMap.<String, List<String>>builder()
        .put(MRUtils.AC_MOCK_PROP, ImmutableList.of(ConfigUtils.USE_MOCK_INSTANCE))
        .put(MRUtils.AC_INSTANCE_PROP, ImmutableList.of(ConfigUtils.CLOUDBASE_INSTANCE))
        .put(MRUtils.AC_USERNAME_PROP, ImmutableList.of(ConfigUtils.CLOUDBASE_USER))
        .put(MRUtils.AC_PWD_PROP, ImmutableList.of(ConfigUtils.CLOUDBASE_PASSWORD))
        .put(MRUtils.AC_AUTH_PROP, ImmutableList.of(ConfigUtils.CLOUDBASE_AUTHS, RdfCloudTripleStoreConfiguration.CONF_QUERY_AUTH))
        .put(MRUtils.AC_ZK_PROP, ImmutableList.of(ConfigUtils.CLOUDBASE_ZOOKEEPERS))
        .put(MRUtils.TABLE_PREFIX_PROPERTY, ImmutableList.of(ConfigUtils.CLOUDBASE_TBL_PREFIX, RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX))
        .put(MRUtils.AC_MOCK_PROP + CHILD_SUFFIX, ImmutableList.of(ConfigUtils.USE_MOCK_INSTANCE + CHILD_SUFFIX))
        .put(MRUtils.AC_INSTANCE_PROP + CHILD_SUFFIX, ImmutableList.of(ConfigUtils.CLOUDBASE_INSTANCE + CHILD_SUFFIX))
        .put(MRUtils.AC_USERNAME_PROP + CHILD_SUFFIX, ImmutableList.of(ConfigUtils.CLOUDBASE_USER + CHILD_SUFFIX))
        .put(MRUtils.AC_PWD_PROP + CHILD_SUFFIX, ImmutableList.of(ConfigUtils.CLOUDBASE_PASSWORD + CHILD_SUFFIX))
        .put(MRUtils.AC_AUTH_PROP + CHILD_SUFFIX, ImmutableList.of(ConfigUtils.CLOUDBASE_AUTHS + CHILD_SUFFIX, RdfCloudTripleStoreConfiguration.CONF_QUERY_AUTH + CHILD_SUFFIX))
        .put(MRUtils.AC_ZK_PROP + CHILD_SUFFIX, ImmutableList.of(ConfigUtils.CLOUDBASE_ZOOKEEPERS + CHILD_SUFFIX))
        .put(MRUtils.TABLE_PREFIX_PROPERTY + CHILD_SUFFIX, ImmutableList.of(ConfigUtils.CLOUDBASE_TBL_PREFIX + CHILD_SUFFIX, RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX + CHILD_SUFFIX))
        .build();


    /**
     * Sets duplicate keys in the config.
     * @param config the {@link Configuration}.
     */
    public static void setDuplicateKeys(Configuration config) {
        for (Entry<String, List<String>> entry : DUPLICATE_KEY_MAP.entrySet()) {
            String key = entry.getKey();
            List<String> duplicateKeys = entry.getValue();
            String value = config.get(key);
            if (value != null) {
                for (String duplicateKey : duplicateKeys) {
                    config.set(duplicateKey, value);
                }
            }
        }
    }

    /**
     * Sets all duplicate keys for the property in the config to the specified value.
     * @param config the {@link Configuration}.
     * @param property the property to set and all its duplicates.
     * @param value the value to set the property to.
     */
    public static void setDuplicateKeysForProperty(Configuration config, String property, String value) {
        List<String> duplicateKeys = DUPLICATE_KEY_MAP.get(property);
        config.set(property, value);
        if (duplicateKeys != null) {
            for (String key : duplicateKeys) {
                config.set(key, value);
            }
        }
    }

    /**
     * Sets up and initializes the merge tool's configuration.
     * @throws Exception
     */
    public void setup() throws Exception {
        super.init();

        tempDir = conf.get("hadoop.tmp.dir", null);
        if (tempDir == null) {
            throw new Exception("Invalid hadoop temp directory. \"hadoop.tmp.dir\" could not be found in the configuration.");
        }

        useMergeFileInput = conf.getBoolean(USE_MERGE_FILE_INPUT, false);
        localMergeFileImportDir = conf.get(MERGE_FILE_INPUT_PATH, null);
        baseImportDir = tempDir + "/merge_tool_file_input/";

        startTime = conf.get(START_TIME_PROP, null);

        if (!useMergeFileInput) {
            // Display start time dialog if requested
            if (USE_START_TIME_DIALOG.equals(startTime)) {
                log.info("Select start time from dialog...");

                DateTimePickerDialog dateTimePickerDialog = new DateTimePickerDialog(DIALOG_TITLE, DIALOG_MESSAGE);
                dateTimePickerDialog.setVisible(true);

                Date date = dateTimePickerDialog.getSelectedDateTime();
                startTime = START_TIME_FORMATTER.format(date);
                conf.set(START_TIME_PROP, startTime);
                log.info("Will merge all data after " + date);
            } else if (startTime != null) {
                try {
                    Date date = START_TIME_FORMATTER.parse(startTime);
                    log.info("Will merge all data after " + date);
                } catch (ParseException e) {
                    throw new Exception("Unable to parse the provided start time: " + startTime, e);
                }
            }

            boolean useTimeSync = conf.getBoolean(CopyTool.USE_NTP_SERVER_PROP, false);
            if (useTimeSync) {
                String tomcatUrl = conf.get(CopyTool.CHILD_TOMCAT_URL_PROP, null);
                String ntpServerHost = conf.get(CopyTool.NTP_SERVER_HOST_PROP, null);
                Long timeOffset = null;
                try {
                    log.info("Comparing child machine's time to NTP server time...");
                    timeOffset = TimeUtils.getNtpServerAndMachineTimeDifference(ntpServerHost, tomcatUrl);
                } catch (IOException | ParseException e) {
                    throw new Exception("Unable to get time difference between machine and NTP server.", e);
                }
                if (timeOffset != null) {
                    conf.set(CopyTool.CHILD_TIME_OFFSET_PROP, "" + timeOffset);
                }
            }
        }

        setDuplicateKeys(conf);

        tables.add(tablePrefix + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX);
    }

    @Override
    public int run(String[] strings) throws Exception {
        useMergeFileInput = conf.getBoolean(USE_MERGE_FILE_INPUT, false);

        log.info("Setting up Merge Tool...");
        setup();

        if (useMergeFileInput) {
            // When using file input mode the child instance will use a temporary table in the parent instance to
            // store the child table data.  The two tables will then be merged together.
            copyParentPropertiesToChild(conf);
        }

        for (String table : tables) {
            String childTable = table.replaceFirst(tablePrefix, childTablePrefix);
            String jobName = "Merge Tool, merging Child Table: " + childTable + ", into Parent Table: " + table + ", " + System.currentTimeMillis();
            log.info("Initializing job: " + jobName);
            conf.set(MRUtils.JOB_NAME_PROP, jobName);
            conf.set(TABLE_NAME_PROP, table);

            Job job = Job.getInstance(conf);
            job.setJarByClass(MergeTool.class);

            if (useMergeFileInput) {
                importChildFilesToTempParentTable(childTable);
            }

            setupAccumuloInput(job);

            AccumuloInputFormat.setInputTableName(job, table);

            // Set input output of the particular job
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Mutation.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Mutation.class);

            setupAccumuloOutput(job, table);

            // Set mapper and reducer classes
            job.setMapperClass(MergeToolMapper.class);
            job.setReducerClass(Reducer.class);

            // Submit the job
            Date beginTime = new Date();
            log.info("Job for table \"" + table + "\" started: " + beginTime);
            int exitCode = job.waitForCompletion(true) ? 0 : 1;

            if (useMergeFileInput && StringUtils.isNotBlank(tempChildAuths)) {
                // Clear any of the temporary child auths given to the parent
                AccumuloRdfConfiguration parentAccumuloRdfConfiguration = new AccumuloRdfConfiguration(conf);
                parentAccumuloRdfConfiguration.setTablePrefix(tablePrefix);
                Connector parentConnector = AccumuloRyaUtils.setupConnector(parentAccumuloRdfConfiguration);
                SecurityOperations secOps = parentConnector.securityOperations();

                AccumuloRyaUtils.removeUserAuths(userName, secOps, tempChildAuths);
            }

            if (exitCode == 0) {
                Date endTime = new Date();
                log.info("Job for table \"" + table + "\" finished: " + endTime);
                log.info("The job took " + (endTime.getTime() - beginTime.getTime()) / 1000 + " seconds.");
            } else {
                log.error("Job for table \"" + table + "\" Failed!!!");
                return exitCode;
            }
        }

        return 0;
    }

    /**
     * Creates the temp child table if it doesn't already exist in the parent.
     * @param childTableName the name of the child table.
     * @throws IOException
     */
    public void createTempTableIfNeeded(String childTableName) throws IOException {
        try {
            AccumuloRdfConfiguration accumuloRdfConfiguration = new AccumuloRdfConfiguration(conf);
            accumuloRdfConfiguration.setTablePrefix(childTablePrefix);
            Connector connector = AccumuloRyaUtils.setupConnector(accumuloRdfConfiguration);
            if (!connector.tableOperations().exists(childTableName)) {
                log.info("Creating table: " + childTableName);
                connector.tableOperations().create(childTableName);
                log.info("Created table: " + childTableName);
                log.info("Granting authorizations to table: " + childTableName);
                SecurityOperations secOps = connector.securityOperations();
                secOps.grantTablePermission(userName, childTableName, TablePermission.WRITE);
                log.info("Granted authorizations to table: " + childTableName);

                Authorizations parentAuths = secOps.getUserAuthorizations(userName);
                // Add child authorizations so the temp parent table can be accessed.
                if (!parentAuths.equals(childAuthorizations)) {
                    List<String> childAuthList = findUniqueAuthsFromChild(parentAuths.toString(), childAuthorizations.toString());
                    tempChildAuths = Joiner.on(",").join(childAuthList);
                    log.info("Adding the authorization, \"" + tempChildAuths + "\", to the parent user, \"" + userName + "\"");
                    Authorizations newAuths = AccumuloRyaUtils.addUserAuths(userName, secOps, new Authorizations(tempChildAuths));
                    secOps.changeUserAuthorizations(userName, newAuths);
                }
            }
        } catch (TableExistsException | AccumuloException | AccumuloSecurityException e) {
            throw new IOException(e);
        }
    }

    /**
     * Gets any unique user auths that the child has that the parent does not.
     * @param parentAuths the comma-separated string of parent authorizations.
     * @param childAuths the comma-separated string of parent authorizations.
     * @return the unique child authorizations that are not in the parent.
     */
    private static List<String> findUniqueAuthsFromChild(String parentAuths, String childAuths) {
        List<String> parentAuthList = AccumuloRyaUtils.convertAuthStringToList(parentAuths);
        List<String> childAuthList = AccumuloRyaUtils.convertAuthStringToList(childAuths);

        childAuthList.removeAll(parentAuthList);

        return childAuthList;
    }

    /**
     * Imports the child files that hold the table data into the parent instance as a temporary table.
     * @param childTableName the name of the child table to import into a temporary parent table.
     * @throws Exception
     */
    public void importChildFilesToTempParentTable(String childTableName) throws Exception {
        // Create a temporary table in the parent instance to import the child files to.  Then run the merge process on the parent table and temp child table.
        String tempChildTable = childTableName + TEMP_SUFFIX;

        createTempTableIfNeeded(tempChildTable);

        AccumuloRdfConfiguration parentAccumuloRdfConfiguration = new AccumuloRdfConfiguration(conf);
        parentAccumuloRdfConfiguration.setTablePrefix(childTablePrefix);
        Connector parentConnector = AccumuloRyaUtils.setupConnector(parentAccumuloRdfConfiguration);
        TableOperations parentTableOperations = parentConnector.tableOperations();

        Path localWorkDir = CopyTool.getPath(localMergeFileImportDir, childTableName);
        Path hdfsBaseWorkDir = CopyTool.getPath(baseImportDir, childTableName);

        CopyTool.copyLocalToHdfs(localWorkDir, hdfsBaseWorkDir, conf);

        Path files = CopyTool.getPath(hdfsBaseWorkDir.toString(), "files");
        Path failures = CopyTool.getPath(hdfsBaseWorkDir.toString(), "failures");
        FileSystem fs = FileSystem.get(conf);
        // With HDFS permissions on, we need to make sure the Accumulo user can read/move the files
        fs.setPermission(hdfsBaseWorkDir, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
        if (fs.exists(failures)) {
            fs.delete(failures, true);
        }
        fs.mkdirs(failures);

        parentTableOperations.importDirectory(tempChildTable, files.toString(), failures.toString(), false);

        AccumuloRyaUtils.printTablePretty(tempChildTable, conf);
    }

    /**
     * Copies all the relevant parent instance config properties to the corresponding child properties.
     * @param config the {@link Configuration} to use.
     */
    public static void copyParentPropertiesToChild(Configuration config) {
        // Copy the parent properties for the child to use.
        copyParentPropToChild(config, MRUtils.AC_MOCK_PROP);
        copyParentPropToChild(config, MRUtils.AC_INSTANCE_PROP);
        copyParentPropToChild(config, MRUtils.AC_USERNAME_PROP);
        copyParentPropToChild(config, MRUtils.AC_PWD_PROP);
        //copyParentPropToChild(config, MRUtils.TABLE_PREFIX_PROPERTY);
        //copyParentPropToChild(config, MRUtils.AC_AUTH_PROP);
        //copyParentPropToChild(config, RdfCloudTripleStoreConfiguration.CONF_QUERY_AUTH);
        copyParentPropToChild(config, MRUtils.AC_ZK_PROP);

        MergeTool.setDuplicateKeys(config);
    }

    /**
     * Copies the parent config property to the corresponding child property.
     * @param config the {@link Configuration} to use.
     * @param parentPropertyName the parent property name to use.
     */
    public static void copyParentPropToChild(Configuration config, String parentPropertyName) {
        String parentValue = config.get(parentPropertyName, "");
        config.set(parentPropertyName + MergeTool.CHILD_SUFFIX, parentValue);
    }

    @Override
    protected void setupAccumuloInput(Job job) throws AccumuloSecurityException {
        // set up accumulo input
        if (!hdfsInput) {
            job.setInputFormatClass(AccumuloInputFormat.class);
        } else {
            job.setInputFormatClass(AccumuloHDFSFileInputFormat.class);
        }
        AccumuloInputFormat.setConnectorInfo(job, userName, new PasswordToken(pwd));
        AccumuloInputFormat.setInputTableName(job, RdfCloudTripleStoreUtils.layoutPrefixToTable(rdfTableLayout, tablePrefix));
        AccumuloInputFormat.setScanAuthorizations(job, authorizations);
        if (!mock) {
            AccumuloInputFormat.setZooKeeperInstance(job, new ClientConfiguration().withInstance(instance).withZkHosts(zk));
        } else {
            AccumuloInputFormat.setMockInstance(job, instance);
        }
        if (ttl != null) {
            IteratorSetting setting = new IteratorSetting(1, "fi", AgeOffFilter.class);
            AgeOffFilter.setTTL(setting, Long.valueOf(ttl));
            AccumuloInputFormat.addIterator(job, setting);
        }
        for (IteratorSetting iteratorSetting : AccumuloRyaUtils.COMMON_REG_EX_FILTER_SETTINGS) {
            AccumuloInputFormat.addIterator(job, iteratorSetting);
        }
    }

    /**
     * Sets up and runs the merge tool with the provided args.
     * @param args the arguments list.
     * @return the execution result.
     */
    public static int setupAndRun(String[] args) {
        int returnCode = -1;
        try {
            Configuration conf = new Configuration();
            Set<String> toolArgs = ToolConfigUtils.getUserArguments(conf, args);
            if (!toolArgs.isEmpty()) {
                String parameters = Joiner.on("\r\n\t").join(toolArgs);
                log.info("Running Merge Tool with the following parameters...\r\n\t" + parameters);
            }

            returnCode = ToolRunner.run(conf, new MergeTool(), args);
        } catch (Exception e) {
            log.error("Error running merge tool", e);
        }
        return returnCode;
    }

    public static void main(String[] args) {
        String log4jConfiguration = System.getProperties().getProperty("log4j.configuration");
        if (StringUtils.isNotBlank(log4jConfiguration)) {
            String parsedConfiguration = StringUtils.removeStart(log4jConfiguration, "file:");
            File configFile = new File(parsedConfiguration);
            if (configFile.exists()) {
                DOMConfigurator.configure(parsedConfiguration);
            } else {
                BasicConfigurator.configure();
            }
        }
        log.info("Starting Merge Tool");

        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread thread, Throwable throwable) {
                log.error("Uncaught exception in " + thread.getName(), throwable);
            }
        });

        int returnCode = setupAndRun(args);

        log.info("Finished running Merge Tool");

        System.exit(returnCode);
    }

    /**
     * Creates a formatted string for the start time based on the specified date and whether the dialog is to be displayed.
     * @param startDate the start {@link Date} to format.
     * @param isStartTimeDialogEnabled {@code true} to display the time dialog instead of using the date. {@code false}
     * to use the provided {@code startDate}.
     * @return the formatted start time string or {@code "dialog"}.
     */
    public static String getStartTimeString(Date startDate, boolean isStartTimeDialogEnabled) {
        String startTimeString;
        if (isStartTimeDialogEnabled) {
            startTimeString = USE_START_TIME_DIALOG; // set start date from dialog box
        } else {
            startTimeString = convertDateToStartTimeString(startDate);
        }
        return startTimeString;
    }

    /**
     * Converts the specified date into a string to use as the start time for the timestamp filter.
     * @param date the start {@link Date} of the filter that will be formatted as a string.
     * @return the formatted start time string.
     */
    public static String convertDateToStartTimeString(Date date) {
        String startTimeString = START_TIME_FORMATTER.format(date);
        return startTimeString;
    }

    /**
     * Converts the specified string into a date to use as the start time for the timestamp filter.
     * @param startTimeString the formatted time string.
     * @return the start {@link Date}.
     */
    public static Date convertStartTimeStringToDate(String startTimeString) {
        Date date;
        try {
            date = START_TIME_FORMATTER.parse(startTimeString);
        } catch (ParseException e) {
            log.error("Could not parse date", e);
            return null;
        }
        return date;
    }
}
