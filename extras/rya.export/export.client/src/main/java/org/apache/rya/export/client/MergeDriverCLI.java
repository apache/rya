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
package org.apache.rya.export.client;

import static org.apache.rya.export.DBType.ACCUMULO;

import java.io.File;
import java.net.UnknownHostException;
import java.io.IOException;
import java.util.Date;

import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;
import org.apache.rya.export.accumulo.AccumuloMerger;
import org.apache.rya.export.accumulo.AccumuloParentMetadataRepository;
import org.apache.rya.export.accumulo.AccumuloRyaStatementStore;
import org.apache.rya.export.accumulo.common.InstanceType;
import org.apache.rya.export.accumulo.conf.AccumuloExportConstants;
import org.apache.rya.export.accumulo.util.TimeUtils;
import org.apache.rya.export.api.parent.ParentMetadataRepository;
import org.apache.rya.export.api.store.RyaStatementStore;
import org.apache.rya.export.client.merge.ExportStatementMerger;
import org.apache.rya.export.client.merge.MemoryTimeMerger;
import org.apache.rya.export.mongo.MongoRyaStatementStore;
import org.apache.rya.export.mongo.parent.MongoParentMetadataRepository;
import org.apache.rya.export.api.MergerException;
import org.apache.rya.export.api.conf.MergeConfiguration;
import org.apache.rya.export.api.conf.MergeConfigurationCLI;
import org.apache.rya.export.api.conf.MergeConfigurationException;
import org.apache.rya.export.client.gui.DateTimePickerDialog;

import com.mongodb.MongoClient;
import mvm.rya.indexing.accumulo.ConfigUtils;

/**
 * Drives the MergeTool.
 */
public class MergeDriverCLI {
    private static final Logger LOG = Logger.getLogger(MergeDriverCLI.class);

    private static final String DIALOG_TITLE = "Select a Start Time/Date";
    private static final String DIALOG_MESSAGE =
        "<html>Choose the time of the data to merge.<br>Only data modified AFTER the selected time will be merged.</html>";

    private static MergeConfiguration configuration;

    public static void main(final String [] args) throws ParseException, MergeConfigurationException, UnknownHostException {
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

        final MergeConfigurationCLI config = new MergeConfigurationCLI(args);

        try {
            configuration = config.createConfiguration();
        } catch (final MergeConfigurationException e) {
            LOG.error("Configuration failed.", e);
        }

        final Date startTime = config.getRyaStatementMergeTime();
        final MongoClient client = new MongoClient(configuration.getParentHostname(), configuration.getParentPort());
        final RyaStatementStore parentStore = new MongoRyaStatementStore(client, configuration.getParentRyaInstanceName());
        final ParentMetadataRepository parentMetadataRepo = new MongoParentMetadataRepository(client, configuration.getParentRyaInstanceName());

        final RyaStatementStore childStore = new MongoRyaStatementStore(client, configuration.getChildRyaInstanceName());
        final ParentMetadataRepository childMetadataRepo = new MongoParentMetadataRepository(client, configuration.getChildRyaInstanceName());





        final Configuration parentConfig = new Configuration();
        parentConfig.setBoolean(ConfigUtils.USE_MOCK_INSTANCE, InstanceType.fromName(configuration.getParentInstanceType()).isMock());
        parentConfig.set(AccumuloExportConstants.ACCUMULO_INSTANCE_TYPE_PROP, configuration.getParentInstanceType());
        parentConfig.set(ConfigUtils.CLOUDBASE_INSTANCE, configuration.getParentRyaInstanceName());
        parentConfig.set(ConfigUtils.CLOUDBASE_USER, configuration.getParentUsername());
        parentConfig.set(ConfigUtils.CLOUDBASE_PASSWORD, configuration.getParentPassword());
        parentConfig.set(ConfigUtils.CLOUDBASE_ZOOKEEPERS, configuration.getParentZookeepers());
        parentConfig.set(ConfigUtils.CLOUDBASE_AUTHS, configuration.getParentAuths());
        parentConfig.set(ConfigUtils.CLOUDBASE_TBL_PREFIX, configuration.getParentTablePrefix());
        parentConfig.set(AccumuloExportConstants.PARENT_TOMCAT_URL_PROP, configuration.getParentTomcatUrl());

        final Configuration childConfig = new Configuration();
        childConfig.setBoolean(ConfigUtils.USE_MOCK_INSTANCE + AccumuloExportConstants.CHILD_SUFFIX, InstanceType.fromName(configuration.getChildInstanceType()).isMock());
        childConfig.set(AccumuloExportConstants.ACCUMULO_INSTANCE_TYPE_PROP + AccumuloExportConstants.CHILD_SUFFIX, configuration.getChildInstanceType());
        childConfig.set(ConfigUtils.CLOUDBASE_INSTANCE + AccumuloExportConstants.CHILD_SUFFIX, configuration.getChildRyaInstanceName());
        childConfig.set(ConfigUtils.CLOUDBASE_USER + AccumuloExportConstants.CHILD_SUFFIX, configuration.getChildUsername());
        childConfig.set(ConfigUtils.CLOUDBASE_PASSWORD + AccumuloExportConstants.CHILD_SUFFIX, configuration.getChildPassword());
        childConfig.set(ConfigUtils.CLOUDBASE_ZOOKEEPERS + AccumuloExportConstants.CHILD_SUFFIX, configuration.getChildZookeepers());
        childConfig.set(ConfigUtils.CLOUDBASE_AUTHS + AccumuloExportConstants.CHILD_SUFFIX, configuration.getChildAuths());
        childConfig.set(ConfigUtils.CLOUDBASE_TBL_PREFIX + AccumuloExportConstants.CHILD_SUFFIX, configuration.getChildTablePrefix());
        childConfig.set(AccumuloExportConstants.PARENT_TOMCAT_URL_PROP + AccumuloExportConstants.CHILD_SUFFIX, configuration.getChildTomcatUrl());


        String startTime = configuration.getToolStartTime();

        // Display start time dialog if requested
        if (AccumuloExportConstants.USE_START_TIME_DIALOG.equals(startTime)) {
            LOG.info("Select start time from dialog...");

            final DateTimePickerDialog dateTimePickerDialog = new DateTimePickerDialog(DIALOG_TITLE, DIALOG_MESSAGE);
            dateTimePickerDialog.setVisible(true);

            final Date date = dateTimePickerDialog.getSelectedDateTime();
            startTime = AccumuloExportConstants.START_TIME_FORMATTER.format(date);
            LOG.info("Will merge all data after " + date);
        } else if (startTime != null) {
            try {
                final Date date = AccumuloExportConstants.START_TIME_FORMATTER.parse(startTime);
                LOG.info("Will merge all data after " + date);
            } catch (final java.text.ParseException e) {
                LOG.error("Unable to parse the provided start time: " + startTime, e);
            }
        }

        final boolean useTimeSync = configuration.getUseNtpServer();
        if (useTimeSync) {
            final String tomcatUrl = configuration.getChildTomcatUrl();
            final String ntpServerHost = configuration.getNtpServerHost();
            Long timeOffset = null;
            try {
                LOG.info("Comparing child machine's time to NTP server time...");
                timeOffset = TimeUtils.getNtpServerAndMachineTimeDifference(ntpServerHost, tomcatUrl);
            } catch (IOException | java.text.ParseException e) {
                LOG.error("Unable to get time difference between machine and NTP server.", e);
            }
            if (timeOffset != null) {
                parentConfig.set(AccumuloExportConstants.CHILD_TIME_OFFSET_PROP, "" + timeOffset);
            }
        }

        if(configuration.getParentDBType() == ACCUMULO && configuration.getChildDBType() == ACCUMULO) {
            //do traditional Mergetool shenanigans
            AccumuloRyaStatementStore parentAccumuloRyaStatementStore = null;
            AccumuloRyaStatementStore childAccumuloRyaStatementStore = null;
            try {
                parentAccumuloRyaStatementStore = new AccumuloRyaStatementStore(parentConfig);
                childAccumuloRyaStatementStore = new AccumuloRyaStatementStore(childConfig);
            } catch (final MergerException e) {
                LOG.error("Failed to create statement stores", e);
            }

            final AccumuloParentMetadataRepository accumuloParentMetadataRepository = new AccumuloParentMetadataRepository(parentAccumuloRyaStatementStore.getRyaDAO());

            final AccumuloMerger accumuloMerger = new AccumuloMerger(parentAccumuloRyaStatementStore, childAccumuloRyaStatementStore, accumuloParentMetadataRepository);
            accumuloMerger.runJob();
        } else {

        }

        LOG.info("Starting Merge Tool");

        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(final Thread thread, final Throwable throwable) {
                LOG.error("Uncaught exception in " + thread.getName(), throwable);
            }
        });

        final MemoryTimeMerger merger = new MemoryTimeMerger(parentStore, childStore,
                parentMetadataRepo, childMetadataRepo, new ExportStatementMerger(),
                startTime, configuration.getParentRyaInstanceName());
        merger.runJob();

        LOG.info("Finished running Merge Tool");
        System.exit(1);
    }
}
