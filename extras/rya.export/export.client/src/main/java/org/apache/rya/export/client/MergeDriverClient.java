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
import static org.apache.rya.export.MergePolicy.TIMESTAMP;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;
import org.apache.rya.export.accumulo.AccumuloRyaStatementStore;
import org.apache.rya.export.api.MergerException;
import org.apache.rya.export.api.conf.MergeConfiguration;
import org.apache.rya.export.api.conf.MergeConfigurationException;
import org.apache.rya.export.api.conf.policy.TimestampPolicyMergeConfiguration;
import org.apache.rya.export.api.store.RyaStatementStore;
import org.apache.rya.export.client.conf.MergeConfigurationCLI;
import org.apache.rya.export.client.conf.TimeUtils;
import org.apache.rya.export.client.merge.MemoryTimeMerger;
import org.apache.rya.export.client.merge.StatementStoreFactory;
import org.apache.rya.export.client.merge.VisibilityStatementMerger;
import org.apache.rya.rdftriplestore.inference.InferenceEngineException;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;

import com.google.common.base.Optional;

/**
 * Drives the MergeTool.
 */
public class MergeDriverClient {
    private static final Logger LOG = Logger.getLogger(MergeDriverClient.class);
    private static MergeConfiguration configuration;

    public static void main(final String [] args) throws ParseException,
        MergeConfigurationException, UnknownHostException, MergerException,
        java.text.ParseException, SailException, AccumuloException,
        AccumuloSecurityException, InferenceEngineException, RepositoryException,
        MalformedQueryException, UpdateExecutionException {

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

        final boolean useTimeSync = configuration.getUseNtpServer();
        Optional<Long> offset = Optional.absent();
        if (useTimeSync) {
            final String tomcat = configuration.getChildTomcatUrl();
            final String ntpHost = configuration.getNtpServerHost();
            try {
                offset = Optional.<Long>fromNullable(TimeUtils.getNtpServerAndMachineTimeDifference(ntpHost, tomcat));
            } catch (final IOException e) {
                LOG.error("Unable to get time difference between time server: " + ntpHost + " and the server: " + tomcat, e);
            }
        }

        final StatementStoreFactory storeFactory = new StatementStoreFactory(configuration);
        try {
            final RyaStatementStore parentStore = storeFactory.getParentStatementStore();
            final RyaStatementStore childStore = storeFactory.getChildStatementStore();

            LOG.info("Starting Merge Tool");
            if(configuration.getParentDBType() == ACCUMULO && configuration.getChildDBType() == ACCUMULO) {
                final AccumuloRyaStatementStore childAStore = (AccumuloRyaStatementStore) childStore;
                final AccumuloRyaStatementStore parentAStore = (AccumuloRyaStatementStore) parentStore;

                //do map reduce merging.
                //TODO: Run Merger
            } else {
                if(configuration.getMergePolicy() == TIMESTAMP) {
                    final TimestampPolicyMergeConfiguration timeConfig = (TimestampPolicyMergeConfiguration) configuration;
                    final Long timeOffset;
                    if (offset.isPresent()) {
                        timeOffset = offset.get();
                    } else {
                        timeOffset = 0L;
                    }
                    final MemoryTimeMerger merger = new MemoryTimeMerger(parentStore, childStore,
                            new VisibilityStatementMerger(), timeConfig.getToolStartTime(),
                            configuration.getParentRyaInstanceName(), timeOffset);
                    merger.runJob();
                }
            }
        } catch (final Exception e) {
            LOG.error("Something went wrong creating a Rya Statement Store connection.", e);
        }

        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(final Thread thread, final Throwable throwable) {
                LOG.error("Uncaught exception in " + thread.getName(), throwable);
            }
        });

        LOG.info("Finished running Merge Tool");
        System.exit(1);
    }
}
