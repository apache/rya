/**
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
package org.apache.rya.streams.querymanager;

import static java.util.Objects.requireNonNull;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import javax.xml.bind.JAXBException;

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;
import org.apache.rya.streams.kafka.KafkaStreamsFactory;
import org.apache.rya.streams.kafka.SingleThreadKafkaStreamsFactory;
import org.apache.rya.streams.kafka.interactor.CreateKafkaTopic;
import org.apache.rya.streams.querymanager.kafka.KafkaQueryChangeLogSource;
import org.apache.rya.streams.querymanager.kafka.LocalQueryExecutor;
import org.apache.rya.streams.querymanager.xml.Kafka;
import org.apache.rya.streams.querymanager.xml.QueryManagerConfig;
import org.apache.rya.streams.querymanager.xml.QueryManagerConfig.PerformanceTunning.QueryChanngeLogDiscoveryPeriod;
import org.apache.rya.streams.querymanager.xml.QueryManagerConfigUnmarshaller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.util.concurrent.AbstractScheduledService.Scheduler;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * JSVC integration code for a {@link QueryManager} to be used as a non-Windows daemon.
 */
@DefaultAnnotation(NonNull.class)
public class QueryManagerDaemon implements Daemon {

    private static final Logger log = LoggerFactory.getLogger(QueryManagerDaemon.class);

    /**
     * The default configuration file's path for the application.
     */
    private static final Path DEFAULT_CONFIGURATION_PATH = Paths.get("config/configuration.xml");

    /**
     * Command line parameters that are used by all commands that interact with Kafka.
     */
    class DaemonParameters {
        @Parameter(names = {"--config", "-c"}, required = false, description = "The path to the application's configuration file.")
        public String config;
    }

    private QueryManager manager = null;

    @Override
    public void init(final DaemonContext context) throws DaemonInitException, Exception {
        requireNonNull(context);

        // Parse the command line arguments for the configuration file to use.
        final String[] args = context.getArguments();
        final DaemonParameters params = new DaemonParameters();
        try {
            new JCommander(params).parse(args);
        } catch(final ParameterException e) {
            throw new DaemonInitException("Unable to parse the command line arguments.", e);
        }
        final Path configFile = params.config != null ? Paths.get(params.config) : DEFAULT_CONFIGURATION_PATH;
        log.info("Loading the following configuration file: " + configFile);

        // Unmarshall the configuration file into an object.
        final QueryManagerConfig config;
        try(final InputStream stream = Files.newInputStream(configFile)) {
            config = QueryManagerConfigUnmarshaller.unmarshall(stream);
        } catch(final JAXBException | SAXException e) {
            throw new DaemonInitException("Unable to marshall the configuration XML file: " + configFile, e);
        }

        // Read the source polling period from the configuration.
        final QueryChanngeLogDiscoveryPeriod periodConfig = config.getPerformanceTunning().getQueryChanngeLogDiscoveryPeriod();
        final long period = periodConfig.getValue().longValue();
        final TimeUnit units = TimeUnit.valueOf( periodConfig.getUnits().toString() );
        log.info("Query Change Log Polling Period: " + period + " " + units);
        final Scheduler scheduler = Scheduler.newFixedRateSchedule(0, period, units);

        // Initialize a QueryChangeLogSource.
        final Kafka kafka = config.getQueryChangeLogSource().getKafka();
        log.info("Kafka Source: " + kafka.getHostname() + ":" + kafka.getPort());
        final QueryChangeLogSource source = new KafkaQueryChangeLogSource(kafka.getHostname(), kafka.getPort(), scheduler);

        // Initialize a QueryExecutor.
        final String zookeeperServers = config.getQueryExecutor().getLocalKafkaStreams().getZookeepers();
        final KafkaStreamsFactory streamsFactory = new SingleThreadKafkaStreamsFactory(kafka.getHostname() + ":" + kafka.getPort());
        final QueryExecutor queryExecutor = new LocalQueryExecutor(new CreateKafkaTopic(zookeeperServers), streamsFactory);

        // Initialize the QueryManager using the configured resources.
        manager = new QueryManager(queryExecutor, source, period, units);
    }

    @Override
    public void start() throws Exception {
        log.info("Starting the Rya Streams Query Manager Daemon.");
        manager.startAndWait();
    }

    @Override
    public void stop() throws Exception {
        log.info("Stopping the Rya Streams Query Manager Daemon.");
        manager.stopAndWait();
    }

    @Override
    public void destroy() { }
}