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
package org.apache.rya.periodic.notification.twill.yarn;

import java.io.File;
import java.io.FileInputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.rya.periodic.notification.application.PeriodicNotificationApplicationConfiguration;
import org.apache.rya.periodic.notification.twill.PeriodicNotificationTwillApp;
import org.apache.rya.periodic.notification.twill.PeriodicNotificationTwillRunnable;
import org.apache.twill.api.ClassAcceptor;
import org.apache.twill.api.ResourceReport;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunResources;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.apache.twill.yarn.YarnTwillRunnerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;

/**
 * This class is responsible for starting and stopping the {@link PeriodicNotificationTwillApp} on a Hadoop YARN cluster.
 */
public class PeriodicNotificationTwillRunner implements AutoCloseable {

    public static final Logger LOG = LoggerFactory.getLogger(PeriodicNotificationTwillRunner.class);

    private final YarnConfiguration yarnConfiguration;
    private final TwillRunnerService twillRunner;
    private final File configFile;

    /**
     *
     * @param yarnZookeepers - The zookeeper connect string used by the Hadoop YARN cluster.
     * @param configFile - The config file used by {@link PeriodicNotificationTwillApp}.  Typically notification.properties.
     */
    public PeriodicNotificationTwillRunner(final String yarnZookeepers, final File configFile) {
        Preconditions.checkArgument(configFile.exists(), "Config File must exist");
        Objects.requireNonNull(yarnZookeepers, "YARN Zookeepers must not be null.");
        this.configFile = configFile;
        yarnConfiguration = new YarnConfiguration();
        twillRunner = new YarnTwillRunnerService(yarnConfiguration, yarnZookeepers);
        twillRunner.start();

        // sleep to give the YarnTwillRunnerService time to retrieve state from zookeeper
        try {
            Thread.sleep(1000);
        } catch (final InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Start an instance of the {@link PeriodicNotificationTwillApp}.
     *
     * @param interactive - If true, this method will block until the user terminates this JVM, at which point the
     *            {@link PeriodicNotificationTwillApp} on the YARN cluster will also be terminated. If false, this
     *            method will return after startup.
     */
    public void startApp(final boolean interactive) {
        final String yarnClasspath = yarnConfiguration.get(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                Joiner.on(",").join(YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH));
        final List<String> applicationClassPaths = Lists.newArrayList();
        Iterables.addAll(applicationClassPaths, Splitter.on(",").split(yarnClasspath));
        final TwillController controller = twillRunner
                .prepare(new PeriodicNotificationTwillApp(configFile))
                .addLogHandler(new PrinterLogHandler(new PrintWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8), true)))
                .withApplicationClassPaths(applicationClassPaths)
                //.withApplicationArguments(args)
                //.withArguments(runnableName, args)
                // .withBundlerClassAcceptor(new HadoopClassExcluder())
                .start();

        final ResourceReport r = getResourceReport(controller, 5, TimeUnit.MINUTES);
        LOG.info("Received ResourceReport: {}", r);
        LOG.info("{} started successfully!", PeriodicNotificationTwillApp.APPLICATION_NAME);

        if(interactive) {
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        Futures.getUnchecked(controller.terminate());
                    } finally {
                        twillRunner.stop();
                    }
                }
            });

            try {
                LOG.info("{} waiting termination by user.  Type ctrl-c to terminate.", PeriodicNotificationTwillApp.class.getSimpleName());
                controller.awaitTerminated();
            } catch (final ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Terminates all instances of the {@link PeriodicNotificationTwillApp} on the YARN cluster.
     */
    public void stopApp() {
        LOG.info("Stopping any running instances...");

        int counter = 0;
        // It is possible that we have launched multiple instances of the app.  For now, stop them all, one at a time.
        for(final TwillController c : twillRunner.lookup(PeriodicNotificationTwillApp.APPLICATION_NAME)) {
            final ResourceReport report = c.getResourceReport();
            LOG.info("Attempting to stop {} with YARN ApplicationId: {} and Twill RunId: {}", PeriodicNotificationTwillApp.APPLICATION_NAME, report.getApplicationId(), c.getRunId());
            Futures.getUnchecked(c.terminate());
            LOG.info("Stopped {} with YARN ApplicationId: {} and Twill RunId: {}", PeriodicNotificationTwillApp.APPLICATION_NAME, report.getApplicationId(), c.getRunId());
            counter++;
        }

        LOG.info("Stopped {} instance(s) of {}", counter, PeriodicNotificationTwillApp.APPLICATION_NAME);
    }

    /**
     * Blocks until a non-null Resource report is returned.
     * @param controller - The controller to interrogate.
     * @param timeout - The maximum time to poll {@controller}.  Use -1 for infinite polling.
     * @param timeoutUnits - The units of {@code timeout}.
     * @return The ResourceReport for the application.
     * @throws IllegalStateException If a timeout occurs before a ResourceReport is returned.
     */
    private ResourceReport getResourceReport(final TwillController controller, final long timeout, final TimeUnit timeoutUnits) {
        Preconditions.checkArgument(timeout >= -1, "timeout cannot be less than -1");
        final long timeoutMillis = TimeUnit.MILLISECONDS.convert(timeout, timeoutUnits);
        final long sleepMillis = 1000; // how long to sleep between retrieval attempts.
        long totalElapsedMillis = 0;
        ResourceReport report = controller.getResourceReport();
        while (reportIsLoading(report)) {
            try {
                Thread.sleep(sleepMillis);
            } catch (final InterruptedException e) {
                throw new IllegalStateException(e);
            }
            totalElapsedMillis += sleepMillis;
            if ((timeout != -1) && (totalElapsedMillis >= timeoutMillis)) {
                final String errorMessage = "Timeout while waiting for the Twill Application to start on YARN.  Total elapsed time: " + TimeUnit.SECONDS.convert(totalElapsedMillis, TimeUnit.MILLISECONDS) + "s.";
                LOG.error(errorMessage);
                throw new IllegalStateException(errorMessage);
            }
            if ((totalElapsedMillis % 5000) == 0) {
                LOG.info("Waiting for the Twill Application to start on YARN... Total elapsed time: {}s.", TimeUnit.SECONDS.convert(totalElapsedMillis, TimeUnit.MILLISECONDS));
            }
            report = controller.getResourceReport();
        }
        return report;
    }

    /**
     * Checks to see if the report has loaded.
     * @param report - The {@link ResourceReport} for this Twill Application.
     * @return Return true if the report is null or incomplete.  Return false if the report is completely loaded.
     */
    private boolean reportIsLoading(@Nullable final ResourceReport report) {
        if(report == null) {
            return true;
        }

        final String yarnApplicationID = report.getApplicationId();
        final Collection<TwillRunResources> runnableResources = report.getResources().get(PeriodicNotificationTwillRunnable.TWILL_RUNNABLE_NAME);

        if(runnableResources == null || runnableResources.isEmpty()) {
            LOG.info("Received Resource Report for YARN ApplicationID: {}, runnable resources are still loading...", yarnApplicationID);
            return true;
        } else {
            LOG.info("Received Resource Report for YARN ApplicationID: {}, runnable resources are loaded.", yarnApplicationID);
            return false;
        }
    }

    @Override
    public void close() throws Exception {
        if(twillRunner != null) {
            twillRunner.stop();
        }
    }

    private static class MainOptions {
        @Parameter(names = { "-c", "--config-file" }, description = "PeriodicNotification Application config file", required = true)
        private File configFile;

        @Parameter(names = { "-z", "--yarn-zookeepers" }, description = "(Optional) YARN Zookeepers connect string.  If not specified, the value of 'accumulo.zookeepers' from the specified '--config-file' will be reused.", required = false)
        private String zookeepers;
    }


    @Parameters(commandNames = { "start" }, separators = "=", commandDescription = "Start the PeriodicNotification Application on YARN")
    private static class CommandStart {
        @Parameter(names = { "-i", "--interactive" }, description = "(Optional) Interactive.  If specified, blocks the console until the user types ctrl-c.", required = false)
        private boolean interactive;

        //TODO future feature.
        //@Parameter(names = { "-p", "--accumulo.password"}, description = "Leave value blank to be prompted interactively for the 'accumulo.password' of the 'accumulo.user' specified by the '--config-file'", password = true, required = true)
        //private String password;
    }

    @Parameters(commandNames = { "stop" }, commandDescription = "Stops PeriodicNotification Applications on YARN")
    private static class CommandStop {
        //TODO future feature.
        //@Parameter(names = { "-a", "--all" }, description = "Stops all PeriodicNotification Application instances.", required = false)
        //private boolean all = true;

        //@Parameter(names = { "-i", "--instances"}, description = "CSV List of application instances to be stopped", required = false)
        //private List<String> instanceList;
    }


    public static void main(final String[] args) {

        final MainOptions options = new MainOptions();
        final String START = "start";
        final String STOP = "stop";
        String parsedCommand = null;
        final CommandStart commandStart = new CommandStart();
        final CommandStop commandStop = new CommandStop();
        final JCommander cli = new JCommander(options);
        cli.addCommand(START, commandStart);
        cli.addCommand(STOP, commandStop);
        cli.setProgramName(PeriodicNotificationTwillRunner.class.getName());
        try {
            cli.parse(args);
            parsedCommand = cli.getParsedCommand();
            if(parsedCommand == null) {
                throw new ParameterException("A command must be specified.");
            }
        } catch (final ParameterException e) {
            System.err.println("Error! Invalid input: " + e.getMessage());
            cli.usage();
            System.exit(1);
        }

        // load the config file
        PeriodicNotificationApplicationConfiguration conf = null;
        try (FileInputStream fin = new FileInputStream(options.configFile)) {
            final Properties p = new Properties();
            p.load(fin);
            conf = new PeriodicNotificationApplicationConfiguration(p);
        } catch (final Exception e) {
            LOG.warn("Unable to load specified properties file", e);
            System.exit(1);
        }

        // pick the correct zookeepers
        String zookeepers = null;
        if(options.zookeepers != null && !options.zookeepers.isEmpty()) {
            zookeepers = options.zookeepers;
        } else if (conf != null) {
            zookeepers = conf.getAccumuloZookeepers();
        }
        if (zookeepers == null) {
            LOG.warn("Zookeeper connection info can not be determined from main options nor configuration file.");
            System.exit(1);
        }

        try (final PeriodicNotificationTwillRunner app = new PeriodicNotificationTwillRunner(zookeepers, options.configFile)) {
            if(START.equals(parsedCommand)) {
                app.startApp(commandStart.interactive);
            } else if(STOP.equals(parsedCommand)) {
                app.stopApp();
            } else {
                throw new IllegalStateException("Invalid Command."); // this state should be impossible.
            }
        } catch (final Exception e) {
            LOG.warn("Error occurred.", e);
            System.exit(1);
        }
    }

    static class HadoopClassExcluder extends ClassAcceptor {
        @Override
        public boolean accept(final String className, final URL classUrl, final URL classPathUrl) {
            // exclude hadoop but not hbase package
            return !(className.startsWith("org.apache.hadoop") && !className.startsWith("org.apache.hadoop.hbase"));
        }
    }
}