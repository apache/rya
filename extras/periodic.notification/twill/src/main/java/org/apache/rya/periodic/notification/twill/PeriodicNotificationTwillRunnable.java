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
package org.apache.rya.periodic.notification.twill;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.rya.periodic.notification.application.PeriodicApplicationException;
import org.apache.rya.periodic.notification.application.PeriodicNotificationApplication;
import org.apache.rya.periodic.notification.application.PeriodicNotificationApplicationConfiguration;
import org.apache.rya.periodic.notification.application.PeriodicNotificationApplicationFactory;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.Command;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PeriodicNotificationTwillRunnable extends AbstractTwillRunnable {

    private static final Logger logger = LoggerFactory.getLogger(PeriodicNotificationTwillRunnable.class);

    public static final String TWILL_RUNNABLE_NAME = PeriodicNotificationTwillRunnable.class.getSimpleName();
    public static final String CONFIG_FILE_NAME = "notification.properties";

    private PeriodicNotificationApplication app;

    /**
     * Called when the container process starts. Executed in container machine. If any exception is thrown from this
     * method, this runnable won't get retry.
     *
     * @param context Contains information about the runtime context.
     */
    @Override
    public void initialize(final TwillContext context) {
        logger.info("Initializing the PeriodicNotificationApplication.");

        final File propsFile = new File(CONFIG_FILE_NAME);
        final PeriodicNotificationApplicationConfiguration conf;
        try (final FileInputStream fin = new FileInputStream(propsFile)) {
            final Properties p = new Properties();
            p.load(fin);
            logger.debug("Loaded properties: {}", p);
            conf = new PeriodicNotificationApplicationConfiguration(p);
        } catch (final Exception e) {
            logger.error("Error loading notification properties", e);
            throw new RuntimeException(e); // kill the Runnable
        }

        try {
            this.app = PeriodicNotificationApplicationFactory.getPeriodicApplication(conf);
        } catch (final PeriodicApplicationException e) {
            logger.error("Error occurred creating PeriodicNotificationApplication", e);
            throw new RuntimeException(e);  // kill the Runnable
        }
    }

    /**
     * Called when a command is received. A normal return denotes the command has been processed successfully, otherwise
     * {@link Exception} should be thrown.
     * @param command Contains details of the command.
     * @throws Exception
     */
    @Override
    public void handleCommand(final Command command) throws Exception {
        // no-op
    }

    @Override
    public void run() {
        logger.info("Starting up the PeriodicNotificationApplication.");
        app.start();
        try {
            logger.info("Blocking thread termination until the PeriodicNotificationApplication is stopped.");
            app.blockUntilFinished();
        } catch (IllegalStateException | ExecutionException | InterruptedException e) {
            logger.error("An error occurred while blocking on the PeriodicNotificationApplication", e);
        }
        logger.info("Exiting the PeriodicNotificationApplication.");
    }

    /**
     * Requests to stop the running service.
     */
    @Override
    public void stop() {
        logger.info("Stopping the PeriodicNotificationApplication...");
        app.stop();
    }

    /**
     * Called when the {@link TwillRunnable#run()} completed. Useful for doing
     * resource cleanup. This method would only get called if the call to {@link #initialize(TwillContext)} was
     * successful.
     */
    @Override
    public void destroy() {
        // no-op
    }
}
