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
package org.apache.rya.accumulo.mr.merge.demo.util;

import java.util.Scanner;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

/**
 * Utilities methods for the demo.
 */
public final class DemoUtilities {
    private static final Logger log = Logger.getLogger(DemoUtilities.class);

    /**
     * Holds different logging patterns to use.
     */
    public static enum LoggingDetail {
        /**
         * Uses a light pattern layout.
         * */
        LIGHT(new PatternLayout("%-5p - %m%n")),
        /**
         * Uses a detailed pattern layout.
         */
        DETAILED(new PatternLayout("%d{MMM dd yyyy HH:mm:ss} %5p [%t] (%F:%L) - %m%n"));

        private PatternLayout patternLayout;

        /**
         * Create a new {@link LoggingDetail}.
         * @param patternLayout the {@link PatternLayout}.
         */
        private LoggingDetail(final PatternLayout patternLayout) {
            this.patternLayout = patternLayout;
        }

        /**
         * @return the {@link PatternLayout}.
         */
        public PatternLayout getPatternLayout() {
            return patternLayout;
        }
    }

    private static final Scanner KEYBOARD_SCANNER = new Scanner(System.in);

    /**
     * Private constructor to prevent instantiation.
     */
    private DemoUtilities() {
    }

    /**
     * Generates a random {@code long} number within the specified range.
     * @param min the minimum value (inclusive) of the range of {@code long}s to include.
     * @param max the maximum value (inclusive) of the range of {@code long}s to include.
     * @return the random {@code long}.
     */
    public static long randLong(final long min, final long max) {
        return new RandomDataGenerator().nextLong(min, max);
    }

    /**
     * Sets up log4j logging to fit the demo's needs.
     */
    public static void setupLogging() {
        setupLogging(LoggingDetail.LIGHT);
    }

    /**
     * Sets up log4j logging to fit the demo's needs.
     * @param loggingDetail the {@link LoggingDetail} to use.
     */
    public static void setupLogging(final LoggingDetail loggingDetail) {
        // Turn off all the loggers and customize how they write to the console.
        final Logger rootLogger = LogManager.getRootLogger();
        rootLogger.setLevel(Level.OFF);
        final ConsoleAppender ca = (ConsoleAppender) rootLogger.getAppender("stdout");
        ca.setLayout(loggingDetail.getPatternLayout());

        // Turn the loggers used by the demo back on.
        //log.setLevel(Level.INFO);
        rootLogger.setLevel(Level.INFO);
    }

    /**
     * Pauses the program until the user presses "Enter" in the console.
     */
    public static void promptEnterKey() {
        promptEnterKey(true);
    }

    /**
     * Pauses the program until the user presses "Enter" in the console.
     * @param isPromptEnabled {@code true} if prompt display is enabled. {@code false}
     * otherwise.
     */
    public static void promptEnterKey(final boolean isPromptEnabled) {
        if (isPromptEnabled) {
            log.info("Press \"ENTER\" to continue...");
            KEYBOARD_SCANNER.nextLine();
        }
    }
}
