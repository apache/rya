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
package org.apache.rya.streams.client;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A command that may be executed by the {@link PcjAdminClient}.
 */
@DefaultAnnotation(NonNull.class)
public interface RyaStreamsCommand {

    /**
     * @return What a user would type into the command line to indicate
     *   they want to execute this command.
     */
    public String getCommand();

    /**
     * @return Briefly describes what the command does.
     */
    public String getDescription();

    /**
     * @return Describes what arguments may be provided to the command.
     */
    public String getUsage();

    /**
     * Execute the command using the command line arguments.
     *
     * @param args - Command line arguments that configure how the command will execute. (not null)
     * @throws UnsupportedQueryException
     */
    public void execute(final String[] args) throws ArgumentsException, ExecutionException;

    /**
     * A {@link RyaStreamsCommand} could not be executed because of a problem with
     * the arguments that were provided to it.
     */
    public static final class ArgumentsException extends Exception {
        private static final long serialVersionUID = 1L;

        public ArgumentsException(final String message) {
            super(message);
        }

        public ArgumentsException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * A {@link RyaStreamsCommand} could not be executed.
     */
    public static final class ExecutionException extends Exception {
        private static final long serialVersionUID = 1L;

        public ExecutionException(final String message) {
            super(message);
        }

        public ExecutionException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }
}
