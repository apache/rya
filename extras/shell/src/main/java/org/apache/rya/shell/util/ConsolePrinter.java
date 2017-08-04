/**
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
package org.apache.rya.shell.util;

import java.io.IOException;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;
import jline.console.ConsoleReader;

/**
 * A mechanism for printing content to the console.
 */
@DefaultAnnotation(NonNull.class)
public interface ConsolePrinter {

    /**
     * Prints the provided content to the console.
     * @param cs - Output the specified String to the console.
     * @throws IOException There was a problem reading the user's input.
     */
    public void print(CharSequence cs) throws IOException;

    /**
     * Prints the provided content to the console with a newline.
     * @param cs - Output the specified String to the console.
     * @throws IOException There was a problem reading the user's input.
     */
    public void println(CharSequence cs) throws IOException;

    /**
     * Prints a newline.
     * @throws IOException There was a problem reading the user's input.
     */
    public void println() throws IOException;

    /**
     * Flush any pending console updates to the console output stream.
     * @throws IOException
     */
    public void flush() throws IOException;

    /**
     * Prints to the console using a JLine {@link ConsoleReader}.
     */
    @DefaultAnnotation(NonNull.class)
    public static class JLineConsolePrinter extends JLinePrompt implements ConsolePrinter {

        @Override
        public void print(final CharSequence cs) throws IOException {
            getReader().print(cs);
        }

        @Override
        public void println(final CharSequence cs) throws IOException {
            getReader().println(cs);
        }

        @Override
        public void println() throws IOException {
            getReader().println();
        }

        @Override
        public void flush() throws IOException {
            getReader().flush();
        }
    }
}