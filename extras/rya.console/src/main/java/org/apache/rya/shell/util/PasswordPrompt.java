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

import javax.annotation.ParametersAreNonnullByDefault;

import jline.console.ConsoleReader;

/**
 * A mechanism for prompting a user of the application for a password.
 */
@ParametersAreNonnullByDefault
public interface PasswordPrompt {

    /**
     * Prompt the user for a password, wait for their input, and then get the
     * value they entered.
     *
     * @return A character array holding the entered password.
     * @throws IOEXception There was a problem reading the password.
     */
    public char[] getPassword() throws IOException;

    /**
     * Prompts a user for their password using a JLine {@link ConsoleReader}.
     * <p>
     * This prompt has a known security issue. ConsoleReader only reads passwords
     * into Strings, so they can't easily be cleared out. We many an attempt to
     * garbage collect the String after converting it to a character array, but
     * this could be improved.
     */
    public static class JLinePasswordPrompt extends JLinePrompt implements PasswordPrompt {

        @Override
        public char[] getPassword() throws IOException {
            char[] password = new char[0];

            final ConsoleReader reader = getReader();
            reader.setPrompt("Password: ");
            String passwordStr = reader.readLine('*');
            password = passwordStr.toCharArray();

            // Reading the password into memory as a String is less safe than a char[]
            // because the String is immutable. We can't clear it out. At best, we can
            // remove all references to it and suggest the GC clean it up. There are no
            // guarantees though.
            passwordStr = null;
            System.gc();

            return password;
        }
    }
}