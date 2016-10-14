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

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.Set;

import javax.annotation.ParametersAreNonnullByDefault;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.util.FieldUtils;
import org.springframework.shell.core.Shell;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;

import jline.console.ConsoleReader;

/**
 * Provides access to the host {@link Shell}'s {@link ConsoleReader} and some
 * utility functions for using it.
 */
@ParametersAreNonnullByDefault
public abstract class JLinePrompt {

    /**
     * Defines things that may be typed in response to a boolean prompt that evaluate to true.
     */
    private static final Set<String> affirmativeStrings = Sets.newHashSet("true", "t", "yes", "y");

    /**
     * Defines things that may be typed in response to a boolean prompt that evaluate to false.
     */
    private static final Set<String> negativeStrings = Sets.newHashSet("false", "f", "no", "n");

    @Autowired
    private Shell shell;

    /**
     * @return The shell's {@link ConsoleReader}.
     */
    public ConsoleReader getReader() {
        // XXX Spring Shell doesn't expose the reader that we need to use to
        //     read values from a terminal, so use reflection to pull it out.
        return (ConsoleReader) FieldUtils.getProtectedFieldValue("reader", shell);
    }

    /**
     * Formats a prompt that shows a default value.
     *
     * @param fieldName - The text portion that appears before the default. (not null)
     * @param defaultValue - The default value that will be shown in the prompt.
     * @return A prompt that shows the default value for a field.
     */
    public String makeFieldPrompt(final String fieldName, final boolean defaultValue) {
        return String.format("%s [default: %s]: ", fieldName, defaultValue);
    }

    /**
     * Checks if a user's input matches one of the affirmative strings.
     *
     * @param input - The user's input. (not null)
     * @return {@code true} if the input is one of the affirmative values; otherwise {@code false}.
     */
    private boolean isAffirmative(final String input) {
        requireNonNull(input);
        for(final String affirmativeString : affirmativeStrings) {
            if( input.equalsIgnoreCase(affirmativeString) ) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks if a user's input matches one of the negative strings.
     *
     * @param input - The user's input. (not null)
     * @return {@code true} if the input is one of the negative values; otherwise {@code false}.
     */
    private boolean isNegative(final String input) {
        requireNonNull(input);
        for(final String negativeString : negativeStrings) {
            if( input.equalsIgnoreCase(negativeString) ) {
                return true;
            }
        }
        return false;
    }

    /**
     * Prompts a user for a boolean value. The prompt will be repeated until
     * a value true/false string has been submitted. If a default value is
     * provided, then it will be used if the user doens't enter anything.
     *
     * @param prompt - The prompt for the input. (not null)
     * @param defaultValue - The default value for the input if one is provided. (not null)
     * @return The value the user entered.
     * @throws IOException There was a problem reading values from the user.
     */
    protected boolean promptBoolean(final String prompt, final Optional<Boolean> defaultValue) throws IOException {
        requireNonNull(prompt);
        requireNonNull(defaultValue);

        final ConsoleReader reader = getReader();
        reader.setPrompt(prompt);

        Boolean value = null;
        boolean prompting = true;

        while(prompting) {
            // An empty input means to use the default value.
            final String input = reader.readLine();
            if(input.isEmpty() && defaultValue.isPresent()) {
                value = defaultValue.get();
                prompting = false;
            }

            // Check if it is one of the affirmative answers.
            if(isAffirmative(input)) {
                value = true;
                prompting = false;
            }

            // Check if it is one of the negative answers.
            if(isNegative(input)) {
                value = false;
                prompting = false;
            }

            // If we are still prompting, the input was invalid.
            if(prompting) {
                reader.println("Invalid response (true/false)");
            }
        }

        return value;
    }

    /**
     * Prompts a user for a String value. The prompt will be repeated until a
     * value has been submitted. If a default value is provided, then it will be
     * used if the user doesn't enter anything.
     *
     * @param prompt - The prompt for the input. (not null)
     * @param defaultValue - The default value for the input if one is provided. (not null)
     * @return The value the user entered.
     * @throws IOException There was a problem reading values from the user.
     */
    protected String promptString(final String prompt, final Optional<String> defaultValue) throws IOException {
        requireNonNull(prompt);
        requireNonNull(defaultValue);

        final ConsoleReader reader = getReader();
        reader.setPrompt(prompt);

        String value = null;
        boolean prompting = true;

        while(prompting) {
            // Read a line of input.
            final String input = reader.readLine();

            if(!input.isEmpty()) {
                // If a value was provided, return it.
                value = input;
                prompting = false;
            } else {
                // Otherwise, if a default value was provided, return it;
                if(defaultValue.isPresent()) {
                    value = defaultValue.get();
                    prompting = false;
                } else {
                    // Otherwise, the user must provide a value.
                    reader.println("Invalid response. Must provide a value.");
                }
            }
        }

        return value;
    }
}