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

import com.google.common.base.Optional;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;
import jline.console.ConsoleReader;

/**
 * A mechanism for prompting a user of the application to ensure they want to
 * uninstall an instance of Rya.
 */
@DefaultAnnotation(NonNull.class)
public interface UninstallPrompt {

    /**
     * Prompt the user to make sure they want to uninstall the instance of Rya.
     *
     * @param ryaInstanceName - The name of the Rya instance being prompted for. (not null)
     * @return The value they entered.
     * @throws IOException There was a problem reading the values.
     */
    public boolean promptAreYouSure(final String ryaInstanceName) throws IOException;

    /**
     * Prompts a user for uninstall information using a JLine {@link ConsoleReader}.
     */
    public static class JLineUninstallPrompt extends JLinePrompt implements UninstallPrompt {
        @Override
        public boolean promptAreYouSure(final String ryaInstanceName) throws IOException {
            requireNonNull(ryaInstanceName);
            return promptBoolean("Are you sure you want to uninstall this instance of Rya named '" +
                    ryaInstanceName + "'? ", Optional.<Boolean>absent());
        }
    }
}