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

import com.google.common.base.Optional;

import jline.console.ConsoleReader;
import org.apache.rya.api.client.Install.InstallConfiguration;

/**
 * A mechanism for prompting a user of the application for a the parameters
 * that will be used when installing an instance of Rya.
 */
public interface InstallPrompt {

    /**
     * Prompt the user for the name of the Rya instance that will be created.
     *
     * @return The value they entered.
     * @throws IOException There was a problem reading the value.
     */
    public String promptInstanceName() throws IOException;

    /**
     * Prompt the user for which features of Rya they want enabled.
     *
     * @return The value they entered.
     * @throws IOException There was a problem reading the values.
     */
    public InstallConfiguration promptInstallConfiguration() throws IOException;

    /**
     * Prompt the user asking them if they are sure they would like to do the
     * install.
     *
     * @return The value they entered.
     * @throws IOException There was a problem reading the value.
     */
    public boolean promptVerified(String instanceName, InstallConfiguration installConfig) throws IOException;

    /**
     * Prompts a user for install information using a JLine {@link ConsoleReader}.
     */
    public static class JLineAccumuloInstallPrompt extends JLinePrompt implements InstallPrompt {

        @Override
        public String promptInstanceName() throws IOException {
            final ConsoleReader reader = getReader();
            reader.setPrompt("Rya Instance Name: ");
            final String instanceName = reader.readLine();
            return instanceName;
        }

        @Override
        public InstallConfiguration promptInstallConfiguration() throws IOException {
            final InstallConfiguration.Builder builder = InstallConfiguration.builder();

            String prompt = makeFieldPrompt("Use Shard Balancing (improves streamed input write speeds)", false);
            final boolean enableTableHashPrefix = promptBoolean(prompt, Optional.of(false));
            builder.setEnableTableHashPrefix( enableTableHashPrefix );

            prompt = makeFieldPrompt("Use Entity Centric Indexing", true);
            final boolean enableEntityCentricIndexing = promptBoolean(prompt, Optional.of(true));
            builder.setEnableEntityCentricIndex( enableEntityCentricIndexing );

            prompt = makeFieldPrompt("Use Free Text Indexing", true);
            final boolean enableFreeTextIndexing = promptBoolean(prompt, Optional.of(true));
            builder.setEnableFreeTextIndex( enableFreeTextIndexing );

            prompt = makeFieldPrompt("Use Geospatial Indexing", true);
            final boolean enableGeoIndexing = promptBoolean(prompt, Optional.of(true));
            builder.setEnableGeoIndex( enableGeoIndexing );

            prompt = makeFieldPrompt("Use Precomputed Join Indexing", true);
            final boolean enablePCJIndexing = promptBoolean(prompt, Optional.of(true));
            builder.setEnablePcjIndex( enablePCJIndexing );

            if(enablePCJIndexing) {
                final boolean useFluoApp = promptBoolean("Use a Fluo application to update the PCJ? ", Optional.absent());

                if(useFluoApp) {
                    prompt = "PCJ Updater Fluo Application Name: ";
                    final String fluoAppName = promptString(prompt, Optional.<String>absent());
                    builder.setFluoPcjAppName(fluoAppName);
                }
            }

            prompt = makeFieldPrompt("Use Temporal Indexing", true);
            final boolean useTemporalIndexing = promptBoolean(prompt, Optional.of(true));
            builder.setEnableTemporalIndex( useTemporalIndexing );

            return builder.build();
        }

        @Override
        public boolean promptVerified(final String instanceName, final InstallConfiguration installConfig) throws IOException {
            final ConsoleReader reader = getReader();
            reader.println();
            reader.println("A Rya instance will be installed using the following values:");
            reader.println("   Instance Name: " + instanceName);
            reader.println("   Use Shard Balancing: " + installConfig.isTableHashPrefixEnabled());
            reader.println("   Use Entity Centric Indexing: " + installConfig.isEntityCentrixIndexEnabled());
            reader.println("   Use Free Text Indexing: " + installConfig.isFreeTextIndexEnabled());
            reader.println("   Use Geospatial Indexing: " + installConfig.isGeoIndexEnabled());

            reader.println("   Use Precomputed Join Indexing: " + installConfig.isPcjIndexEnabled());
            if(installConfig.isPcjIndexEnabled()) {
                if(installConfig.getFluoPcjAppName().isPresent()) {
                    reader.println("   PCJ Updater Fluo Application Name: " + installConfig.getFluoPcjAppName().get());
                } else {
                    reader.println("   Not using a PCJ Updater Fluo Application");
                }
            }

            reader.println("   Use Temporal Indexing: " + installConfig.isTemporalIndexEnabled());
            reader.println("");

            return promptBoolean("Continue with the install? ", Optional.<Boolean>absent());
        }
    }
}