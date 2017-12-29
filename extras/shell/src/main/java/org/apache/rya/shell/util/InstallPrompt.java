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

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

import java.io.IOException;

import org.apache.rya.api.client.Install.InstallConfiguration;
import org.apache.rya.shell.SharedShellState;
import org.apache.rya.shell.SharedShellState.StorageType;
import org.springframework.beans.factory.annotation.Autowired;

import com.google.common.base.Optional;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;
import jline.console.ConsoleReader;

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
     * @param instanceName - The Rya instance name. (not null)
     * @return The value they entered.
     * @throws IOException There was a problem reading the values.
     */
    public InstallConfiguration promptInstallConfiguration(String instanceName) throws IOException;

    /**
     * Prompt the user asking them if they are sure they would like to do the
     * install.
     *
     * @param instanceName - The Rya instance name. (not null)
     * @param installConfig - The configuration that will be presented to the user. (not null)
     * @return The value they entered.
     * @throws IOException There was a problem reading the value.
     */
    public boolean promptVerified(String instanceName, InstallConfiguration installConfig) throws IOException;

    /**
     * Prompts a user for install information using a JLine {@link ConsoleReader}.
     * The prompt it uses depends on the storage that is connected to.
     */
    @DefaultAnnotation(NonNull.class)
    public static class JLineInstallPropmpt extends JLinePrompt implements InstallPrompt {

        @Autowired
        private SharedShellState sharedShellState;

        @Override
        public String promptInstanceName() throws IOException {
            final String prompt = makeFieldPrompt("Rya Instance Name", "rya_");
            final String instanceName = promptString(prompt, Optional.of("rya_"));
            return instanceName;
        }

        @Override
        public InstallConfiguration promptInstallConfiguration(final String instanceName) throws IOException {
            final Optional<StorageType> storageType = sharedShellState.getShellState().getStorageType();
            checkState(storageType.isPresent(), "The shell must be connected to a storage to use the install prompt.");

            switch(sharedShellState.getShellState().getStorageType().get()) {
                case ACCUMULO:
                    return promptAccumuloConfig(instanceName);

                case MONGO:
                    return promptMongoConfig(instanceName);

                default:
                    throw new IllegalStateException("Unsupported storage type: " + storageType.get());
            }
        }

        @Override
        public boolean promptVerified(final String instanceName, final InstallConfiguration installConfig) throws IOException {
            final Optional<StorageType> storageType = sharedShellState.getShellState().getStorageType();
            checkState(storageType.isPresent(), "The shell must be connected to a storage to use the install prompt.");

            switch(sharedShellState.getShellState().getStorageType().get()) {
                case ACCUMULO:
                    return promptAccumuloVerified(instanceName, installConfig);

                case MONGO:
                    return promptMongoVerified(instanceName, installConfig);

                default:
                    throw new IllegalStateException("Unsupported storage type: " + storageType.get());
            }
        }

        /**
         * Prompt the user for which Accumulo specific features of Rya they want enabled.
         *
         * @param instanceName - The Rya instance name. (not null)
         * @return The value they entered.
         * @throws IOException There was a problem reading the values.
         */
        private InstallConfiguration promptAccumuloConfig(final String instanceName) throws IOException {
            requireNonNull(instanceName);

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

// RYA-215            prompt = makeFieldPrompt("Use Geospatial Indexing", true);
//            final boolean enableGeoIndexing = promptBoolean(prompt, Optional.of(true));
//            builder.setEnableGeoIndex( enableGeoIndexing );

            prompt = makeFieldPrompt("Use Temporal Indexing", true);
            final boolean useTemporalIndexing = promptBoolean(prompt, Optional.of(true));
            builder.setEnableTemporalIndex( useTemporalIndexing );

            prompt = makeFieldPrompt("Use Precomputed Join Indexing", true);
            final boolean enablePCJIndexing = promptBoolean(prompt, Optional.of(true));
            builder.setEnablePcjIndex( enablePCJIndexing );

            if(enablePCJIndexing) {
                final boolean useFluoApp = promptBoolean("Use a Fluo application to update the PCJ Index? (y/n) ", Optional.absent());

                if(useFluoApp) {
                    prompt = makeFieldPrompt("PCJ Updater Fluo Application Name (must be initialized)", instanceName + "pcj_updater");
                    final String fluoAppName = promptString(prompt, Optional.of(instanceName + "pcj_updater"));
                    builder.setFluoPcjAppName(fluoAppName);
                }
            }

            return builder.build();
        }

        /**
         * Prompt the user asking them if they are sure they would like to do the
         * install.
         *
         * @param instanceName - The Rya instance name. (not null)
         * @param installConfig - The configuration that will be presented to the user. (not null)
         * @return The value they entered.
         * @throws IOException There was a problem reading the value.
         */
        private boolean promptAccumuloVerified(final String instanceName, final InstallConfiguration installConfig)  throws IOException {
            requireNonNull(instanceName);
            requireNonNull(installConfig);

            final ConsoleReader reader = getReader();
            reader.println();
            reader.println("A Rya instance will be installed using the following values:");
            reader.println("   Instance Name: " + instanceName);
            reader.println("   Use Shard Balancing: " + installConfig.isTableHashPrefixEnabled());
            reader.println("   Use Entity Centric Indexing: " + installConfig.isEntityCentrixIndexEnabled());
            reader.println("   Use Free Text Indexing: " + installConfig.isFreeTextIndexEnabled());
// RYA-215            reader.println("   Use Geospatial Indexing: " + installConfig.isGeoIndexEnabled());
            reader.println("   Use Temporal Indexing: " + installConfig.isTemporalIndexEnabled());
            reader.println("   Use Precomputed Join Indexing: " + installConfig.isPcjIndexEnabled());
            if(installConfig.isPcjIndexEnabled()) {
                if(installConfig.getFluoPcjAppName().isPresent()) {
                    reader.println("   PCJ Updater Fluo Application Name: " + installConfig.getFluoPcjAppName().get());
                } else {
                    reader.println("   Not using a PCJ Updater Fluo Application");
                }
            }

            reader.println("");

            return promptBoolean("Continue with the install? (y/n) ", Optional.absent());
        }

        /**
         * Prompt the user for which Mongo specific features of Rya they want enabled.
         *
         * @param instanceName - The Rya instance name. (not null)
         * @return The value they entered.
         * @throws IOException There was a problem reading the values.
         */
        private InstallConfiguration promptMongoConfig(final String instanceName) throws IOException {
            requireNonNull(instanceName);

            final InstallConfiguration.Builder builder = InstallConfiguration.builder();

            String prompt = makeFieldPrompt("Use Free Text Indexing", true);
            final boolean enableFreeTextIndexing = promptBoolean(prompt, Optional.of(true));
            builder.setEnableFreeTextIndex( enableFreeTextIndexing );

            prompt = makeFieldPrompt("Use Temporal Indexing", true);
            final boolean useTemporalIndexing = promptBoolean(prompt, Optional.of(true));
            builder.setEnableTemporalIndex( useTemporalIndexing );

            return builder.build();
        }

        /**
         * Prompt the user asking them if they are sure they would like to do the
         * install.
         *
         * @param instanceName - The Rya instance name. (not null)
         * @param installConfig - The configuration that will be presented to the user. (not null)
         * @return The value they entered.
         * @throws IOException There was a problem reading the value.
         */
        private boolean promptMongoVerified(final String instanceName, final InstallConfiguration installConfig)  throws IOException {
            requireNonNull(instanceName);
            requireNonNull(installConfig);

            final ConsoleReader reader = getReader();
            reader.println();
            reader.println("A Rya instance will be installed using the following values:");
            reader.println("   Instance Name: " + instanceName);
            reader.println("   Use Free Text Indexing: " + installConfig.isFreeTextIndexEnabled());
            reader.println("   Use Temporal Indexing: " + installConfig.isTemporalIndexEnabled());
            reader.println("");

            return promptBoolean("Continue with the install? (y/n) ", Optional.absent());
        }
    }
}