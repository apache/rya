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
package org.apache.rya.api.client;

import static java.util.Objects.requireNonNull;

import java.util.Objects;

import edu.umd.cs.findbugs.annotations.Nullable;
import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;
import net.jcip.annotations.Immutable;

import com.google.common.base.Optional;

/**
 * Installs a new instance of Rya.
 */
@DefaultAnnotation(NonNull.class)
public interface Install {

    /**
     * Install a new instance of Rya.
     *
     * @param instanceName - Indicates the name of the Rya instance to install. (not null)
     * @param installConfig - Configures how the Rya instance will operate. The
     *   instance name that is in this variable must match the {@code instanceName}. (not null)
     * @throws DuplicateInstanceNameException A Rya instance already exists for the provided name.
     * @throws RyaClientException Something caused the command to fail.
     */
    public void install(final String instanceName, final InstallConfiguration installConfig) throws DuplicateInstanceNameException, RyaClientException;

    /**
     * A Rya instance already exists for the provided name.
     */
    public static class DuplicateInstanceNameException extends RyaClientException {
        private static final long serialVersionUID = 1L;

        public DuplicateInstanceNameException(final String message) {
            super(message);
        }
    }

    /**
     * Configures how an instance of Rya will be configured when it is installed.
     */
    @Immutable
    @DefaultAnnotation(NonNull.class)
    public static class InstallConfiguration {

        private final boolean enableTableHashPrefix;
        private final boolean enableFreeTextIndex;
        private final boolean enableGeoIndex;
        private final boolean enableEntityCentricIndex;
        private final boolean enableTemporalIndex;
        private final boolean enableGeoTemporalIndex;
        private final boolean enablePcjIndex;
        private final Optional<String> fluoPcjAppName;

        /**
         * Use a {@link Builder} to create instances of this class.
         */
        private InstallConfiguration(
                final boolean enableTableHashPrefix,
                final boolean enableFreeTextIndex,
                final boolean enableGeoIndex,
                final boolean enableEntityCentricIndex,
                final boolean enableTemporalIndex,
                final boolean enableGeoTemporalIndex,
                final boolean enablePcjIndex,
                final Optional<String> fluoPcjAppName) {
            this.enableTableHashPrefix = requireNonNull(enableTableHashPrefix);
            this.enableFreeTextIndex = requireNonNull(enableFreeTextIndex);
            this.enableGeoIndex = requireNonNull(enableGeoIndex);
            this.enableEntityCentricIndex = requireNonNull(enableEntityCentricIndex);
            this.enableTemporalIndex = requireNonNull(enableTemporalIndex);
            this.enableGeoTemporalIndex = requireNonNull(enableGeoTemporalIndex);
            this.enablePcjIndex = requireNonNull(enablePcjIndex);
            this.fluoPcjAppName = requireNonNull(fluoPcjAppName);
        }

        /**
         * @return Whether or not the installed instance of Rya will include table prefix hashing.
         */
        public boolean isTableHashPrefixEnabled() {
            return enableTableHashPrefix;
        }

        /**
         * @return Whether or not the installed instance of Rya will maintain a Free Text index.
         */
        public boolean isFreeTextIndexEnabled() {
            return enableFreeTextIndex;
        }

        /**
         * @return Whether or not the installed instance of Rya will maintain a Geospatial index.
         */
        public boolean isGeoIndexEnabled() {
            return enableGeoIndex;
        }

        /**
         * @return Whether or not the installed instance of Rya will maintain an Entity Centric index.
         */
        public boolean isEntityCentrixIndexEnabled() {
            return enableEntityCentricIndex;
        }

        /**
         * @return Whether or not the installed instance of Rya will maintain a Temporal index.
         */
        public boolean isTemporalIndexEnabled() {
            return enableTemporalIndex;
        }

        /**
         * @return Whether or not the installed instance of Rya will maintain a Geo Temporal index.
         */
        public boolean isGeoTemporalIndexEnabled() {
            return enableGeoTemporalIndex;
        }

        /**
         * @return Whether or not the installed instance of Rya will maintain a PCJ index.
         */
        public boolean isPcjIndexEnabled() {
            return enablePcjIndex;
        }

        /**
         * @return The name of the Fluo application that updates this instance of Rya's PCJs.
         *  Optional because this does not have to be the update paradigm used.
         */
        public Optional<String> getFluoPcjAppName() {
            return fluoPcjAppName;
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    enableTableHashPrefix,
                    enableFreeTextIndex,
                    enableGeoIndex,
                    enableEntityCentricIndex,
                    enableTemporalIndex,
                    enableGeoTemporalIndex,
                    enablePcjIndex,
                    fluoPcjAppName);
        }

        @Override
        public boolean equals(final Object obj) {
            if(this == obj) {
                return true;
            }
            if(obj instanceof InstallConfiguration) {
                final InstallConfiguration config = (InstallConfiguration) obj;
                return enableTableHashPrefix == config.enableTableHashPrefix &&
                        enableFreeTextIndex == config.enableFreeTextIndex &&
                        enableGeoIndex == config.enableGeoIndex &&
                        enableEntityCentricIndex == config.enableEntityCentricIndex &&
                        enableTemporalIndex == config.enableTemporalIndex &&
                        enableGeoTemporalIndex == config.enableGeoTemporalIndex &&
                        enablePcjIndex == config.enablePcjIndex &&
                        Objects.equals(fluoPcjAppName, config.fluoPcjAppName);
            }
            return false;
        }

        /**
         * @return An empty instance of {@link Builder}.
         */
        public static Builder builder() {
            return new Builder();
        }

        /**
         * Builds instances of {@link InstallConfiguration}.
         */
        @DefaultAnnotation(NonNull.class)
        public static class Builder {
            private boolean enableTableHashPrefix = false;
            private boolean enableFreeTextIndex = false;
            private boolean enableGeoIndex = false;
            private boolean enableEntityCentricIndex = false;
            private boolean enableTemporalIndex = false;
            private boolean enableGeoTemporalIndex = false;
            private boolean enablePcjIndex = false;
            private String fluoPcjAppName = null;

            /**
             * @param enabled - Whether or not the installed instance of Rya will include table prefix hashing.
             * @return This {@link Builder} so that method invocations may be chained.
             */
            public Builder setEnableTableHashPrefix(final boolean enabled) {
                enableTableHashPrefix = enabled;
                return this;
            }

            /**
             * @param enabled - Whether or not the installed instance of Rya will maintain a Free Text index.
             * @return This {@link Builder} so that method invocations may be chained.
             */
            public Builder setEnableFreeTextIndex(final boolean enabled) {
                enableFreeTextIndex = enabled;
                return this;
            }

            /**
             * @param enabled - Whether or not the installed instance of Rya will maintain a Geospatial index.
             * @return This {@link Builder} so that method invocations may be chained.
             */
            public Builder setEnableGeoIndex(final boolean enabled) {
                enableGeoIndex = enabled;
                return this;
            }

            /**
             * @param enabled - Whether or not the installed instance of Rya will maintain an Entity Centric index.
             * @return This {@link Builder} so that method invocations may be chained.
             */
            public Builder setEnableEntityCentricIndex(final boolean enabled) {
                enableEntityCentricIndex = enabled;
                return this;
            }

            /**
             * @param enabled - Whether or not the installed instance of Rya will maintain a Temporal index.
             * @return This {@link Builder} so that method invocations may be chained.
             */
            public Builder setEnableTemporalIndex(final boolean enabled) {
                enableTemporalIndex = enabled;
                return this;
            }

            /**
             * install Geo temporal indexing using Geomesa
             * @param enabled True means created the index tables
             * @return this so you can chain more methods!
             */
            public Builder SetEnableGeoTemporalIndex
            (final boolean enabled) {
            	enableGeoTemporalIndex = enabled;
                return this;
            }

            /**
             * @param enabled - Whether or not the installed instance of Rya will maintain a PCJ index.
             * @return This {@link Builder} so that method invocations may be chained.
             */
            public Builder setEnablePcjIndex(final boolean enabled) {
                enablePcjIndex = enabled;
                return this;
            }

            public Builder setFluoPcjAppName(@Nullable final String fluoPcjAppName) {
                this.fluoPcjAppName = fluoPcjAppName;
                return this;
            }

            /**
             * @return Builds an instance of {@link InstallConfiguration} using this builder's values.
             */
            public InstallConfiguration build() {
                return new InstallConfiguration(
                        enableTableHashPrefix,
                        enableFreeTextIndex,
                        enableGeoIndex,
                        enableEntityCentricIndex,
                        enableTemporalIndex,
                        enableGeoTemporalIndex,
                        enablePcjIndex,
                        Optional.fromNullable(fluoPcjAppName));
            }
        }
    }
}