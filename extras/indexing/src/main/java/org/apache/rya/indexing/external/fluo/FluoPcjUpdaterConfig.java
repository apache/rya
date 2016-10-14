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
package mvm.rya.indexing.external.fluo;

import static com.google.common.base.Preconditions.checkNotNull;
import mvm.rya.indexing.accumulo.ConfigUtils;

import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Optional;

/**
 * Configuration values required to initialize a {@link FluoPcjUpdater}.
 */
public final class FluoPcjUpdaterConfig {

    // Defines which Fluo application is running for this instance of Rya.
    public static final String FLUO_APP_NAME = ConfigUtils.FLUO_APP_NAME;

    // Values that define which Accumulo instance hosts the Fluo application's table.
    public static final String ACCUMULO_ZOOKEEPERS = ConfigUtils.CLOUDBASE_ZOOKEEPERS;
    public static final String ACCUMULO_INSTANCE = ConfigUtils.CLOUDBASE_INSTANCE;
    public static final String ACCUMULO_USERNAME = ConfigUtils.CLOUDBASE_USER;
    public static final String ACCUMULO_PASSWORD = ConfigUtils.CLOUDBASE_PASSWORD;

    // Values that define the visibilities associated with statement that are inserted by the fluo updater.
    public static final String STATEMENT_VISIBILITY = ConfigUtils.CLOUDBASE_AUTHS;

    // The configuration object that is provided to Secondary Indexing implementations.
    private final Configuration config;

    /**
     * Constructs an instance of {@link FluoPcjUpdaterConfig}.
     *
     * @param config - The configuration values that will be interpreted. (not null)
     */
    public FluoPcjUpdaterConfig(final Configuration config) {
        this.config = checkNotNull(config);
    }

    /**
     * @return The name of the Fluo Application this instance of RYA is
     *   using to incrementally update PCJs.
     */
    public Optional<String> getFluoAppName() {
        return Optional.fromNullable(config.get(FLUO_APP_NAME));
    }

    /**
     * This value is the {@link #getAccumuloInstance()} value appended with the
     * "/fluo" namespace.
     *
     * @return The zookeepers that are used to manage Fluo state. ({@code null}
     *   if not configured)
     */
    public Optional<String> getFluoZookeepers() {
        final Optional<String> accumuloZookeepers = getAccumuloZookeepers();
        if(!accumuloZookeepers.isPresent()) {
            return Optional.absent();
        }
        return Optional.of( accumuloZookeepers.get() + "/fluo" );
    }

    /**
     * @return The zookeepers used to connect to the Accumulo instance that
     *   is storing the state of the Fluo Application.
     */
    public Optional<String> getAccumuloZookeepers() {
        return Optional.fromNullable(config.get(ACCUMULO_ZOOKEEPERS));
    }

    /**
     * @return The instance name of the Accumulo instance that is storing
     *   the state of the Fluo Application.
     */
    public Optional<String> getAccumuloInstance() {
        return Optional.fromNullable(config.get(ACCUMULO_INSTANCE));
    }

    /**
     * @return The username the indexer will authenticate when connecting
     *   to the Accumulo instance that stores the state of the Fluo Application.
     */
    public Optional<String> getAccumuloUsername() {
        return Optional.fromNullable(config.get(ACCUMULO_USERNAME));
    }

    /**
     * @return The password the indexer will authenticate when connecting
     *   to the Accumulo instance that stores the state of the Fluo Application.
     */
    public Optional<String> getAccumuloPassword() {
        return Optional.fromNullable(config.get(ACCUMULO_PASSWORD));
    }

    /**
     * @return The visibility labels that will be attached to the statements
     *   that are inserted into the Fluo Application.
     */
    public Optional<String> getStatementVisibility() {
        return Optional.fromNullable(config.get(STATEMENT_VISIBILITY));
    }
}