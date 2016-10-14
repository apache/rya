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
package org.apache.rya.indexing.external;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.rya.indexing.pcj.update.PrecomputedJoinUpdater;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;

import org.apache.rya.indexing.external.PrecomputedJoinIndexerConfig.PrecomputedJoinUpdaterType;
import org.apache.rya.indexing.external.fluo.FluoPcjUpdaterSupplier;

/**
 * Creates instance of {@link PrecomputedJoinUpdater} based on the application's configuration.
 */
public class PrecomputedJoinUpdaterSupplier implements Supplier<PrecomputedJoinUpdater> {

    private final Supplier<Configuration> configSupplier;
    private final FluoPcjUpdaterSupplier fluoSupplier;

    /**
     * Creates an instance of {@link PrecomputedJoinUpdaterSupplier}.
     *
     * @param configSupplier - Provides access to the configuration of the
     *   application used to initialize the updater. (not null)
     * @param fluoSupplier - Used to create a Fluo instace of the updater
     *   if that is the configured type. (not null)
     */
    public PrecomputedJoinUpdaterSupplier(
            final Supplier<Configuration> configSupplier,
            final FluoPcjUpdaterSupplier fluoSupplier) {
        this.configSupplier = checkNotNull(configSupplier);
        this.fluoSupplier = checkNotNull(fluoSupplier);
    }

    @Override
    public PrecomputedJoinUpdater get() {
        // Ensure a configuration has been set.
        final Configuration config = configSupplier.get();
        checkNotNull(config, "Can not build the PrecomputedJoinUpdater until the PrecomputedJoinIndexer has been configured.");

        final PrecomputedJoinIndexerConfig indexerConfig = new PrecomputedJoinIndexerConfig(config);

        // Ensure an updater type has been set.
        final Optional<PrecomputedJoinUpdaterType> updaterType = indexerConfig.getPcjUpdaterType();
        checkArgument(updaterType.isPresent(), "The '" + PrecomputedJoinIndexerConfig.PCJ_UPDATER_TYPE +
                "' property must have one of the following values: " + PrecomputedJoinUpdaterType.values());

        // Create and return the configured updater.
        switch(updaterType.get()) {
            case FLUO:
                return fluoSupplier.get();

            default:
                throw new IllegalArgumentException("Unsupported PrecomputedJoinUpdaterType: " + updaterType.get());
        }
    }
}