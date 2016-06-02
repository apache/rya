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
package mvm.rya.indexing.external;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.hadoop.conf.Configuration;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage;
import org.apache.rya.indexing.pcj.update.PrecomputedJoinUpdater;

import com.google.common.base.Optional;

import mvm.rya.api.persist.index.RyaSecondaryIndexer;

/**
 * Inspects the {@link Configuration} object that is provided to all instances
 * of {@link RyaSecondaryIndexer} to provide {@link PrecomputedJoinIndexer}
 * specific values.
 */
@ParametersAreNonnullByDefault
public class PrecomputedJoinIndexerConfig {

    /**
     * Enumerates the different methodologies implemented to store the PCJ indices.
     */
    public static enum PrecomputedJoinStorageType {
        /**
         * Stores each PCJ within an Accumulo table.
         */
        ACCUMULO;
    }

    /**
     * Enumerates the different methodologies implemented to update the PCJ indices.
     */
    public static enum PrecomputedJoinUpdaterType {
        /**
         * Incrementally updates the PCJs is pseudo-realtime new adds/deletes are encountered.
         */
        FLUO;
    }

    // Indicates which implementation of PrecomputedJoinStorage to use.
    public static final String PCJ_STORAGE_TYPE = "rya.indexing.pcj.storageType";

    // Indicates which implementation of PrecomputedJoinUpdater to use.
    public static final String PCJ_UPDATER_TYPE = "rya.indexing.pcj.updaterType";

    // The configuration object that is provided to Secondary Indexing implementations.
    private final Configuration config;

    /**
     * Constructs an instance of {@link PrecomputedJoinIndexerConfig}.
     *
     * @param config - The {@link Configuration} object that is provided to
     *   all instance of {@link RyaSecondaryIndexer}. It will be inspected
     *   for {@link PrecomputedJoinIndexer} specific values. (not null)
     */
    public PrecomputedJoinIndexerConfig(final Configuration config) {
        this.config = checkNotNull(config);
    }

    /**
     * @return The type of {@link PrecomputedJoinStorage} to use.
     */
    public Optional<PrecomputedJoinStorageType> getPcjStorageType() {
        final String storageTypeString = config.get(PCJ_STORAGE_TYPE);
        if(storageTypeString == null) {
            return Optional.absent();
        }

        final PrecomputedJoinStorageType storageType = PrecomputedJoinStorageType.valueOf(storageTypeString);
        return Optional.fromNullable(storageType);
    }

    /**
     * @return The type of {@link PrecomputedJoinUpdater} to use.
     */
    public Optional<PrecomputedJoinUpdaterType> getPcjUpdaterType() {
        final String updaterTypeString = config.get(PCJ_UPDATER_TYPE);
        if(updaterTypeString == null) {
            return Optional.absent();
        }

        final PrecomputedJoinUpdaterType updaterType = PrecomputedJoinUpdaterType.valueOf(updaterTypeString);
        return Optional.fromNullable(updaterType);
    }

    /**
     * @return The configuration object that has been wrapped.
     */
    public Configuration getConfig() {
        return config;
    }
}