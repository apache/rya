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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;

import mvm.rya.indexing.external.PrecomputedJoinIndexerConfig.PrecomputedJoinStorageType;
import mvm.rya.indexing.external.accumulo.AccumuloPcjStorageSupplier;

/**
 * Creates an instance of {@link PrecomputedJoinStorage} based on the application's configuration.
 */
public class PrecomputedJoinStorageSupplier implements Supplier<PrecomputedJoinStorage> {

    private final Supplier<Configuration> configSupplier;
    private final AccumuloPcjStorageSupplier accumuloSupplier;

    /**
     * Constructs an instance of {@link PrecomputedJoinStorageSupplier}.
     *
     * @param configSupplier - Provides access to the configuration of the
     *   application used to initialize the storage. (not null)
     * @param accumuloSupplier - Used to create an Accumulo instance of the
     *   storage if that is the configured type. (not null)
     */
    public PrecomputedJoinStorageSupplier(
            final Supplier<Configuration> configSupplier,
            final AccumuloPcjStorageSupplier accumuloSupplier) {
        this.configSupplier = checkNotNull(configSupplier);
        this.accumuloSupplier = checkNotNull(accumuloSupplier);
    }

    @Override
    public PrecomputedJoinStorage get() {
        // Ensure a configuration has been set.
        final Configuration config = configSupplier.get();
        checkNotNull(config, "Could not build the PrecomputedJoinStorage until the PrecomputedJoinIndexer has been configured.");

        final PrecomputedJoinIndexerConfig indexerConfig = new PrecomputedJoinIndexerConfig(config);

        // Ensure the storage type has been set.
        final Optional<PrecomputedJoinStorageType> storageType = indexerConfig.getPcjStorageType();
        checkArgument(storageType.isPresent(), "The '" + PrecomputedJoinIndexerConfig.PCJ_STORAGE_TYPE +
                "' property must have one of the following values: " + PrecomputedJoinStorageType.values());

        // Create and return the configured storage.
        switch(storageType.get()) {
            case ACCUMULO:
                return accumuloSupplier.get();

            default:
                throw new IllegalArgumentException("Unsupported PrecomputedJoinStorageType: " + storageType.get());
        }
    }
}