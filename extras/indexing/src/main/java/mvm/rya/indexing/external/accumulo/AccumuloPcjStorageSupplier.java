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
package mvm.rya.indexing.external.accumulo;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.accumulo.core.client.Connector;
import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;

import mvm.rya.indexing.external.PrecomputedJoinIndexerConfig;
import mvm.rya.indexing.external.PrecomputedJoinIndexerConfig.PrecomputedJoinStorageType;

/**
 * Creates instances of {@link AccumuloPcjStorage} using the values found in a {@link Configuration}.
 */
public class AccumuloPcjStorageSupplier implements Supplier<AccumuloPcjStorage> {

    private final Supplier<Configuration> configSupplier;
    private final Supplier<Connector> accumuloSupplier;

    /**
     * Constructs an instance of {@link AccumuloPcjStorageSupplier}.
     *
     * @param configSupplier - Configures the {@link AccumuloPcjStorage} that is
     *   supplied by this class. (not null)
     * @param accumuloSupplier - Provides the {@link Connector} that is used by
     *   the {@link AccumuloPcjStorage} that is supplied by this class. (not null)
     */
    public AccumuloPcjStorageSupplier(
            final Supplier<Configuration> configSupplier,
            final Supplier<Connector> accumuloSupplier) {
        this.configSupplier = checkNotNull(configSupplier);
        this.accumuloSupplier = checkNotNull(accumuloSupplier);
    }

    @Override
    public AccumuloPcjStorage get() {
        // Ensure a configuration has been set.
        final Configuration config = configSupplier.get();
        checkNotNull(config, "Could not create a AccumuloPcjStorage because the application's configuration has not been provided yet.");

        // Ensure the correct storage type has been set.
        final PrecomputedJoinIndexerConfig indexerConfig = new PrecomputedJoinIndexerConfig(config);

        final Optional<PrecomputedJoinStorageType> storageType = indexerConfig.getPcjStorageType();
        checkArgument(storageType.isPresent() && (storageType.get() == PrecomputedJoinStorageType.ACCUMULO),
                "This supplier requires the '" + PrecomputedJoinIndexerConfig.PCJ_STORAGE_TYPE +
                "' value be set to '" + PrecomputedJoinStorageType.ACCUMULO + "'.");

        // Ensure the Accumulo connector has been set.
        final Connector accumuloConn = accumuloSupplier.get();
        checkNotNull(accumuloConn, "The Accumulo Connector must be set before initializing the AccumuloPcjStorage.");

        final String ryaInstanceName = new AccumuloPcjStorageConfig(config).getRyaInstanceName();
        return new AccumuloPcjStorage(accumuloConn, ryaInstanceName);
    }
}