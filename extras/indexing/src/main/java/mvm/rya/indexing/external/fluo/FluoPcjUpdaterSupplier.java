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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;

import io.fluo.api.client.FluoClient;
import io.fluo.api.client.FluoFactory;
import io.fluo.api.config.FluoConfiguration;
import mvm.rya.indexing.external.PrecomputedJoinIndexerConfig;
import mvm.rya.indexing.external.PrecomputedJoinIndexerConfig.PrecomputedJoinUpdaterType;

/**
 * Creates instances of {@link FluoPcjUpdater} using the values found in a {@link Configuration}.
 */
@ParametersAreNonnullByDefault
public class FluoPcjUpdaterSupplier implements Supplier<FluoPcjUpdater> {

    private final Supplier<Configuration> configSupplier;

    /**
     * Constructs an instance of {@link FluoPcjUpdaterSupplier}.
     *
     * @param configSupplier - Configures the {@link FluoPcjUpdater} that is supplied by this class. (not null)
     */
    public FluoPcjUpdaterSupplier(final Supplier<Configuration> configSupplier) {
        this.configSupplier = checkNotNull(configSupplier);
    }

    @Override
    public FluoPcjUpdater get() {
        final Configuration config = configSupplier.get();
        checkNotNull(config, "Could not create a FluoPcjUpdater because the application's configuration has not been provided yet.");

        // Ensure the correct updater type has been set.
        final PrecomputedJoinIndexerConfig indexerConfig = new PrecomputedJoinIndexerConfig(config);

        final Optional<PrecomputedJoinUpdaterType> updaterType = indexerConfig.getPcjUpdaterType();
        checkArgument(updaterType.isPresent() && (updaterType.get() == PrecomputedJoinUpdaterType.FLUO),
                "This supplier requires the '" + PrecomputedJoinIndexerConfig.PCJ_UPDATER_TYPE +
                "' value be set to '" + PrecomputedJoinUpdaterType.FLUO + "'.");

        final FluoPcjUpdaterConfig fluoUpdaterConfig = new FluoPcjUpdaterConfig( indexerConfig.getConfig() );

        // Make sure the required values are present.
        checkArgument(fluoUpdaterConfig.getFluoAppName().isPresent(), "Missing configuration: " + FluoPcjUpdaterConfig.FLUO_APP_NAME);
        checkArgument(fluoUpdaterConfig.getFluoZookeepers().isPresent(), "Missing configuration: " + FluoPcjUpdaterConfig.ACCUMULO_ZOOKEEPERS);
        checkArgument(fluoUpdaterConfig.getAccumuloZookeepers().isPresent(), "Missing configuration: " + FluoPcjUpdaterConfig.ACCUMULO_ZOOKEEPERS);
        checkArgument(fluoUpdaterConfig.getAccumuloInstance().isPresent(), "Missing configuration: " + FluoPcjUpdaterConfig.ACCUMULO_INSTANCE);
        checkArgument(fluoUpdaterConfig.getAccumuloUsername().isPresent(), "Missing configuration: " + FluoPcjUpdaterConfig.ACCUMULO_USERNAME);
        checkArgument(fluoUpdaterConfig.getAccumuloPassword().isPresent(), "Missing configuration: " + FluoPcjUpdaterConfig.ACCUMULO_PASSWORD);
        checkArgument(fluoUpdaterConfig.getStatementVisibility().isPresent(), "Missing configuration: " + FluoPcjUpdaterConfig.STATEMENT_VISIBILITY);

        // Fluo configuration values.
        final FluoConfiguration fluoClientConfig = new FluoConfiguration();
        fluoClientConfig.setApplicationName( fluoUpdaterConfig.getFluoAppName().get() );
        fluoClientConfig.setInstanceZookeepers( fluoUpdaterConfig.getFluoZookeepers().get() );

        // Accumulo Fluo Table configuration values.
        fluoClientConfig.setAccumuloZookeepers( fluoUpdaterConfig.getAccumuloZookeepers().get() );
        fluoClientConfig.setAccumuloInstance( fluoUpdaterConfig.getAccumuloInstance().get() );
        fluoClientConfig.setAccumuloUser( fluoUpdaterConfig.getAccumuloUsername().get() );
        fluoClientConfig.setAccumuloPassword( fluoUpdaterConfig.getAccumuloPassword().get() );

        final FluoClient fluoClient = FluoFactory.newClient(fluoClientConfig);
        final String statementVisibilities = fluoUpdaterConfig.getStatementVisibility().get();
        return new FluoPcjUpdater(fluoClient, statementVisibilities);
    }
}