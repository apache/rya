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

import mvm.rya.indexing.external.PrecomputedJoinIndexer;
import mvm.rya.indexing.external.PrecomputedJoinIndexerConfig;

import org.apache.hadoop.conf.Configuration;
import org.apache.rya.indexing.pcj.update.PrecomputedJoinUpdater;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

/**
 * A factory for {@link Supplier}s used by {@link PrecomputedJoinIndexer} to
 * get all update strategies for precomputed joins for a given Rya instance.
 *
 */
public class PcjUpdaterSupplierFactory {

    private Supplier<Configuration> configSupplier;

    public PcjUpdaterSupplierFactory(Supplier<Configuration> configSupplier) {
        this.configSupplier = configSupplier;
    }

    public Supplier<PrecomputedJoinUpdater> getSupplier() {

        PrecomputedJoinIndexerConfig config = new PrecomputedJoinIndexerConfig(configSupplier.get());
        //TODO this should not be read from the config.  Instead,
        //this information should be retrieved from the RyaDetails table
        if(config.getUseFluoUpdater()) {
            return Suppliers.memoize(new FluoPcjUpdaterSupplier(configSupplier));
        }
        else {
            return Suppliers.memoize(new NoOpUpdaterSupplier());
        }

    }

}
