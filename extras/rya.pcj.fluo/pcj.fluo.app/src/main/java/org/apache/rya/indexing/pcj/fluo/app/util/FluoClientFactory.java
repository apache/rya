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
package org.apache.rya.indexing.pcj.fluo.app.util;

import java.util.Optional;

import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.core.client.FluoClientImpl;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;

/**
 * Factory for creating {@link FluoClient}s.
 *
 */
public class FluoClientFactory {

    /**
     * Creates a FluoClient
     * @param appName - name of Fluo application
     * @param tableName - name of Fluo table
     * @param conf - AccumuloConfiguration (must contain Accumulo User, Accumulo Instance, Accumulo Password, and Accumulo Zookeepers)
     * @return FluoClient for connecting to Fluo
     */
    public static FluoClient getFluoClient(String appName, Optional<String> tableName, AccumuloRdfConfiguration conf) {
        FluoConfiguration fluoConfig = new FluoConfiguration();
        fluoConfig.setAccumuloInstance(conf.getAccumuloInstance());
        fluoConfig.setAccumuloUser(conf.getAccumuloUser());
        fluoConfig.setAccumuloPassword(conf.getAccumuloPassword());
        fluoConfig.setInstanceZookeepers(conf.getAccumuloZookeepers() + "/fluo");
        fluoConfig.setAccumuloZookeepers(conf.getAccumuloZookeepers());
        fluoConfig.setApplicationName(appName);
        if (tableName.isPresent()) {
            fluoConfig.setAccumuloTable(tableName.get());
        } else {
            fluoConfig.setAccumuloTable(appName);
        }
        return new FluoClientImpl(fluoConfig);
    }
}
