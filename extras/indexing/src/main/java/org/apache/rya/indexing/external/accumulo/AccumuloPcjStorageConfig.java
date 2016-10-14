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
package org.apache.rya.indexing.external.accumulo;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPcjStorage;

import org.apache.rya.api.RdfCloudTripleStoreConfiguration;

/**
 * Configuration values required to initialize a {@link AccumuloPcjStorage}.
 */
public class AccumuloPcjStorageConfig {

    private final RdfCloudTripleStoreConfiguration config;

    /**
     * Constructs an instance of {@link AccumuloPcjStorageConfig}.
     *
     * @param config - The configuration values that will be interpreted. (not null)
     */
    public AccumuloPcjStorageConfig(final Configuration config) {
        checkNotNull(config);

        // Wrapping the config with this class so that we can use it's getTablePrefix() method.
        this.config = new RdfCloudTripleStoreConfiguration(config) {
            @Override
            public RdfCloudTripleStoreConfiguration clone() {
                return null;
            }
        };
    }

    /**
     * @return The Rya Instance name the storage grants access to.
     */
    public String getRyaInstanceName() {
        return config.getTablePrefix();
    }
}