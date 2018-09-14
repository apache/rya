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
package org.apache.rya.export.client.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.rya.export.api.conf.MergeConfiguration;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;

/**
 * Adapts the {@link MergeConfiguration} to the hadoop {@link Configuration}.
 */
public class MergeConfigHadoopAdapter {
    public static MongoDBRdfConfiguration getMongoConfiguration(final MergeConfiguration config) {
        final MongoDBRdfConfiguration configuration = new MongoDBRdfConfiguration();
        configuration.setMongoHostname(config.getChildHostname());
        configuration.set(MongoDBRdfConfiguration.MONGO_PORT, config.getChildPort() + "");
        configuration.set(MongoDBRdfConfiguration.RYA_INSTANCE_NAME, config.getChildRyaInstanceName());
        return configuration;
    }
}
