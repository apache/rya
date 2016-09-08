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
package org.apache.rya.export.api.conf;

import org.apache.rya.export.MergeToolConfiguration;
import org.apache.rya.export.api.conf.MergeConfiguration.Builder;

/**
 * Helper for creating the immutable application configuration.
 */
public class ConfigurationAdapter {
    /**
     * @param jConfig - The JAXB generated configuration.
     * @return The {@link MergeConfiguration} used in the application
     * @throws MergeConfigurationException
     */
    public MergeConfiguration createConfig(final MergeToolConfiguration jConfig) throws MergeConfigurationException {
        final Builder configBuilder = getBuilder(jConfig);
        return configBuilder.build();
    }

    protected Builder getBuilder(final MergeToolConfiguration jConfig) {
        final Builder configBuilder = new Builder()
            .setParentHostname(jConfig.getParentHostname())
            .setParentUsername(jConfig.getParentUsername())
            .setParentPassword(jConfig.getParentPassword())
            .setParentTablePrefix(jConfig.getParentTablePrefix())
            .setParentRyaInstanceName(jConfig.getParentRyaInstanceName())
            .setParentTomcatUrl(jConfig.getParentTomcatUrl())
            .setParentDBType(jConfig.getParentDBType())
            .setParentPort(jConfig.getParentPort())
            .setChildHostname(jConfig.getChildHostname())
            .setChildUsername(jConfig.getChildUsername())
            .setChildPassword(jConfig.getChildPassword())
            .setChildTablePrefix(jConfig.getChildTablePrefix())
            .setChildRyaInstanceName(jConfig.getChildRyaInstanceName())
            .setChildTomcatUrl(jConfig.getChildTomcatUrl())
            .setChildDBType(jConfig.getChildDBType())
            .setChildPort(jConfig.getChildPort())
            .setMergePolicy(jConfig.getMergePolicy())
            .setUseNtpServer(jConfig.isUseNtpServer())
            .setNtpServerHost(jConfig.getNtpServerHost());
        return configBuilder;
    }
}
