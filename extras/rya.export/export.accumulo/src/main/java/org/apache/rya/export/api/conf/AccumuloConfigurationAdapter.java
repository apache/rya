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

import org.apache.rya.export.JAXBAccumuloMergeConfiguration;
import org.apache.rya.export.accumulo.common.InstanceType;
import org.apache.rya.export.api.conf.AccumuloMergeConfiguration.AccumuloBuilder;

/**
 * Helper for creating the immutable application configuration that uses
 * Accumulo.
 */
public class AccumuloConfigurationAdapter {
    /**
     * @param jConfig - The JAXB generated configuration.
     * @return The {@link MergeConfiguration} used in the application
     * @throws MergeConfigurationException
     */
    public static AccumuloMergeConfiguration createConfig(final JAXBAccumuloMergeConfiguration jConfig) throws MergeConfigurationException {
        final AccumuloBuilder configBuilder = (AccumuloBuilder) new AccumuloBuilder()
        // Accumulo Properties
        .setParentZookeepers(jConfig.getParentZookeepers())
        .setParentAuths(jConfig.getParentAuths())
        .setParentInstanceType(InstanceType.fromName(jConfig.getParentInstanceType()))
        .setChildZookeepers(jConfig.getChildZookeepers())
        .setChildAuths(jConfig.getChildAuths())
        .setChildInstanceType(InstanceType.fromName(jConfig.getChildInstanceType()))
        // Base Properties
        .setParentHostname(jConfig.getParentHostname())
        .setParentUsername(jConfig.getParentUsername())
        .setParentPassword(jConfig.getParentPassword())
        .setParentRyaInstanceName(jConfig.getParentRyaInstanceName())
        .setParentDBType(jConfig.getParentDBType())
        .setParentPort(jConfig.getParentPort())
        .setParentTablePrefix(jConfig.getParentTablePrefix())
        .setParentTomcatUrl(jConfig.getParentTomcatUrl())
        .setChildHostname(jConfig.getChildHostname())
        .setChildUsername(jConfig.getChildUsername())
        .setChildPassword(jConfig.getChildPassword())
        .setChildRyaInstanceName(jConfig.getChildRyaInstanceName())
        .setChildDBType(jConfig.getChildDBType())
        .setChildPort(jConfig.getChildPort())
        .setChildTablePrefix(jConfig.getChildTablePrefix())
        .setChildTomcatUrl(jConfig.getChildTomcatUrl())
        .setMergePolicy(jConfig.getMergePolicy())
        .setUseNtpServer(jConfig.isUseNtpServer())
        .setNtpServerHost(jConfig.getNtpServerHost())
        .setToolStartTime(jConfig.getToolStartTime());
        return configBuilder.build();
    }
}
