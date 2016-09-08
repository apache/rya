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
package org.apache.rya.export.api.conf.policy;

import org.apache.rya.export.MergeToolConfiguration;
import org.apache.rya.export.TimestampMergePolicyConfiguration;
import org.apache.rya.export.api.conf.ConfigurationAdapter;
import org.apache.rya.export.api.conf.MergeConfiguration;
import org.apache.rya.export.api.conf.MergeConfigurationException;
import org.apache.rya.export.api.conf.policy.TimestampPolicyMergeConfiguration.TimestampPolicyBuilder;

/**
 * Helper for creating the immutable application configuration that uses
 * Accumulo.
 */
public class TimestampPolicyConfigurationAdapter extends ConfigurationAdapter {
    /**
     * @param jConfig - The JAXB generated configuration.
     * @return The {@link MergeConfiguration} used in the application
     * @throws MergeConfigurationException
     */
    @Override
    public MergeConfiguration createConfig(final MergeToolConfiguration jConfig) throws MergeConfigurationException {
        final TimestampMergePolicyConfiguration timeConfig = (TimestampMergePolicyConfiguration) jConfig;
        final MergeConfiguration.Builder configBuilder = super.getBuilder(jConfig);
        final TimestampPolicyBuilder builder = new TimestampPolicyBuilder(configBuilder);
        builder.setToolStartTime(timeConfig.getToolStartTime());
        return builder.build();
    }
}
