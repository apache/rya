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

import org.apache.rya.export.AccumuloMergeToolConfiguration;
import org.apache.rya.export.DBType;
import org.apache.rya.export.InstanceType;
import org.apache.rya.export.MergeToolConfiguration;
import org.apache.rya.export.api.conf.AccumuloMergeConfiguration.AccumuloBuilder;

/**
 * Helper for creating the immutable application configuration that uses
 * Accumulo.
 */
public class AccumuloConfigurationAdapter extends ConfigurationAdapter {
    /**
     * @param genConfig - The JAXB generated configuration.
     * @return The {@link MergeConfiguration} used in the application
     * @throws MergeConfigurationException
     */
    @Override
    public MergeConfiguration createConfig(final MergeToolConfiguration genConfig) throws MergeConfigurationException {
        final AccumuloMergeToolConfiguration aConfig = (AccumuloMergeToolConfiguration) genConfig;
        final DBType parentType = aConfig.getParentDBType();
        final DBType childType = aConfig.getChildDBType();
        final MergeConfiguration.Builder configBuilder = super.getBuilder(aConfig);
        final AccumuloBuilder builder = new AccumuloBuilder(configBuilder);
        if(parentType == DBType.ACCUMULO) {
            verifyParentInstanceType(aConfig);
            builder.setParentZookeepers(aConfig.getParentZookeepers())
            .setParentAuths(aConfig.getParentAuths())
            .setParentInstanceType(aConfig.getParentInstanceType());
        }

        if(childType == DBType.ACCUMULO) {
            verifyChildInstanceType(aConfig);
            builder.setChildZookeepers(aConfig.getChildZookeepers())
            .setChildAuths(aConfig.getChildAuths())
            .setChildInstanceType(aConfig.getChildInstanceType());
        }

        return builder.build();
    }

    private void verifyParentInstanceType(final AccumuloMergeToolConfiguration aConfig) throws MergeConfigurationException {
        final InstanceType type = aConfig.getParentInstanceType();
        switch(type) {
        case DISTRIBUTION:
            final String auths = aConfig.getParentAuths();
            if(auths == null) {
                throw new MergeConfigurationException("Missing authorization level for parent accumulo.");
            }
            final String zookeepers = aConfig.getParentZookeepers();
            if(zookeepers == null) {
                throw new MergeConfigurationException("Missing zookeeper location(s) for parent accumulo.");
            }
            break;
        case MINI:
        case MOCK:
            break;
        }
    }

    private void verifyChildInstanceType(final AccumuloMergeToolConfiguration aConfig) throws MergeConfigurationException {
        final InstanceType type = aConfig.getChildInstanceType();
        switch(type) {
        case DISTRIBUTION:
            final String auths = aConfig.getChildAuths();
            if(auths == null) {
                throw new MergeConfigurationException("Missing authorization level for child accumulo.");
            }
            final String zookeepers = aConfig.getChildZookeepers();
            if(zookeepers == null) {
                throw new MergeConfigurationException("Missing zookeeper location(s) for child accumulo.");
            }
            break;
        case MINI:
        case MOCK:
            break;
        }
    }
}
