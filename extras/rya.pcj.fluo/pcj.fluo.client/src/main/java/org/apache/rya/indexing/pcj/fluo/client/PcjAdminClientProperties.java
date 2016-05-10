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
package org.apache.rya.indexing.pcj.fluo.client;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Properties;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

/**
 * Interprets a {@link Properties} object so that it is easier to access
 * configuration values used by {@link PcjAdminClient}.
 */
@ParametersAreNonnullByDefault
public class PcjAdminClientProperties {

    // Properties that configure how Fluo will connect to Accumulo.
    public static final String ACCUMULO_ZOOKEEPERS = "rya.pcj.admin.client.accumulo.zooServers";
    public static final String ACCUMULO_INSTANCE = "rya.pcj.admin.client.accumulo.instanceName";
    public static final String ACCUMULO_USERNAME = "rya.pcj.admin.client.accumulo.username";
    public static final String ACCUMULO_PASSWORD = "rya.pcj.admin.client.accumulo.password";

    // Properties that configure how the client will interact with the Fluo app.
    public static final String FLUO_APP_NAME = "rya.pcj.admin.client.fluo.appName";

    // Properties taht configure how the client will interact with Rya.
    public static final String RYA_TABLE_PREFIX = "rya.pcj.admin.client.rya.tablePrefix";

    private final Properties props;

    /**
     * Constructs an instance of {@link PcjAdminClientProperties}.
     *
     * @param props - The properties this class will interpret. (not null)
     */
    public PcjAdminClientProperties(final Properties props) {
        this.props = checkNotNull(props);
    }

    /**
     * @return A comma delimited list of the Zookeeper servers that manage the
     *   Accumulo instance.
     */
    public @Nullable String getAccumuloZookeepers() {
        return props.getProperty(ACCUMULO_ZOOKEEPERS);
    }

    /**
     * @return The name of the Accumulo instance that is used by Rya and Fluo.
     */
    public @Nullable String getAccumuloInstance() {
        return props.getProperty(ACCUMULO_INSTANCE);
    }

    /**
     * @return The username the application will used to interact with Accumulo.
     */
    public @Nullable String getAccumuloUsername() {
        return props.getProperty(ACCUMULO_USERNAME);
    }

    /**
     * @return The password the application will use to interact with Accumulo.
     */
    public @Nullable String getAccumuloPassword() {
        return props.getProperty(ACCUMULO_PASSWORD);
    }

    /**
     * @return The name of the Fluo app that is incrementally maintaining PCJ results.
     */
    public @Nullable String getFluoAppName() {
        return props.getProperty(FLUO_APP_NAME);
    }

    /**
     * @return The prefix that is applied to the PCJ export table names and the
     *   Rya RDF tables for the Rya instance the Fluo app is exporting to.
     */
    public @Nullable String getRyaTablePrefix() {
        return props.getProperty(RYA_TABLE_PREFIX);
    }
}