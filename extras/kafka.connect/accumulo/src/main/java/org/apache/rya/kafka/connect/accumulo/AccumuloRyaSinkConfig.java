/**
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
package org.apache.rya.kafka.connect.accumulo;

import static java.util.Objects.requireNonNull;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.rya.kafka.connect.api.sink.RyaSinkConfig;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A Kafka Connect configuration that is used to configure {@link AccumuloRyaSinkConnector}s
 * and {@link AccumuloRyaSinkTask}s.
 */
@DefaultAnnotation(NonNull.class)
public class AccumuloRyaSinkConfig extends RyaSinkConfig {

    public static final String ZOOKEEPERS = "accumulo.zookeepers";
    private static final String ZOOKEEPERS_DOC = "A comma delimited list of the Zookeeper server hostname/port pairs.";

    public static final String CLUSTER_NAME = "accumulo.cluster.name";
    private static final String CLUSTER_NAME_DOC = "The name of the Accumulo instance within Zookeeper.";

    public static final String USERNAME = "accumulo.username";
    private static final String USERNAME_DOC = "The Accumulo username the Sail connections will use.";

    public static final String PASSWORD = "accumulo.password";
    private static final String PASSWORD_DOC = "The Accumulo password the Sail connections will use.";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ZOOKEEPERS, Type.STRING, Importance.HIGH, ZOOKEEPERS_DOC)
            .define(CLUSTER_NAME, Type.STRING, Importance.HIGH, CLUSTER_NAME_DOC)
            .define(USERNAME, Type.STRING, Importance.HIGH, USERNAME_DOC)
            .define(PASSWORD, Type.PASSWORD, Importance.HIGH, PASSWORD_DOC);
    static {
        RyaSinkConfig.addCommonDefinitions(CONFIG_DEF);
    }

    /**
     * Constructs an instance of {@link AccumuloRyaSinkConfig}.
     *
     * @param originals - The key/value pairs that define the configuration. (not null)
     */
    public AccumuloRyaSinkConfig(final Map<?, ?> originals) {
        super(CONFIG_DEF, requireNonNull(originals));
    }

    /**
     * @return A comma delimited list of the Zookeeper server hostname/port pairs.
     */
    public String getZookeepers() {
        return super.getString(ZOOKEEPERS);
    }

    /**
     * @return The name of the Accumulo instance within Zookeeper.
     */
    public String getClusterName() {
        return super.getString(CLUSTER_NAME);
    }

    /**
     * @return The Accumulo username the Sail connections will use.
     */
    public String getUsername() {
        return super.getString(USERNAME);
    }

    /**
     * @return The Accumulo password the Sail connections will use.
     */
    public String getPassword() {
        return super.getPassword(PASSWORD).value();
    }
}