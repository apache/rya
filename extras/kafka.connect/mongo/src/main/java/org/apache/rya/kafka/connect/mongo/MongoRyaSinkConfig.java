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
package org.apache.rya.kafka.connect.mongo;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.rya.kafka.connect.api.sink.RyaSinkConfig;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A Kafka Connect configuration that is used to configure {@link MongoRyaSinkConnector}s and {@link MongoRyaSinkTask}s.
 */
@DefaultAnnotation(NonNull.class)
public class MongoRyaSinkConfig extends RyaSinkConfig {

    public static final String HOSTNAME = "mongo.hostname";
    private static final String HOSTNAME_DOC = "The Mongo DB hostname the Sail connections will use.";

    public static final String PORT = "mongo.port";
    private static final String PORT_DOC = "The Mongo DB port the Sail connections will use.";

    public static final String USERNAME = "mongo.username";
    private static final String USERNAME_DOC = "The Mongo DB username the Sail connections will use.";

    public static final String PASSWORD = "mongo.password";
    private static final String PASSWORD_DOC = "The Mongo DB password the Sail connections will use.";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(HOSTNAME, Type.STRING, Importance.HIGH, HOSTNAME_DOC)
            .define(PORT, Type.INT, Importance.HIGH, PORT_DOC)
            .define(USERNAME, Type.STRING, "", Importance.HIGH, USERNAME_DOC)
            .define(PASSWORD, Type.PASSWORD, "", Importance.HIGH, PASSWORD_DOC);
    static {
        RyaSinkConfig.addCommonDefinitions(CONFIG_DEF);
    }

    /**
     * Constructs an instance of {@link MongoRyaSinkConfig}.
     *
     * @param originals - The key/value pairs that define the configuration. (not null)
     */
    public MongoRyaSinkConfig(final Map<?, ?> originals) {
        super(CONFIG_DEF, originals);
    }

    /**
     * @return The Mongo DB hostname the Sail connections wlll use.
     */
    public String getHostname() {
        return super.getString(HOSTNAME);
    }

    /**
     * @return The Mongo DB port the Sail connections will use.
     */
    public int getPort() {
        return super.getInt(PORT);
    }

    /**
     * @return The Mongo DB username the Sail connections will use.
     */
    public String getUsername() {
        return super.getString(USERNAME);
    }

    /**
     * @return The Mongo DB password the Sail connections will use.
     */
    public String getPassword() {
        return super.getPassword(PASSWORD).value();
    }
}