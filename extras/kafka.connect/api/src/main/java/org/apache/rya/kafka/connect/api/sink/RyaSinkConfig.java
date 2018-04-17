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
package org.apache.rya.kafka.connect.api.sink;

import static java.util.Objects.requireNonNull;

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Contains common configuration fields for a Rya Sinks.
 */
@DefaultAnnotation(NonNull.class)
public class RyaSinkConfig extends AbstractConfig {

    public static final String RYA_INSTANCE_NAME = "rya.instance.name";
    private static final String RYA_INSTANCE_NAME_DOC = "The name of the RYA instance that will be connected to.";

    /**
     * @param configDef - The configuration schema definition that will be updated to include
     *   this configuration's fields. (not null)
     */
    public static void addCommonDefinitions(final ConfigDef configDef) {
        requireNonNull(configDef);
        configDef.define(RYA_INSTANCE_NAME, Type.STRING, Importance.HIGH, RYA_INSTANCE_NAME_DOC);
    }

    /**
     * Constructs an instance of {@link RyaSinkConfig}.
     *
     * @param definition - Defines the schema of the configuration. (not null)
     * @param originals - The key/value pairs that define the configuration. (not null)
     */
    public RyaSinkConfig(final ConfigDef definition, final Map<?, ?> originals) {
        super(definition, originals);
    }

    /**
     * @return The name of the RYA instance that will be connected to.
     */
    public String getRyaInstanceName() {
        return super.getString(RYA_INSTANCE_NAME);
    }
}