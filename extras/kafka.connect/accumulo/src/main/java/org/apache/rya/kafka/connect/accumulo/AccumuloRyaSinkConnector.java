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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.rya.kafka.connect.api.sink.RyaSinkConnector;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A {@link RyaSinkConnector} that uses an Accumulo Rya backend when creating tasks.
 */
@DefaultAnnotation(NonNull.class)
public class AccumuloRyaSinkConnector extends RyaSinkConnector {

    @Nullable
    private AccumuloRyaSinkConfig config = null;

    @Override
    public void start(final Map<String, String> props) {
        requireNonNull(props);
        this.config = new AccumuloRyaSinkConfig( props );
    }

    @Override
    protected AbstractConfig getConfig() {
        if(config == null) {
            throw new IllegalStateException("The configuration has not been set yet. Invoke start(Map) first.");
        }
        return config;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return AccumuloRyaSinkTask.class;
    }

    @Override
    public ConfigDef config() {
        return AccumuloRyaSinkConfig.CONFIG_DEF;
    }
}