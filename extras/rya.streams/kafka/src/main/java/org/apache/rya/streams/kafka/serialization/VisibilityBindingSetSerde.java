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
package org.apache.rya.streams.kafka.serialization;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.rya.api.model.VisibilityBindingSet;

/**
 * Provides a {@link Serializer} and {@link Deserializer} for {@link VisibilityBindingSet}s.
 */
public class VisibilityBindingSetSerde implements Serde<VisibilityBindingSet> {

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        // Nothing to do.
    }

    @Override
    public Serializer<VisibilityBindingSet> serializer() {
        return new VisibilityBindingSetSerializer();
    }

    @Override
    public Deserializer<VisibilityBindingSet> deserializer() {
        return new VisibilityBindingSetDeserializer();
    }

    @Override
    public void close() {
        // Nothing to do.
    }
}