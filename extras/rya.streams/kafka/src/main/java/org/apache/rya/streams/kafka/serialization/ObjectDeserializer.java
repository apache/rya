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

import java.io.IOException;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A Kafka {@link Deserializer} that is able to deserialize entities.
 *
 * @param T - The type of entity to deserialize.
 */
@DefaultAnnotation(NonNull.class)
public abstract class ObjectDeserializer<T> implements Deserializer<T> {

    private static final Logger log = LoggerFactory.getLogger(ObjectDeserializer.class);

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        // Nothing to do.
    }

    @Override
    public T deserialize(final String topic, final byte[] data) {
        if(data == null || data.length == 0) {
            // Returning null because that is the contract of this method.
            return null;
        }

        try {
            return ObjectSerialization.deserialize(data, getDeserializedClass());
        } catch (final ClassNotFoundException | ClassCastException | IOException e) {
            log.error("Could not deserialize some data into a " + getDeserializedClass().getName() + ". This data will be skipped.", e);

            // Returning null because that is the contract of this method.
            return null;
        }
    }

    @Override
    public void close() {
        // Nothing to do.
    }

    /**
     * @return - Used by the {@link ObjectSerialization#deserialize()} and the logger.
     */
    protected abstract Class<T> getDeserializedClass();
}