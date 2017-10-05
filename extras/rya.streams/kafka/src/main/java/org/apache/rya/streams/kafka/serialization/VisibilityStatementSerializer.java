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

import org.apache.kafka.common.serialization.Serializer;
import org.apache.rya.api.model.VisibilityStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A Kafka {@link Serializer} that is able to serialize {@link VisibilityStatement}s using Java object serialization.
 */
@DefaultAnnotation(NonNull.class)
public class VisibilityStatementSerializer implements Serializer<VisibilityStatement> {

    private static final Logger log = LoggerFactory.getLogger(VisibilityStatementSerializer.class);

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        // Nothing to do.
    }

    @Override
    public byte[] serialize(final String topic, final VisibilityStatement data) {
        if(data == null) {
            return null;
        }

        try {
            return ObjectSerialization.serialize(data);
        } catch (final IOException e) {
            log.error("Unable to serialize a " + VisibilityStatement.class.getName() + ".", e);

            // Return null when there is an error since that is the contract of this method.
            return null;
        }
    }

    @Override
    public void close() {
        // Nothing to do.
    }
}