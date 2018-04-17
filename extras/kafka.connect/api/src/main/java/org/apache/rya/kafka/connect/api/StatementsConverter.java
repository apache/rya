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
package org.apache.rya.kafka.connect.api;

import static java.util.Objects.requireNonNull;

import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.Converter;
import org.eclipse.rdf4j.model.Statement;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A plugin into the Kafka Connect platform that converts {@link Set}s of {@link Statement}s
 * to/from byte[]s by using a {@link StatementsSerializer} and a {@link StatementsDeserializer}.
 * <p/>
 * This converter does not use Kafka's Schema Registry.
 */
@DefaultAnnotation(NonNull.class)
public class StatementsConverter implements Converter {

    private static final StatementsSerializer SERIALIZER = new StatementsSerializer();
    private static final StatementsDeserializer DESERIALIZER = new StatementsDeserializer();

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        // This converter's behavior can not be tuned with configurations.
    }

    @Override
    public byte[] fromConnectData(final String topic, final Schema schema, final Object value) {
        requireNonNull(value);
        return SERIALIZER.serialize(topic, (Set<Statement>) value);
    }

    @Override
    public SchemaAndValue toConnectData(final String topic, final byte[] value) {
        requireNonNull(value);
        return new SchemaAndValue(null, DESERIALIZER.deserialize(topic, value));
    }
}