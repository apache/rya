package org.apache.rya.indexing.pcj.fluo.app.export.kafka;
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
import java.io.ByteArrayOutputStream;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.rya.api.domain.RyaSubGraph;
import org.apache.rya.api.domain.serialization.kryo.RyaSubGraphSerializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Kafka {@link Serializer} and {@link Deserializer} for {@link RyaSubGraph}s.
 *
 */
public class RyaSubGraphKafkaSerDe implements Serializer<RyaSubGraph>, Deserializer<RyaSubGraph> {

    private final Kryo kryo;
    
    public RyaSubGraphKafkaSerDe() {
        kryo = new Kryo();
        kryo.register(RyaSubGraph.class,new RyaSubGraphSerializer());
    }
    
  /**
   * Deserializes from bytes to a RyaSubGraph
   * @param bundleBytes - byte representation of RyaSubGraph
   * @return - Deserialized RyaSubGraph
   */
    public RyaSubGraph fromBytes(byte[] bundleBytes) {
        return kryo.readObject(new Input(bundleBytes), RyaSubGraph.class);
    }

    /**
     * Serializes RyaSubGraph to bytes
     * @param bundle - RyaSubGraph to be serialized
     * @return - serialized bytes from RyaSubGraph
     */
    public byte[] toBytes(RyaSubGraph bundle) {
        Output output = new Output(new ByteArrayOutputStream());
        kryo.writeObject(output, bundle, new RyaSubGraphSerializer());
        return output.getBuffer();
    }

    /**
     * Deserializes RyaSubGraph
     * @param topic - topic that data is associated with (no effect) 
     * @param bundleBytes - bytes to be deserialized
     * @return - deserialized RyaSubGraph
     */
    @Override
    public RyaSubGraph deserialize(String topic, byte[] bundleBytes) {
        return fromBytes(bundleBytes);
    }

    /**
     * Serializes RyaSubGraph
     * @param subgraph - subgraph to be serialized
     * @param topic - topic that data is associated with
     * @return - serialized bytes from subgraph
     */
    @Override
    public byte[] serialize(String topic, RyaSubGraph subgraph) {
        return toBytes(subgraph);
    }

    /**
     * Closes serializer (no effect)
     */
    @Override
    public void close() {
    }
    
    /**
     * Configures serializer (no effect)
     */
    @Override
    public void configure(Map<String, ?> arg0, boolean arg1) {
    }
}
