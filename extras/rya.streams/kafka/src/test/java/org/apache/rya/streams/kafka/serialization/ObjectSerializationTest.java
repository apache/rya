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

import static org.junit.Assert.assertEquals;

import java.io.EOFException;
import java.io.IOException;

import org.junit.Test;

/**
 * Unit tests the methods of {@link ObjectSerialization}.
 */
public class ObjectSerializationTest {

    @Test
    public void serializeAndDeserialize() throws IOException, ClassNotFoundException, ClassCastException {
        // Create the object that will be serialized.
        final Integer original = new Integer(5);

        // Serialize it.
        final byte[] data = ObjectSerialization.serialize(original);

        // Deserialize it.
        final Integer deserialized = ObjectSerialization.deserialize(data, Integer.class);

        // Show the deserialized value matches the original.
        assertEquals(original, deserialized);
    }

    @Test(expected = ClassCastException.class)
    public void wrongClass() throws IOException, ClassNotFoundException, ClassCastException {
        // Create the object that will be serialized.
        final Integer original = new Integer(5);

        // Serialize it.
        final byte[] data = ObjectSerialization.serialize(original);

        // Deserialize it.
        ObjectSerialization.deserialize(data, Double.class);
    }

    @Test(expected = EOFException.class)
    public void deserializeEmptyData() throws ClassNotFoundException, ClassCastException, IOException {
        // Deserialize it.
        ObjectSerialization.deserialize(new byte[0], Integer.class);
    }
}