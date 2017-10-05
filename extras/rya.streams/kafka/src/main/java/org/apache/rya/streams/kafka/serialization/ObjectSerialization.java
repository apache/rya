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

import static java.util.Objects.requireNonNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A utility class used to serialize objects using Java object serialization.
 */
@DefaultAnnotation(NonNull.class)
public class ObjectSerialization {

    /**
     * Serialize an object using Java object serialization.
     *
     * @param data - The object to serialize. (not null)
     * @return A byte[] representation of the object.
     * @throws IOException The object could not be serialized.
     */
    public static <T> byte[] serialize(final T data) throws IOException {
        requireNonNull(data);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try(final ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(data);
        }

        return baos.toByteArray();
    }

    /**
     * Deserialize an object using Java object deserialization.
     *
     * @param data - The data that will be deserialized. (not null)
     * @param clazz - The expected class of the deserialized object. (not null)
     * @return The object that was read from the data.
     * @throws IOException The object could not be deserialized.
     * @throws ClassNotFoundException The class of the deserialized object could not be found on the classpath.
     * @throws ClassCastException The deserialized object was of the wrong type.
     */
    @SuppressWarnings("unchecked")
    public static <T> T deserialize(final byte[] data, final Class<T> clazz) throws IOException, ClassNotFoundException, ClassCastException {
        requireNonNull(data);
        requireNonNull(clazz);

        try(ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data))) {
            final Object object = ois.readObject();

            if(!object.getClass().equals(clazz)) {
                throw new ClassCastException("Object was of type " + object.getClass().getName() +
                        ", but the expected type was " + clazz.getName() + ".");
            }

            return (T) object;
        }
    }
}