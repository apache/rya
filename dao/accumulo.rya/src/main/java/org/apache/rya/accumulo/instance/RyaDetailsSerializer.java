package org.apache.rya.accumulo.instance;

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

import static java.util.Objects.requireNonNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.regex.Pattern;

import org.apache.commons.io.serialization.ValidatingObjectInputStream;
import org.apache.rya.api.instance.RyaDetails;
import org.apache.rya.api.instance.RyaDetailsRepository.RyaDetailsRepositoryException;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Serializes {@link RyaDetails} instances.
 */
@DefaultAnnotation(NonNull.class)
public class RyaDetailsSerializer {

    /**
     * Serializes an instance of {@link RyaDetails}.
     *
     * @param details - The details that will be serialized. (not null)
     * @return The serialized details.
     */
    public byte[] serialize(final RyaDetails details) throws SerializationException {
        requireNonNull(details);

        try {
            final ByteArrayOutputStream stream = new ByteArrayOutputStream();
            new ObjectOutputStream(stream).writeObject( details );
            return stream.toByteArray();
        } catch (final IOException e) {
            throw new SerializationException("Could not serialize an instance of RyaDetails.", e);
        }
    }

    /**
     * Deserializes an instance of {@link RyaDetails}.
     *
     * @param bytes - The serialized for of a {@link RyaDetails}. (not null)
     * @return The deserialized object.
     */
    public RyaDetails deserialize(final byte[] bytes) throws SerializationException {
        requireNonNull(bytes);

        try (final ByteArrayInputStream stream = new ByteArrayInputStream(bytes); //
                        final ValidatingObjectInputStream vois = new ValidatingObjectInputStream(stream)
        //// this is how you find classes that you missed in the accept list
        // { @Override protected void invalidClassNameFound(String className) throws java.io.InvalidClassException {
        // System.out.println("vois.accept(" + className + ".class, ");};};
        ) {
            vois.accept(RyaDetails.class,
                            com.google.common.base.Optional.class, //
                            java.util.Date.class, //
                            java.lang.Enum.class);
            vois.accept("com.google.common.base.Present", //
                        "com.google.common.base.Absent", //
                        "com.google.common.collect.ImmutableMap$SerializedForm", //
                        "com.google.common.collect.ImmutableBiMap$SerializedForm", //
                        "com.google.common.collect.ImmutableList$SerializedForm", //
                        "[Ljava.lang.Object;");
            vois.accept(Pattern.compile("org\\.apache\\.rya\\.api\\.instance\\.RyaDetails.*"));

            final Object o = vois.readObject();

            if (!(o instanceof RyaDetails)) {
                throw new SerializationException("Wrong type of object was deserialized. Class: " + o.getClass().getName());
            }

            return (RyaDetails) o;

        } catch (final ClassNotFoundException | IOException e) {
            throw new SerializationException("Could not deserialize an instance of RyaDetails.", e);
        }
    }

    /**
     * Could not serialize an instance of {@link RyaDetails}.
     */
    public static class SerializationException extends RyaDetailsRepositoryException {
        private static final long serialVersionUID = 1L;

        public SerializationException(final String message) {
            super(message);
        }

        public SerializationException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }
}