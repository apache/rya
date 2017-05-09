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
package org.apache.rya.indexing.pcj.fluo.app;

import static java.util.Objects.requireNonNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.fluo.api.data.Bytes;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Serializes and deserializes a {@link VisibilityBindingSet} to and from {@link Bytes} objects.
 */
@DefaultAnnotation(NonNull.class)
public class VisibilityBindingSetSerDe {

    /**
     * Serializes a {@link VisibilityBindingSet} into a {@link Bytes} object.
     *
     * @param bindingSet - The binding set that will be serialized. (not null)
     * @return The serialized object.
     * @throws Exception A problem was encountered while serializing the object.
     */
    public Bytes serialize(final VisibilityBindingSet bindingSet) throws Exception {
        requireNonNull(bindingSet);

        final ByteArrayOutputStream boas = new ByteArrayOutputStream();
        try(final ObjectOutputStream oos = new ObjectOutputStream(boas)) {
            oos.writeObject(bindingSet);
        }

        return Bytes.of(boas.toByteArray());
    }

    /**
     * Deserializes a {@link VisibilityBindingSet} from a {@link Bytes} object.
     *
     * @param bytes - The bytes that will be deserialized. (not null)
     * @return The deserialized object.
     * @throws Exception A problem was encountered while deserializing the object.
     */
    public VisibilityBindingSet deserialize(final Bytes bytes) throws Exception {
        requireNonNull(bytes);

        try(final ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes.toArray()))) {
            final Object o = ois.readObject();
            if(o instanceof VisibilityBindingSet) {
                return (VisibilityBindingSet) o;
            } else {
                throw new Exception("Deserialized Object is not a VisibilityBindingSet. Was: " + o.getClass());
            }
        }
    }
}