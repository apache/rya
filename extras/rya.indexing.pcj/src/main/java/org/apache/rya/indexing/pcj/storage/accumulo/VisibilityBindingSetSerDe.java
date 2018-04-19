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
package org.apache.rya.indexing.pcj.storage.accumulo;

import static java.util.Objects.requireNonNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;

import org.apache.commons.io.serialization.ValidatingObjectInputStream;
import org.apache.fluo.api.data.Bytes;
import org.apache.rya.api.model.VisibilityBindingSet;

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
        try (final ValidatingObjectInputStream vois = new ValidatingObjectInputStream(new ByteArrayInputStream(bytes.toArray()))) {
            // Perform input validation.  Only the following classes are allowed to be deserialized.
            vois.accept(
                    VisibilityBindingSet.class,
                    java.lang.Byte.class,
                    java.lang.Double.class,
                    java.lang.Float.class,
                    java.lang.Integer.class,
                    java.lang.Long.class,
                    java.lang.Number.class,
                    java.lang.Short.class,
                    java.math.BigDecimal.class,
                    java.math.BigInteger.class,
                    java.util.LinkedHashMap.class,
                    java.util.HashMap.class,
                    org.apache.rya.api.model.BindingSetDecorator.class,
                    org.eclipse.rdf4j.query.impl.SimpleBinding.class,
                    org.eclipse.rdf4j.model.impl.SimpleIRI.class,
                    org.eclipse.rdf4j.model.impl.SimpleLiteral.class,
                    org.eclipse.rdf4j.model.impl.IntegerLiteral.class,
                    org.eclipse.rdf4j.model.impl.DecimalLiteral.class,
                    org.eclipse.rdf4j.model.impl.NumericLiteral.class,
                    org.eclipse.rdf4j.query.AbstractBindingSet.class,
                    org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet.class,
                    org.eclipse.rdf4j.query.impl.MapBindingSet.class
                );
            vois.accept("[B");
            final Object o = vois.readObject();
            if(o instanceof VisibilityBindingSet) {
                return (VisibilityBindingSet) o;
            } else {
                throw new Exception("Deserialized Object is not a VisibilityBindingSet. Was: " + o.getClass());
            }
        }
    }
}