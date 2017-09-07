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
package org.apache.rya.indexing.pcj.fluo.app.export.kafka;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.impl.ListBindingSet;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Serialize and deserialize a VisibilityBindingSet using Kyro lib. Great for exporting results of queries.
 *
 */
public class KryoVisibilityBindingSetSerializer implements Serializer<VisibilityBindingSet>, Deserializer<VisibilityBindingSet> {
    private static final ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo>() {
        @Override
        protected Kryo initialValue() {
            final Kryo kryo = new Kryo();
            return kryo;
        };
    };

    /**
     * Deserialize a VisibilityBindingSet using Kyro lib. Exporting results of queries.
     * If you don't want to use Kyro, here is an alternative:
     * return (new VisibilityBindingSetStringConverter()).convert(new String(data, StandardCharsets.UTF_8), null);
     *
     * @param topic
     *            ignored
     * @param data
     *            serialized bytes
     * @return deserialized instance of VisibilityBindingSet
     */
    @Override
    public VisibilityBindingSet deserialize(final String topic, final byte[] data) {
        final KryoInternalSerializer internalSerializer = new KryoInternalSerializer();
        final Input input = new Input(new ByteArrayInputStream(data));
        return internalSerializer.read(kryos.get(), input, VisibilityBindingSet.class);
    }

    /**
     * Ignored. Nothing to configure.
     */
    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        // Do nothing.
    }

    /**
     * Serialize a VisibilityBindingSet using Kyro lib. Exporting results of queries.
     * If you don't want to use Kyro, here is an alternative:
     * return (new VisibilityBindingSetStringConverter()).convert(data, null).getBytes(StandardCharsets.UTF_8);
     *
     * @param topic
     *            ignored
     * @param data
     *            serialize this instance
     * @return Serialized form of VisibilityBindingSet
     */
    @Override
    public byte[] serialize(final String topic, final VisibilityBindingSet data) {
        final KryoInternalSerializer internalSerializer = new KryoInternalSerializer();
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final Output output = new Output(baos);
        internalSerializer.write(kryos.get(), output, data);
        output.flush();
        final byte[] array = baos.toByteArray();
        return array;
    }

    /**
     * Ignored. Nothing to close.
     */
    @Override
    public void close() {
        // Do nothing.
    }

    private static Value makeValue(final String valueString, final URI typeURI) {
        // Convert the String Value into a Value.
        final ValueFactory valueFactory = ValueFactoryImpl.getInstance();
        if (typeURI.equals(XMLSchema.ANYURI)) {
            return valueFactory.createURI(valueString);
        } else {
            return valueFactory.createLiteral(valueString, typeURI);
        }
    }

    /**
     * De/Serialize a visibility binding set using the Kryo library.
     *
     */
    private static class KryoInternalSerializer extends com.esotericsoftware.kryo.Serializer<VisibilityBindingSet> {
        @Override
        public void write(final Kryo kryo, final Output output, final VisibilityBindingSet visBindingSet) {
            output.writeString(visBindingSet.getVisibility());
            // write the number count for the reader.
            output.writeInt(visBindingSet.size());
            for (final Binding binding : visBindingSet) {
                output.writeString(binding.getName());
                final RyaType ryaValue = RdfToRyaConversions.convertValue(binding.getValue());
                final String valueString = ryaValue.getData();
                final URI type = ryaValue.getDataType();
                output.writeString(valueString);
                output.writeString(type.toString());
            }
        }

        @Override
        public VisibilityBindingSet read(final Kryo kryo, final Input input, final Class<VisibilityBindingSet> aClass) {
            final String visibility = input.readString();
            final int bindingCount = input.readInt();
            final ArrayList<String> namesList = new ArrayList<String>(bindingCount);
            final ArrayList<Value> valuesList = new ArrayList<Value>(bindingCount);
            for (int i = bindingCount; i > 0; i--) {
                namesList.add(input.readString());
                final String valueString = input.readString();
                final URI type = new URIImpl(input.readString());
                valuesList.add(makeValue(valueString, type));
            }
            final BindingSet bindingSet = new ListBindingSet(namesList, valuesList);
            return new VisibilityBindingSet(bindingSet, visibility);
        }
    }
}
