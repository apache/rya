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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.rya.api.domain.RyaValue;
import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.impl.ListBindingSet;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Map;

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
    private static final ValueFactory VF = SimpleValueFactory.getInstance();

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

    private static Value makeValue(final String valueString, final IRI typeURI) {
        // Convert the String Value into a Value.
        if (typeURI.equals(XMLSchema.ANYURI)) {
            return VF.createIRI(valueString);
        } else {
            return VF.createLiteral(valueString, typeURI);
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
                final RyaValue ryaValue = RdfToRyaConversions.convertValue(binding.getValue());
                final String valueString = ryaValue.getData();
                final IRI type = ryaValue.getDataType();
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
                final IRI type = VF.createIRI(input.readString());
                valuesList.add(makeValue(valueString, type));
            }
            final BindingSet bindingSet = new ListBindingSet(namesList, valuesList);
            return new VisibilityBindingSet(bindingSet, visibility);
        }
    }
}
