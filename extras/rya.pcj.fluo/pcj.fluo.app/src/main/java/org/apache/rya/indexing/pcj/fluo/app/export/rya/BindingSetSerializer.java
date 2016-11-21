package org.apache.rya.indexing.pcj.fluo.app.export.rya;

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

public class BindingSetSerializer implements Serializer<VisibilityBindingSet>, Deserializer<VisibilityBindingSet> {
    private static final ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo>() {
        @Override
        protected Kryo initialValue() {
            Kryo kryo = new Kryo();
            return kryo;
        };
    };

    @Override
    public VisibilityBindingSet deserialize(String topic, byte[] data) {
        KryoInternalSerializer internalSerializer = new KryoInternalSerializer();
        Input input = new Input(new ByteArrayInputStream(data));
        return internalSerializer.read(kryos.get(), input, VisibilityBindingSet.class);
        // return (new VisibilityBindingSetStringConverter()).convert(new String(data, StandardCharsets.UTF_8), null);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // Do nothing.
    }

    @Override
    public byte[] serialize(String topic, VisibilityBindingSet data) {
        KryoInternalSerializer internalSerializer = new KryoInternalSerializer();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Output output = new Output(baos);
        internalSerializer.write(kryos.get(), output, data);
        output.flush();
        byte[] array = baos.toByteArray();
        return array;
        // return (new VisibilityBindingSetStringConverter()).convert(data, null).getBytes(StandardCharsets.UTF_8);
    }

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

    private static class KryoInternalSerializer extends com.esotericsoftware.kryo.Serializer<VisibilityBindingSet> {
        @Override
        public void write(Kryo kryo, Output output, VisibilityBindingSet visBindingSet) {
            System.out.println("writing visBindingSet" + visBindingSet);
            output.writeString(visBindingSet.getVisibility());
            // write the number count for the reader.
            output.writeInt(visBindingSet.size());
            for (Binding binding : visBindingSet) {
                output.writeString(binding.getName());
                final RyaType ryaValue = RdfToRyaConversions.convertValue(binding.getValue());
                final String valueString = ryaValue.getData();
                final URI type = ryaValue.getDataType();
                output.writeString(valueString);
                output.writeString(type.toString());
            }
        }

        @Override
        public VisibilityBindingSet read(Kryo kryo, Input input, Class<VisibilityBindingSet> aClass) {
            System.out.println("reading visBindingSet");
            String visibility = input.readString();
            int bindingCount = input.readInt();
            ArrayList<String> namesList = new ArrayList<String>(bindingCount);
            ArrayList<Value> valuesList = new ArrayList<Value>(bindingCount);
            for (int i = bindingCount; i > 0; i--) {
                namesList.add(input.readString());
                String valueString = input.readString();
                final URI type = new URIImpl(input.readString());
                valuesList.add(makeValue(valueString, type));
            }
            BindingSet bindingSet = new ListBindingSet(namesList, valuesList);
            return new VisibilityBindingSet(bindingSet, visibility);
            
            // String id = input.readString();
            // VisibilityBindingSet.Type type = VisibilityBindingSet.Type.valueOf(input.readString());
            //
            // return new VisibilityBindingSet(new VisibilityBindingSet(id, type), input.readLong(true), input.readDouble());
        }
    }
}
