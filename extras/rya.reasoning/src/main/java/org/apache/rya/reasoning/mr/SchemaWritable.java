package org.apache.rya.reasoning.mr;

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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;

import org.apache.commons.io.serialization.ValidatingObjectInputStream;
import org.apache.hadoop.io.Writable;
import org.apache.rya.reasoning.OwlClass;
import org.apache.rya.reasoning.OwlProperty;
import org.apache.rya.reasoning.Schema;

public class SchemaWritable extends Schema implements Writable {
    @Override
    public void write(DataOutput out) throws IOException {
        ArrayList<OwlProperty> propList = new ArrayList<>(properties.values());
        ArrayList<OwlClass> classList = new ArrayList<>(classes.values());
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        ObjectOutputStream stream = new ObjectOutputStream(bytes);
        stream.writeObject(propList);
        stream.writeObject(classList);
        byte[] arr = bytes.toByteArray();
        stream.close();
        out.writeInt(arr.length);
        out.write(arr);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        if (size < 1)
            throw new Error("De-serializtion failed, count is less than one.");
        byte[] bytes = new byte[size];
        in.readFully(bytes);
        // ObjectInputStream stream = new ObjectInputStream(new ByteArrayInputStream(bytes));
        try (final ByteArrayInputStream bais = new ByteArrayInputStream(bytes); //
                        final ValidatingObjectInputStream vois = new ValidatingObjectInputStream(bais)
        // this is how you find classes that you missed in the vois.accept() list, below.
        // { @Override protected void invalidClassNameFound(String className) throws java.io.InvalidClassException {
        // System.out.println("vois.accept(" + className + ".class, ");};};
        ) {
            // this is a (hopefully) complete list of classes involved in a Schema to be serialized.
            // if a useful class is missing, throws an InvalidClassException.
            vois.accept(java.util.ArrayList.class, //
                            org.apache.rya.reasoning.OwlProperty.class, //
                            java.util.HashSet.class, //
                            org.apache.rya.reasoning.OwlClass.class, //
                            org.openrdf.model.impl.URIImpl.class, //
                            org.openrdf.model.impl.BNodeImpl.class); 
        try {
                Iterable<?> propList = (Iterable<?>) vois.readObject();
                Iterable<?> classList = (Iterable<?>) vois.readObject();
            for (Object p : propList) {
                OwlProperty prop = (OwlProperty) p;
                properties.put(prop.getURI(), prop);
            }
            for (Object c : classList) {
                OwlClass owlClass = (OwlClass) c;
                classes.put(owlClass.getURI(), owlClass);
            }
        }
        catch (ClassNotFoundException e) {
                throw new Error("While reading a schema object.");
            }
        }
    }
}
