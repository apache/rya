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
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;

import org.apache.rya.reasoning.OwlClass;
import org.apache.rya.reasoning.OwlProperty;
import org.apache.rya.reasoning.Schema;

import org.apache.hadoop.io.Writable;

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
        byte[] bytes = new byte[size];
        in.readFully(bytes);
        ObjectInputStream stream = new ObjectInputStream(
            new ByteArrayInputStream(bytes));
        try {
            Iterable<?> propList = (Iterable<?>) stream.readObject();
            Iterable<?> classList = (Iterable<?>) stream.readObject();
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
            e.printStackTrace();
        }
        stream.close();
    }
}
