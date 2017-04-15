package org.apache.rya.api.domain.serialization.kryo;
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
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.XMLSchema;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaURI;

/**
 * Kryo Serializer for {@link RyaStatement}s
 *
 */
public class RyaStatementSerializer extends Serializer<RyaStatement> {
    
    /**
     * Uses Kryo to write RyaStatement to {@lin Output}
     * @param kryo - writes statement to output
     * @param output - output stream that statement is written to
     * @param object - statement written to output
     */
    public static void writeToKryo(Kryo kryo, Output output, RyaStatement object) {
        Preconditions.checkNotNull(kryo);
        Preconditions.checkNotNull(output);
        Preconditions.checkNotNull(object);
        output.writeString(object.getSubject().getData());
        output.writeString(object.getPredicate().getData());
        output.writeString(object.getObject().getDataType().toString());
        output.writeString(object.getObject().getData());
        boolean hasContext = object.getContext() != null;
        output.writeBoolean(hasContext);
        if(hasContext){
            output.writeString(object.getContext().getData());
        }
        boolean shouldWrite = object.getColumnVisibility() != null;
        output.writeBoolean(shouldWrite);
        if(shouldWrite){
            output.writeInt(object.getColumnVisibility().length);
            output.writeBytes(object.getColumnVisibility());
        }
        shouldWrite = object.getQualifer() != null;
        output.writeBoolean(shouldWrite);
        if(shouldWrite){
            output.writeString(object.getQualifer());
        }
        shouldWrite = object.getTimestamp() != null;
        output.writeBoolean(shouldWrite);
        if(shouldWrite){
            output.writeLong(object.getTimestamp());
        }
        shouldWrite = object.getValue() != null;
        output.writeBoolean(shouldWrite);
        if(shouldWrite){
            output.writeBytes(object.getValue());
        }
    }   

    /**
     * Uses Kryo to write RyaStatement to {@lin Output}
     * @param kryo - writes statement to output
     * @param output - output stream that statement is written to
     * @param object - statement written to output
     */
    @Override
    public void write(Kryo kryo, Output output, RyaStatement object) {
        writeToKryo(kryo, output, object);
    }
    
    /**
     * Uses Kryo to read a RyaStatement from {@link Input}
     * @param kryo - reads statement from input
     * @param input - Input stream that statement is read from
     * @param type - Type read from input stream
     * @return - statement read from input stream
     */
    public static RyaStatement readFromKryo(Kryo kryo, Input input, Class<RyaStatement> type){
        return read(input);
    }

    /**
     * Reads RyaStatement from {@link Input} stream
     * @param input - input stream that statement is read from
     * @return - statement read from input stream
     */
    public static RyaStatement read(Input input){
        Preconditions.checkNotNull(input);
        String subject = input.readString();
        String predicate = input.readString();
        String objectType = input.readString();
        String objectValue = input.readString();
        RyaType value;
        if (objectType.equals(XMLSchema.ANYURI.toString())){
            value = new RyaURI(objectValue);
        }
        else {
            value = new RyaType(new URIImpl(objectType), objectValue);
        }
        RyaStatement statement = new RyaStatement(new RyaURI(subject), new RyaURI(predicate), value);
        int length = 0;
        boolean hasNextValue = input.readBoolean();
        if (hasNextValue){
            statement.setContext(new RyaURI(input.readString()));
        }
        hasNextValue = input.readBoolean();
        if (hasNextValue){
            length = input.readInt();
            statement.setColumnVisibility(input.readBytes(length));
        }
        hasNextValue = input.readBoolean();
        if (hasNextValue){
            statement.setQualifer(input.readString());
        }
        hasNextValue = input.readBoolean();
        if (hasNextValue){
            statement.setTimestamp(input.readLong());
        }
        hasNextValue = input.readBoolean();
        if (hasNextValue){
            length = input.readInt();
            statement.setValue(input.readBytes(length));
        }

        return statement;
    }

    /**
     * Uses Kryo to read a RyaStatement from {@link Input}
     * @param kryo - reads statement from input
     * @param input - Input stream that statement is read from
     * @param type - Type read from input stream
     * @return - statement read from input stream
     */
    @Override
    public RyaStatement read(Kryo kryo, Input input, Class<RyaStatement> type) {
        return readFromKryo(kryo, input, type);
    }

}
