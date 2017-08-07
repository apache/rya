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
import org.apache.rya.api.domain.RyaSubGraph;
import org.apache.rya.api.domain.RyaStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Kryo based Serializer/Deserializer for {@link RyaSubGraph}.
 *
 */
public class RyaSubGraphSerializer extends Serializer<RyaSubGraph>{
    static final Logger log = LoggerFactory.getLogger(RyaSubGraphSerializer.class);

    /**
     * Use Kryo to write RyaSubGraph to {@link Output} stream
     * @param kryo - used to write subgraph to output stream
     * @param output - stream that subgraph is written to
     * @param object - subgraph written to output stream
     */
    @Override
    public void write(Kryo kryo, Output output, RyaSubGraph object) {
        output.writeString(object.getId());
        output.writeInt(object.getStatements().size());
        for (RyaStatement statement : object.getStatements()){
            RyaStatementSerializer.writeToKryo(kryo, output, statement);
        }
    }
    
    /**
     * Reads RyaSubGraph from {@link Input} stream
     * @param input - stream that subgraph is read from
     * @return subgraph read from input stream
     */
    public static RyaSubGraph read(Input input){
        RyaSubGraph bundle = new RyaSubGraph(input.readString());
        int numStatements = input.readInt();
        for (int i=0; i < numStatements; i++){
            bundle.addStatement(RyaStatementSerializer.read(input));
        }       
        return bundle;
    }

    /**
     * Uses Kryo to read RyaSubGraph from {@link Input} stream
     * @param kryo - used to read subgraph from input stream
     * @param input - stream that subgraph is read from
     * @param type - class of object to be read from input stream (RyaSubgraph)
     * @return subgraph read from input stream
     */
    @Override
    public RyaSubGraph read(Kryo kryo, Input input, Class<RyaSubGraph> type) {
        RyaSubGraph bundle = new RyaSubGraph(input.readString());
        int numStatements = input.readInt();
        
        for (int i=0; i < numStatements; i++){
            bundle.addStatement(RyaStatementSerializer.readFromKryo(kryo, input, RyaStatement.class));
        }       
        return bundle;
    }

}