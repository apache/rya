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
package org.apache.rya.api.domain;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

import org.apache.rya.api.persist.RdfDAOException;
import org.openrdf.model.impl.URIImpl;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class StatementMetadata {

    private static Gson gson = new GsonBuilder().enableComplexMapKeySerialization()
    .registerTypeHierarchyAdapter(RyaType.class, new RyaTypeAdapter()).create();;
    public static StatementMetadata EMPTY_METADATA = new StatementMetadata();

    private Map<RyaURI, RyaType> metadataMap = new HashMap<>();
    @SuppressWarnings("serial")
    private Type type = new TypeToken<Map<RyaURI, RyaType>>(){}.getType();

    public StatementMetadata() {}

    public StatementMetadata(byte[] value) throws RdfDAOException {
        try {
            if (value == null) {
                metadataMap = new HashMap<>();
            } else {
                // try to convert back to a json string and then back to the
                // map.
                String metadataString = new String(value, "UTF8");
                metadataMap = gson.fromJson(metadataString, type);
                if (metadataMap == null) {
                    metadataMap = new HashMap<>();
                }
            }
        } catch (UnsupportedEncodingException e) {
            throw new RdfDAOException(e);
        }
    }

    public StatementMetadata(String statementMetadata) {
        try {
            metadataMap = gson.fromJson(statementMetadata, type);
        } catch (Exception e) {
            throw new RdfDAOException(e);
        }
    }

    public void addMetadata(RyaURI key, RyaType value) {
        metadataMap.put(key, value);
    }

    public Map<RyaURI, RyaType> getMetadata() {
        return metadataMap;
    }

    public String toString() {
        return gson.toJson(metadataMap, type);
    }

    public byte[] toBytes() {
        // convert the map to a json string
        if (metadataMap.isEmpty()) {
            return null;
        }
        // TODO may want to cache this for performance reasons
        try {
            return toString().getBytes("UTF8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public static class RyaTypeAdapter implements JsonSerializer<RyaType>, JsonDeserializer<RyaType> {
        @Override
        public JsonElement serialize(RyaType src, Type typeOfSrc, JsonSerializationContext context) {
            JsonObject result = new JsonObject();
            result.add("type", new JsonPrimitive(src.getClass().getName()));
            
            StringBuilder builder = new StringBuilder();
            builder.append(src.getData()).append("\u0000").append(src.getDataType().toString());
            result.add("properties", new JsonPrimitive(builder.toString()));
     
            return result;
        }
     
        @Override
        public RyaType deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
            throws JsonParseException {
            JsonObject jsonObject = json.getAsJsonObject();
            String type = jsonObject.get("type").getAsString();
            JsonElement element = jsonObject.get("properties");
            String[] array = element.getAsJsonPrimitive().getAsString().split("\u0000");
            Preconditions.checkArgument(array.length == 2);
     
            if(type.equals(RyaURI.class.getName())){
                return new RyaURI(array[0]);
            } else if(type.equals(RyaType.class.getName())){
                RyaType ryaType = new RyaType(new URIImpl(array[1]), array[0]);
                return ryaType;
            } else {
                throw new IllegalArgumentException("Unparseable RyaType.");
            }
        }
    }

}
