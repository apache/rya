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
package org.apache.rya.indexing.pcj.fluo.app.query;

import java.lang.reflect.Type;

import org.apache.log4j.Logger;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;

import com.google.common.base.Optional;
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

/**
 * Implementation of {@link CommonNodeMetadata} for storing metadata nodeIds and
 * {@link VariableOrder}s.  This class is used primarily for storing Aggregation State
 * Metadata.
 */
public class CommonNodeMetadataImpl extends CommonNodeMetadata {

    /**
     * Creates an instance of CommonNodeMetadata from the provided nodeId and VariableOrder
     * @param nodeId - id of the node
     * @param varOrder - VariableOrder used to order Binding values
     */
    public CommonNodeMetadataImpl(String nodeId, VariableOrder varOrder) {
        super(nodeId, varOrder);
    }

    /**
     * Copy constructor for CommonNodeMetadataImpl.
     * @param metadata - CommonNodeMetadata instance whose values will be copied
     */
    public CommonNodeMetadataImpl(CommonNodeMetadataImpl metadata) {
        super(metadata.getNodeId(), metadata.getVariableOrder());
    }

    /**
     * Reads/Writes instances of {@link CommonNodeMetadataImpl} to/from Json Strings.
     */
    public static class CommonNodeMetadataSerDe {

        private static Logger log = Logger.getLogger(CommonNodeMetadataSerDe.class);
        private static Gson gson = new GsonBuilder().registerTypeAdapter(CommonNodeMetadataImpl.class, new CommonNodeTypeAdapter())
                .create();

        /**
         * Serializes the CommonNodeMetadata instance to a JSON String
         * @param metadata - CommonNodeMetadata instance to be serialized
         * @return - serialized JSON String representation of metadata object
         */
        public static String serialize(CommonNodeMetadataImpl metadata) {
            try {
                return gson.toJson(metadata);
            } catch (Exception e) {
                log.info("Unable to serialize BatchInformation: " + metadata);
                throw new RuntimeException(e);
            }
        }

        /**
         * Deserialize CommonNodeMetadata instance from a String
         * @param json - JSON String representation of CommonNodeMetadata
         * @return - deserialized CommonNodeMetadata object
         */
        public static Optional<CommonNodeMetadataImpl> deserialize(String json) {
            try {
                return Optional.of(gson.fromJson(json, CommonNodeMetadataImpl.class));
            } catch (Exception e) {
                log.info("Invalid String encoding.  BatchInformation cannot be deserialized.");
                return Optional.absent();
            }
        }
    }

    /**
     * Gson Type adapter to be used with {@link CommonNodeMetadataSerDe}.
     */
    public static class CommonNodeTypeAdapter implements JsonSerializer<CommonNodeMetadataImpl>, JsonDeserializer<CommonNodeMetadataImpl> {

        @Override
        public CommonNodeMetadataImpl deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
                throws JsonParseException {
            JsonObject object = json.getAsJsonObject();
            String nodeId = object.get("nodeId").getAsString();
            String varOrderString = object.get("varOrder").getAsString();
            VariableOrder varOrder = new VariableOrder();
            if (!varOrderString.isEmpty()) {
                varOrder = new VariableOrder(varOrderString);
            }

            return new CommonNodeMetadataImpl(nodeId, varOrder);
        }

        @Override
        public JsonElement serialize(CommonNodeMetadataImpl src, Type typeOfSrc, JsonSerializationContext context) {
            JsonObject result = new JsonObject();
            result.add("nodeId", new JsonPrimitive(src.getNodeId()));
            result.add("varOrder", new JsonPrimitive(src.getVariableOrder().toString()));
            return result;
        }
    }

}
