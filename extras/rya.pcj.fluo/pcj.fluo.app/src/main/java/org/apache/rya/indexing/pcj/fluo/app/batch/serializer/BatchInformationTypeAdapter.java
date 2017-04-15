package org.apache.rya.indexing.pcj.fluo.app.batch.serializer;
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
import java.lang.reflect.Type;

import org.apache.log4j.Logger;
import org.apache.rya.indexing.pcj.fluo.app.batch.BatchInformation;
import org.apache.rya.indexing.pcj.fluo.app.batch.JoinBatchInformation;
import org.apache.rya.indexing.pcj.fluo.app.batch.SpanBatchDeleteInformation;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

/**
 * JsonSerializer/JsonDeserializer for serializing/deserializing
 * {@link BatchInformation} objects. This makes use of the
 * {@link BatchInformationTypeAdapterFactory} to retrieve the appropriate
 * JsonSerializer/JsonDeserializer given the class name of the particular
 * implementation of BatchInformation.
 *
 */
public class BatchInformationTypeAdapter implements JsonSerializer<BatchInformation>, JsonDeserializer<BatchInformation> {

    private static final Logger log = Logger.getLogger(BatchInformationTypeAdapter.class);
    private static final BatchInformationTypeAdapterFactory factory = new BatchInformationTypeAdapterFactory();

    @Override
    public BatchInformation deserialize(JsonElement arg0, Type arg1, JsonDeserializationContext arg2) throws JsonParseException {
        try {
            JsonObject json = arg0.getAsJsonObject();
            String type = json.get("class").getAsString();
            JsonDeserializer<? extends BatchInformation> deserializer = factory.getDeserializerFromName(type);
            return deserializer.deserialize(arg0, arg1, arg2);
        } catch (Exception e) {
            log.trace("Unable to deserialize JsonElement: " + arg0);
            log.trace("Returning an empty Batch");
            throw new JsonParseException(e);
        }
    }

    @Override
    public JsonElement serialize(BatchInformation batch, Type arg1, JsonSerializationContext arg2) {
        JsonSerializer<? extends BatchInformation> serializer = factory.getSerializerFromName(batch.getClass().getName());
        
        if(batch instanceof SpanBatchDeleteInformation) {
            return ((SpanBatchInformationTypeAdapter) serializer).serialize((SpanBatchDeleteInformation) batch, arg1, arg2);
        } else {
            return ((JoinBatchInformationTypeAdapter) serializer).serialize((JoinBatchInformation) batch, arg1, arg2);
        }
    }

}