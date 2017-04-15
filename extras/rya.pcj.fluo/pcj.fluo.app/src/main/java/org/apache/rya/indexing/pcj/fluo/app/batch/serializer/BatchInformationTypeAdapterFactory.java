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
import java.util.Map;

import org.apache.rya.indexing.pcj.fluo.app.batch.BatchInformation;
import org.apache.rya.indexing.pcj.fluo.app.batch.JoinBatchInformation;
import org.apache.rya.indexing.pcj.fluo.app.batch.SpanBatchDeleteInformation;

import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonSerializer;

/**
 * Factory the uses class names to return the appropriate {@link JsonSerializer} and {@link JsonDeserializer} for serializing
 * and deserializing {@link BatchInformation} objects.
 *
 */
public class BatchInformationTypeAdapterFactory {

    /**
     * Retrieve the appropriate {@link JsonSerializer} using the class name of the {@link BatchInformation} implementation
     * @param name - class name of the BatchInformation object
     * @return JsonSerializer for serializing BatchInformation objects
     */
    public JsonSerializer<? extends BatchInformation> getSerializerFromName(String name) {
        return serializers.get(name);
    }
    
    /**
     * Retrieve the appropriate {@link JsonDeserializer} using the class name of the {@link BatchInformation} implementation
     * @param name - class name of the BatchInformation object
     * @return JsonDeserializer for deserializing BatchInformation objects
     */
    public JsonDeserializer<? extends BatchInformation> getDeserializerFromName(String name) {
        return deserializers.get(name);
    }
    
    static final Map<String, JsonSerializer<? extends BatchInformation>> serializers = ImmutableMap.of(
            SpanBatchDeleteInformation.class.getName(), new SpanBatchInformationTypeAdapter(),
            JoinBatchInformation.class.getName(), new JoinBatchInformationTypeAdapter()
        );
    
    static final Map<String, JsonDeserializer<? extends BatchInformation>> deserializers = ImmutableMap.of(
            SpanBatchDeleteInformation.class.getName(), new SpanBatchInformationTypeAdapter(),
            JoinBatchInformation.class.getName(), new JoinBatchInformationTypeAdapter()
        );
    
}
