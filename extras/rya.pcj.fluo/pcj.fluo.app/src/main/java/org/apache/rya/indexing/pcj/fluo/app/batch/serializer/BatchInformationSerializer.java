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
import java.util.Optional;

import org.apache.log4j.Logger;
import org.apache.rya.indexing.pcj.fluo.app.batch.BatchInformation;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Serializer/Deserializer for {@link BatchInformation} objects that uses the Gson
 * Type Adapter {@link BatchInformationTypeAdapter} to do all of the serializing and deserializing.
 * 
 *
 */
public class BatchInformationSerializer {

    private static Logger log = Logger.getLogger(BatchInformationSerializer.class);
    private static Gson gson = new GsonBuilder().registerTypeHierarchyAdapter(BatchInformation.class, new BatchInformationTypeAdapter())
            .create();

    public static byte[] toBytes(BatchInformation arg0) {
        try {
            return gson.toJson(arg0).getBytes("UTF-8");
        } catch (Exception e) {
            log.info("Unable to serialize BatchInformation: " + arg0);
            throw new RuntimeException(e);
        }
    }

    public static Optional<BatchInformation> fromBytes(byte[] arg0) {
        try {
            String json = new String(arg0, "UTF-8");
            return Optional.of(gson.fromJson(json, BatchInformation.class));
        } catch (Exception e) {
            log.info("Invalid String encoding.  BatchInformation cannot be deserialized.");
            return Optional.empty();
        }
    }
}
