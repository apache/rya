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
package org.apache.rya.periodic.notification.serialization;

import java.lang.reflect.Type;

import org.apache.rya.periodic.notification.notification.BasicNotification;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

/**
 * {@link TypeAdapter} for {@link BasicNotification}s.  Used in {@link CommandNotificationTypeAdapter} to
 * serialize {@link CommandNotification}s.  
 *
 */
public class BasicNotificationTypeAdapter implements JsonDeserializer<BasicNotification>, JsonSerializer<BasicNotification> {

    @Override
    public JsonElement serialize(BasicNotification arg0, Type arg1, JsonSerializationContext arg2) {
        JsonObject result = new JsonObject();
        result.add("id", new JsonPrimitive(arg0.getId()));
        return result;
    }

    @Override
    public BasicNotification deserialize(JsonElement arg0, Type arg1, JsonDeserializationContext arg2) throws JsonParseException {
        JsonObject json = arg0.getAsJsonObject();
        String id = json.get("id").getAsString();
        return new BasicNotification(id);
    }

}
