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

import org.apache.rya.periodic.notification.api.Notification;
import org.apache.rya.periodic.notification.notification.BasicNotification;
import org.apache.rya.periodic.notification.notification.CommandNotification;
import org.apache.rya.periodic.notification.notification.PeriodicNotification;
import org.apache.rya.periodic.notification.notification.CommandNotification.Command;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

/**
 * {@link TypeAdapter} used to serialize and deserialize {@link CommandNotification}s.
 * This TypeAdapter is used in {@link CommandNotificationSerializer} for producing and 
 * consuming messages to and from Kafka.
 *
 */
public class CommandNotificationTypeAdapter
        implements JsonDeserializer<CommandNotification>, JsonSerializer<CommandNotification> {

    @Override
    public JsonElement serialize(CommandNotification arg0, Type arg1, JsonSerializationContext arg2) {
        JsonObject result = new JsonObject();
        result.add("command", new JsonPrimitive(arg0.getCommand().name()));
        Notification notification = arg0.getNotification();
        if (notification instanceof PeriodicNotification) {
            result.add("type", new JsonPrimitive(PeriodicNotification.class.getSimpleName()));
            PeriodicNotificationTypeAdapter adapter = new PeriodicNotificationTypeAdapter();
            result.add("notification",
                    adapter.serialize((PeriodicNotification) notification, PeriodicNotification.class, arg2));
        } else if (notification instanceof BasicNotification) {
            result.add("type", new JsonPrimitive(BasicNotification.class.getSimpleName()));
            BasicNotificationTypeAdapter adapter = new BasicNotificationTypeAdapter();
            result.add("notification",
                    adapter.serialize((BasicNotification) notification, BasicNotification.class, arg2));
        } else {
            throw new IllegalArgumentException("Invalid notification type.");
        }
        return result;
    }

    @Override
    public CommandNotification deserialize(JsonElement arg0, Type arg1, JsonDeserializationContext arg2)
            throws JsonParseException {

        JsonObject json = arg0.getAsJsonObject();
        Command command = Command.valueOf(json.get("command").getAsString());
        String type = json.get("type").getAsString();
        Notification notification = null;
        if (type.equals(PeriodicNotification.class.getSimpleName())) {
            notification = (new PeriodicNotificationTypeAdapter()).deserialize(json.get("notification"),
                    PeriodicNotification.class, arg2);
        } else if (type.equals(BasicNotification.class.getSimpleName())) {
            notification = (new BasicNotificationTypeAdapter()).deserialize(json.get("notification"),
                    BasicNotification.class, arg2);
        } else {
            throw new JsonParseException("Cannot deserialize Json");
        }

        return new CommandNotification(command, notification);
    }

}
