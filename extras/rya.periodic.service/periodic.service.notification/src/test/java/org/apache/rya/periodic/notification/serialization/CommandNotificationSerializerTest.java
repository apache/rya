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

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.rya.periodic.notification.notification.BasicNotification;
import org.apache.rya.periodic.notification.notification.CommandNotification;
import org.apache.rya.periodic.notification.notification.CommandNotification.Command;
import org.apache.rya.periodic.notification.notification.PeriodicNotification;
import org.apache.rya.periodic.notification.serialization.CommandNotificationSerializer;
import org.junit.Assert;
import org.junit.Test;

public class CommandNotificationSerializerTest {

    private CommandNotificationSerializer serializer = new CommandNotificationSerializer();
    private static final String topic = "topic";

    @Test
    public void basicSerializationTest() {
        PeriodicNotification notification = PeriodicNotification.builder().id(UUID.randomUUID().toString()).period(24)
                .timeUnit(TimeUnit.DAYS).initialDelay(1).build();
        CommandNotification command = new CommandNotification(Command.ADD, notification);
        Assert.assertEquals(command, serializer.deserialize(topic,serializer.serialize(topic, command)));

        PeriodicNotification notification1 = PeriodicNotification.builder().id(UUID.randomUUID().toString()).period(32)
                .timeUnit(TimeUnit.SECONDS).initialDelay(15).build();
        CommandNotification command1 = new CommandNotification(Command.ADD, notification1);
        Assert.assertEquals(command1, serializer.deserialize(topic,serializer.serialize(topic,command1)));

        PeriodicNotification notification2 = PeriodicNotification.builder().id(UUID.randomUUID().toString()).period(32)
                .timeUnit(TimeUnit.SECONDS).initialDelay(15).build();
        CommandNotification command2 = new CommandNotification(Command.ADD, notification2);
        Assert.assertEquals(command2, serializer.deserialize(topic,serializer.serialize(topic,command2)));

        BasicNotification notification3 = new BasicNotification(UUID.randomUUID().toString());
        CommandNotification command3 = new CommandNotification(Command.ADD, notification3);
        Assert.assertEquals(command3, serializer.deserialize(topic,serializer.serialize(topic,command3)));

    }

}
