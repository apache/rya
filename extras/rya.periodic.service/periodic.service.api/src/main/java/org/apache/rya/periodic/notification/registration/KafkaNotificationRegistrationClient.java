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
package org.apache.rya.periodic.notification.registration;

import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.rya.periodic.notification.api.Notification;
import org.apache.rya.periodic.notification.api.PeriodicNotificationClient;
import org.apache.rya.periodic.notification.notification.BasicNotification;
import org.apache.rya.periodic.notification.notification.CommandNotification;
import org.apache.rya.periodic.notification.notification.CommandNotification.Command;
import org.apache.rya.periodic.notification.notification.PeriodicNotification;

/**
 *  Implementation of {@link PeriodicNotificaitonClient} used to register new notification
 *  requests with the PeriodicQueryService. 
 *
 */
public class KafkaNotificationRegistrationClient implements PeriodicNotificationClient {

    private KafkaProducer<String, CommandNotification> producer;
    private String topic;
    
    public KafkaNotificationRegistrationClient(String topic, KafkaProducer<String, CommandNotification> producer) {
        this.topic = topic;
        this.producer = producer;
    }
    
    @Override
    public void addNotification(PeriodicNotification notification) {
        processNotification(new CommandNotification(Command.ADD, notification));

    }

    @Override
    public void deleteNotification(BasicNotification notification) {
        processNotification(new CommandNotification(Command.DELETE, notification));
    }

    @Override
    public void deleteNotification(String notificationId) {
        processNotification(new CommandNotification(Command.DELETE, new BasicNotification(notificationId)));
    }

    @Override
    public void addNotification(String id, long period, long delay, TimeUnit unit) {
        Notification notification = PeriodicNotification.builder().id(id).period(period).initialDelay(delay).timeUnit(unit).build();
        processNotification(new CommandNotification(Command.ADD, notification));
    }
    
   
    private void processNotification(CommandNotification notification) {
        producer.send(new ProducerRecord<String, CommandNotification>(topic, notification.getId(), notification));
    }
    
    @Override
    public void close() {
        producer.close();
    }
    

}
