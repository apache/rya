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
package org.apache.rya.periodic.notification.application;

import java.util.Objects;
import java.util.Properties;

import org.apache.rya.accumulo.AccumuloRdfConfiguration;


/**
 * Configuration object for creating a {@link PeriodicNotificationApplication}.
 */
public class PeriodicNotificationApplicationConfiguration extends AccumuloRdfConfiguration {

    public static final String RYA_PERIODIC_PREFIX = "rya.periodic.notification.";
    public static final String RYA_PCJ_PREFIX = "rya.pcj.";
    public static final String FLUO_APP_NAME = RYA_PCJ_PREFIX +"fluo.app.name";
    public static final String FLUO_TABLE_NAME = RYA_PCJ_PREFIX + "fluo.table.name";
    public static final String KAFKA_BOOTSTRAP_SERVERS = RYA_PERIODIC_PREFIX + "kafka.bootstrap.servers";
    public static final String NOTIFICATION_TOPIC = RYA_PERIODIC_PREFIX + "kafka.topic";
    public static final String NOTIFICATION_GROUP_ID = RYA_PERIODIC_PREFIX + "kafka.group.id";
    public static final String NOTIFICATION_CLIENT_ID = RYA_PERIODIC_PREFIX + "kafka.client.id";
    public static final String COORDINATOR_THREADS = RYA_PERIODIC_PREFIX + "coordinator.threads";
    public static final String PRODUCER_THREADS = RYA_PERIODIC_PREFIX + "producer.threads";
    public static final String EXPORTER_THREADS = RYA_PERIODIC_PREFIX + "exporter.threads";
    public static final String PROCESSOR_THREADS = RYA_PERIODIC_PREFIX + "processor.threads";
    public static final String PRUNER_THREADS = RYA_PERIODIC_PREFIX + "pruner.threads";

    public PeriodicNotificationApplicationConfiguration() {}

    /**
     * Creates an PeriodicNotificationApplicationConfiguration object from a Properties file.  This method assumes
     * that all values in the Properties file are Strings and that the Properties file uses the keys below.
     * <br>
     * <ul>
     * <li>"accumulo.auths" - String of Accumulo authorizations. Default is empty String.
     * <li>"accumulo.instance" - Accumulo instance name (required)
     * <li>"accumulo.user" - Accumulo user (required)
     * <li>"accumulo.password" - Accumulo password (required)
     * <li>"accumulo.rya.prefix" - Prefix for Accumulo backed Rya instance.  Default is "rya_"
     * <li>"accumulo.zookeepers" - Zookeepers for underlying Accumulo instance (required)
     * <li>"rya.pcj.fluo.app.name" - Name of Fluo Application (required)
     * <li>"rya.pcj.fluo.table.name" - Name of Fluo Table (required)
     * <li>"rya.periodic.notification.kafka.bootstrap.servers" - Kafka Bootstrap servers for Producers and Consumers (required)
     * <li>"rya.periodic.notification.kafka.topic" - Topic to which new Periodic Notifications are published. Default is "notifications".
     * <li>"rya.periodic.notification.kafka.client.id" - Client Id for notification topic.  Default is "consumer0"
     * <li>"rya.periodic.notification.kafka.group.id" - Group Id for notification topic.  Default is "group0"
     * <li>"rya.periodic.notification.coordinator.threads" - Number of threads used by coordinator. Default is 1.
     * <li>"rya.periodic.notification.producer.threads" - Number of threads used by producer.  Default is 1.
     * <li>"rya.periodic.notification.exporter.threads" - Number of threads used by exporter.  Default is 1.
     * <li>"rya.periodic.notification.processor.threads" - Number of threads used by processor.  Default is 1.
     * <li>"rya.periodic.notification.pruner.threads" - Number of threads used by pruner.  Default is 1.
     * </ul>
     * <br>
     * @param props - Properties file containing Accumulo specific configuration parameters
     */
    public PeriodicNotificationApplicationConfiguration(final Properties props) {
       super(fromProperties(props));
       setFluoAppName(props.getProperty(FLUO_APP_NAME));
       setFluoTableName(props.getProperty(FLUO_TABLE_NAME));
       setBootStrapServers(props.getProperty(KAFKA_BOOTSTRAP_SERVERS));
       setNotificationClientId(props.getProperty(NOTIFICATION_CLIENT_ID, "consumer0"));
       setNotificationTopic(props.getProperty(NOTIFICATION_TOPIC, "notifications"));
       setNotificationGroupId(props.getProperty(NOTIFICATION_GROUP_ID, "group0"));
       setProducerThreads(Integer.parseInt(props.getProperty(PRODUCER_THREADS, "1")));
       setProcessorThreads(Integer.parseInt(props.getProperty(PROCESSOR_THREADS, "1")));
       setExporterThreads(Integer.parseInt(props.getProperty(EXPORTER_THREADS, "1")));
       setPrunerThreads(Integer.parseInt(props.getProperty(PRUNER_THREADS, "1")));
       setCoordinatorThreads(Integer.parseInt(props.getProperty(COORDINATOR_THREADS, "1")));
    }

    /**
     * Sets the name of the Fluo Application
     * @param fluoAppName
     */
    public void setFluoAppName(final String fluoAppName) {
        set(FLUO_APP_NAME, Objects.requireNonNull(fluoAppName));
    }

    /**
     * Sets the name of the Fluo table
     * @param fluoTableName
     */
    public void setFluoTableName(final String fluoTableName) {
       set(FLUO_TABLE_NAME, Objects.requireNonNull(fluoTableName));
    }

    /**
     * Sets the Kafka bootstrap servers
     * @param bootStrapServers
     */
    public void setBootStrapServers(final String bootStrapServers) {
        set(KAFKA_BOOTSTRAP_SERVERS, Objects.requireNonNull(bootStrapServers));
    }

    /**
     * Sets the Kafka topic name for new notification requests
     * @param notificationTopic
     */
    public void setNotificationTopic(final String notificationTopic) {
        set(NOTIFICATION_TOPIC, Objects.requireNonNull(notificationTopic));
    }

    /**
     * Sets the GroupId for new notification request topic
     * @param notificationGroupId
     */
    public void setNotificationGroupId(final String notificationGroupId) {
        set(NOTIFICATION_GROUP_ID, Objects.requireNonNull(notificationGroupId));
    }

    /**
     * Sets the ClientId for the Kafka notification topic
     * @param notificationClientId
     */
    public void setNotificationClientId(final String notificationClientId) {
        set(NOTIFICATION_CLIENT_ID, Objects.requireNonNull(notificationClientId));
    }

    /**
     * Sets the number of threads for the coordinator
     * @param threads
     */
    public void setCoordinatorThreads(final int threads) {
        setInt(COORDINATOR_THREADS, threads);
    }

    /**
     * Sets the number of threads for the exporter
     * @param threads
     */
    public void setExporterThreads(final int threads) {
        setInt(EXPORTER_THREADS, threads);
    }

    /**
     * Sets the number of threads for the producer for reading new periodic notifications
     * @param threads
     */
    public void setProducerThreads(final int threads) {
        setInt(PRODUCER_THREADS, threads);
    }

    /**
     * Sets the number of threads for the bin pruner
     * @param threads
     */
    public void setPrunerThreads(final int threads) {
        setInt(PRUNER_THREADS, threads);
    }

    /**
     * Sets the number of threads for the Notification processor
     * @param threads
     */
    public void setProcessorThreads(final int threads) {
        setInt(PROCESSOR_THREADS, threads);
    }

    /**
     * @return name of the Fluo application
     */
    public String getFluoAppName() {
        return get(FLUO_APP_NAME);
    }

    /**
     * @return name of the Fluo table
     */
    public String getFluoTableName() {
       return get(FLUO_TABLE_NAME);
    }

    /**
     * @return Kafka bootstrap servers
     */
    public String getBootStrapServers() {
        return get(KAFKA_BOOTSTRAP_SERVERS);
    }

    /**
     * @return notification topic
     */
    public String getNotificationTopic() {
        return get(NOTIFICATION_TOPIC, "notifications");
    }

    /**
     * @return Kafka GroupId for the notificaton topic
     */
    public String getNotificationGroupId() {
        return get(NOTIFICATION_GROUP_ID, "group0");
    }

    /**
     * @return Kafka ClientId for the notification topic
     */
    public String getNotificationClientId() {
        return get(NOTIFICATION_CLIENT_ID, "consumer0");
    }

    /**
     * @return the number of threads for the coordinator
     */
    public int getCoordinatorThreads() {
        return getInt(COORDINATOR_THREADS, 1);
    }

    /**
     * @return the number of threads for the exporter
     */
    public int getExporterThreads() {
        return getInt(EXPORTER_THREADS, 1);
    }

    /**
     * @return the number of threads for the notification producer
     */
    public int getProducerThreads() {
        return getInt(PRODUCER_THREADS, 1);
    }

    /**
     * @return the number of threads for the bin pruner
     */
    public int getPrunerThreads() {
        return getInt(PRUNER_THREADS, 1);
    }

    /**
     * @return number of threads for the processor
     */
    public int getProcessorThreads() {
        return getInt(PROCESSOR_THREADS, 1);
    }

}
