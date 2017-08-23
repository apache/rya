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
package org.apache.rya.kafka.base;

import java.nio.file.Files;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.fluo.core.util.PortUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.server.KafkaConfig;
import kafka.server.KafkaConfig$;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.Time;
import kafka.zk.EmbeddedZookeeper;

/**
 * This class provides a {@link KafkaServer} and a dedicated
 * {@link EmbeddedZookeeper} server for integtration testing. Both servers use a
 * random free port, so it is necesssary to use the
 * {@link #getZookeeperConnect()} and {@link #createBootstrapServerConfig()}
 * methods to determine how to connect to them.
 *
 */
public class EmbeddedKafkaInstance {

    private static final Logger logger = LoggerFactory.getLogger(EmbeddedKafkaInstance.class);

    private static final AtomicInteger KAFKA_TOPIC_COUNTER = new AtomicInteger(1);
    private static final String IPv4_LOOPBACK = "127.0.0.1";
    private static final String ZKHOST = IPv4_LOOPBACK;
    private static final String BROKERHOST = IPv4_LOOPBACK;
    private KafkaServer kafkaServer;
    private EmbeddedZookeeper zkServer;
    private String brokerPort;
    private String zookeperConnect;

    /**
     * Starts the Embedded Kafka and Zookeeper Servers.
     * @throws Exception - If an exeption occurs during startup.
     */
    protected void startup() throws Exception {
        // Setup the embedded zookeeper
        logger.info("Starting up Embedded Zookeeper...");
        zkServer = new EmbeddedZookeeper();
        zookeperConnect = ZKHOST + ":" + zkServer.port();
        logger.info("Embedded Zookeeper started at: {}", zookeperConnect);

        // setup Broker
        logger.info("Starting up Embedded Kafka...");
        brokerPort = Integer.toString(PortUtils.getRandomFreePort());
        final Properties brokerProps = new Properties();
        brokerProps.setProperty(KafkaConfig$.MODULE$.BrokerIdProp(), "0");
        brokerProps.setProperty(KafkaConfig$.MODULE$.HostNameProp(), BROKERHOST);
        brokerProps.setProperty(KafkaConfig$.MODULE$.PortProp(), brokerPort);
        brokerProps.setProperty(KafkaConfig$.MODULE$.ZkConnectProp(), zookeperConnect);
        brokerProps.setProperty(KafkaConfig$.MODULE$.LogDirsProp(), Files.createTempDirectory(getClass().getSimpleName() + "-").toAbsolutePath().toString());
        final KafkaConfig config = new KafkaConfig(brokerProps);
        final Time mock = new MockTime();
        kafkaServer = TestUtils.createServer(config, mock);
        logger.info("Embedded Kafka Server started at: {}:{}", BROKERHOST, brokerPort);
    }

    /**
     * Shutdown the Embedded Kafka and Zookeeper.
     * @throws Exception
     */
    protected void shutdown() throws Exception {
        try {
            if(kafkaServer != null) {
                kafkaServer.shutdown();
            }
        } finally {
            if(zkServer != null) {
                zkServer.shutdown();
            }
        }
    }

    /**
     * @return A new Property object containing the correct value of
     *         {@link CommonClientConfigs#BOOTSTRAP_SERVERS_CONFIG}, for
     *         connecting to this instance.
     */
    public Properties createBootstrapServerConfig() {
        final Properties config = new Properties();
        config.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BROKERHOST + ":" + brokerPort);
        return config;
    }

    /**
     *
     * @return The host of the Kafka Broker.
     */
    public String getBrokerHost() {
        return BROKERHOST;
    }

    /**
     *
     * @return The port of the Kafka Broker.
     */
    public String getBrokerPort() {
        return brokerPort;
    }

    /**
     *
     * @return The Zookeeper Connect String.
     */
    public String getZookeeperConnect() {
        return zookeperConnect;
    }

    /**
     *
     * @return A unique Kafka topic name for this instance.
     */
    public String getUniqueTopicName() {
        return "topic_" + KAFKA_TOPIC_COUNTER.getAndIncrement() + "_";
    }
}
