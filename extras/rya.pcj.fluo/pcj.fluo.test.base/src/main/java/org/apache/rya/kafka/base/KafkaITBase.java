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

import org.apache.fluo.core.util.PortUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.junit.After;
import org.junit.Before;

import kafka.server.KafkaConfig;
import kafka.server.KafkaConfig$;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.Time;
import kafka.zk.EmbeddedZookeeper;

public class KafkaITBase {

    private static final String ZKHOST = "127.0.0.1";
    private static final String BROKERHOST = "127.0.0.1";
    private KafkaServer kafkaServer;
    private EmbeddedZookeeper zkServer;
    private String brokerPort;

    @Before
    public void setupKafka() throws Exception {
        // Setup Kafka.
        zkServer = new EmbeddedZookeeper();
        final String zkConnect = ZKHOST + ":" + zkServer.port();

        // setup Broker
        brokerPort = Integer.toString(PortUtils.getRandomFreePort());
        final Properties brokerProps = new Properties();
        brokerProps.setProperty(KafkaConfig$.MODULE$.BrokerIdProp(), "0");
        brokerProps.setProperty(KafkaConfig$.MODULE$.HostNameProp(), BROKERHOST);
        brokerProps.setProperty(KafkaConfig$.MODULE$.ZkConnectProp(), zkConnect);
        brokerProps.setProperty(KafkaConfig$.MODULE$.LogDirsProp(), Files.createTempDirectory(getClass().getSimpleName()+"-").toAbsolutePath().toString());
        brokerProps.setProperty(KafkaConfig$.MODULE$.PortProp(), brokerPort);
        final KafkaConfig config = new KafkaConfig(brokerProps);

        final Time mock = new MockTime();
        kafkaServer = TestUtils.createServer(config, mock);
    }

    /**
     * Close all the Kafka mini server and mini-zookeeper
     *
     * @see org.apache.rya.indexing.pcj.fluo.ITBase#shutdownMiniResources()
     */
    @After
    public void teardownKafka() {
        kafkaServer.shutdown();
        zkServer.shutdown();
    }

    /**
     * @return A new Property object containing the correct value for Kafka's
     *         {@link CommonClientConfigs#BOOTSTRAP_SERVERS_CONFIG}.
     */
   protected Properties createBootstrapServerConfig() {
       final Properties config = new Properties();
       config.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BROKERHOST + ":" + brokerPort);
       return config;
   }
}
