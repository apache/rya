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

public class EmbeddedKafkaInstance {

    private static final Logger logger = LoggerFactory.getLogger(EmbeddedKafkaInstance.class);

    private static final AtomicInteger kafkaTopicNameCounter = new AtomicInteger(1);
    private static final String IPv4_LOOPBACK = "127.0.0.1";
    private static final String ZKHOST = IPv4_LOOPBACK;
    private static final String BROKERHOST = IPv4_LOOPBACK;
    private KafkaServer kafkaServer;
    private EmbeddedZookeeper zkServer;
    private String brokerPort;
    private String zookeperConnect;

    /**
     * Startup the Embedded Kafka and Zookeeper.
     * @throws Exception
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
     * @return A new Property object containing the correct value for Kafka's
     *         {@link CommonClientConfigs#BOOTSTRAP_SERVERS_CONFIG}.
     */
    public Properties createBootstrapServerConfig() {
        final Properties config = new Properties();
        config.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BROKERHOST + ":" + brokerPort);
        return config;
    }

    public String getBrokerHost() {
        return BROKERHOST;
    }

    public String getBrokerPort() {
        return brokerPort;
    }

    public String getZookeeperConnect() {
        return zookeperConnect;
    }

    public String getUniqueTopicName() {
        return "topic" + kafkaTopicNameCounter.getAndIncrement() + "_";
    }
}
