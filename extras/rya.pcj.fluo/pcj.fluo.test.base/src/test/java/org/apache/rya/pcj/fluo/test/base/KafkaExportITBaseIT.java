package org.apache.rya.pcj.fluo.test.base;

import static org.junit.Assert.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.rya.kafka.base.EmbeddedKafkaSingleton;
import org.junit.Test;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;


public class KafkaExportITBaseIT extends KafkaExportITBase {

    /**
     * Test kafka without rya code to make sure kafka works in this environment.
     * If this test fails then its a testing environment issue, not with Rya.
     * Source: https://github.com/asmaier/mini-kafka
     */
    @Test
    public void embeddedKafkaTest() throws Exception {
        // create topic
        final String topic = getKafkaTopicName();

        // grab the connection string for the zookeeper spun up by our parent class.
        final String zkConnect = EmbeddedKafkaSingleton.getInstance().getZookeeperConnect();

        // Setup Kafka.
        ZkUtils zkUtils = null;
        try {
            zkUtils = ZkUtils.apply(new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer$.MODULE$), false);
            AdminUtils.createTopic(zkUtils, topic, 1, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);
        } finally {
            if(zkUtils != null) {
                zkUtils.close();
            }
        }

        // setup producer
        final Properties producerProps = createBootstrapServerConfig();
        producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer");
        producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        final KafkaProducer<Integer, byte[]> producer = new KafkaProducer<>(producerProps);

        // setup consumer
        final Properties consumerProps = createBootstrapServerConfig();
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group0");
        consumerProps.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "consumer0");
        consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        // to make sure the consumer starts from the beginning of the topic
        consumerProps.put("auto.offset.reset", "earliest");

        final KafkaConsumer<Integer, byte[]> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Arrays.asList(topic));

        // send message
        final ProducerRecord<Integer, byte[]> data = new ProducerRecord<>(topic, 42, "test-message".getBytes(StandardCharsets.UTF_8));
        producer.send(data);
        producer.close();

        // starting consumer
        final ConsumerRecords<Integer, byte[]> records = consumer.poll(3000);
        assertEquals(1, records.count());
        final Iterator<ConsumerRecord<Integer, byte[]>> recordIterator = records.iterator();
        final ConsumerRecord<Integer, byte[]> record = recordIterator.next();
        assertEquals(42, (int) record.key());
        assertEquals("test-message", new String(record.value(), StandardCharsets.UTF_8));
        consumer.close();
    }
}
