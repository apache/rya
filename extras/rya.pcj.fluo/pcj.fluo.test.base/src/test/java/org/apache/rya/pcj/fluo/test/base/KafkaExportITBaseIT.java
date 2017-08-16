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
package org.apache.rya.pcj.fluo.test.base;

import static org.junit.Assert.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.rya.kafka.base.KafkaTestInstanceRule;
import org.junit.Rule;
import org.junit.Test;


public class KafkaExportITBaseIT extends KafkaExportITBase {

    @Rule
    public KafkaTestInstanceRule kafkaTestRule = new KafkaTestInstanceRule(true);

    /**
     * Test kafka without rya code to make sure kafka works in this environment.
     * If this test fails then its a testing environment issue, not with Rya.
     * Source: https://github.com/asmaier/mini-kafka
     */
    @Test
    public void embeddedKafkaTest() throws Exception {
        // create topic
        final String topic = kafkaTestRule.getKafkaTopicName();

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
