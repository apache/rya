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
package org.apache.rya.periodic.notification.exporter;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.rya.indexing.pcj.storage.PeriodicQueryResultStorage;
import org.apache.rya.periodic.notification.api.BindingSetRecord;
import org.apache.rya.periodic.notification.serialization.BindingSetSerDe;
import org.apache.rya.test.kafka.KafkaITBase;
import org.apache.rya.test.kafka.KafkaTestInstanceRule;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;

public class PeriodicNotificationExporterIT extends KafkaITBase {


    @Rule
    public KafkaTestInstanceRule kafkaTestInstanceRule = new KafkaTestInstanceRule(false);


    private static final ValueFactory vf = new ValueFactoryImpl();

    @Test
    public void testExporter() throws InterruptedException {

        final String topic1 = kafkaTestInstanceRule.getKafkaTopicName() + "1";
        final String topic2 = kafkaTestInstanceRule.getKafkaTopicName() + "2";

        kafkaTestInstanceRule.createTopic(topic1);
        kafkaTestInstanceRule.createTopic(topic2);

        final BlockingQueue<BindingSetRecord> records = new LinkedBlockingQueue<>();

        final KafkaExporterExecutor exporter = new KafkaExporterExecutor(new KafkaProducer<String, BindingSet>(createKafkaProducerConfig()), 1, records);
        exporter.start();
        final QueryBindingSet bs1 = new QueryBindingSet();
        bs1.addBinding(PeriodicQueryResultStorage.PeriodicBinId, vf.createLiteral(1L));
        bs1.addBinding("name", vf.createURI("uri:Bob"));
        final BindingSetRecord record1 = new BindingSetRecord(bs1, topic1);

        final QueryBindingSet bs2 = new QueryBindingSet();
        bs2.addBinding(PeriodicQueryResultStorage.PeriodicBinId, vf.createLiteral(2L));
        bs2.addBinding("name", vf.createURI("uri:Joe"));
        final BindingSetRecord record2 = new BindingSetRecord(bs2, topic2);

        records.add(record1);
        records.add(record2);

        final Set<BindingSet> expected1 = new HashSet<>();
        expected1.add(bs1);
        final Set<BindingSet> expected2 = new HashSet<>();
        expected2.add(bs2);

        final Set<BindingSet> actual1 = getBindingSetsFromKafka(topic1);
        final Set<BindingSet> actual2 = getBindingSetsFromKafka(topic2);

        Assert.assertEquals(expected1, actual1);
        Assert.assertEquals(expected2, actual2);

        exporter.stop();
    }


    private Properties createKafkaProducerConfig() {
        final Properties props = createBootstrapServerConfig();
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, BindingSetSerDe.class.getName());
        return props;
    }
    private Properties createKafkaConsumerConfig() {
        final Properties props = createBootstrapServerConfig();
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "consumer0");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BindingSetSerDe.class.getName());
        return props;
    }


    private KafkaConsumer<String, BindingSet> makeBindingSetConsumer(final String topicName) {
        // setup consumer
        final KafkaConsumer<String, BindingSet> consumer = new KafkaConsumer<>(createKafkaConsumerConfig());
        consumer.subscribe(Arrays.asList(topicName));
        return consumer;
    }

    private Set<BindingSet> getBindingSetsFromKafka(final String topicName) {
        KafkaConsumer<String, BindingSet> consumer = null;

        try {
            consumer = makeBindingSetConsumer(topicName);
            final ConsumerRecords<String, BindingSet> records = consumer.poll(20000);  // Wait up to 20 seconds for a result to be published.

            final Set<BindingSet> bindingSets = new HashSet<>();
            records.forEach(x -> bindingSets.add(x.value()));

            return bindingSets;

        } catch (final Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (consumer != null) {
                consumer.close();
            }
        }
    }
}
