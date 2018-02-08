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
package org.apache.rya.streams.kafka.queries;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.rya.streams.api.queries.ChangeLogEntry;
import org.apache.rya.streams.api.queries.QueryChange;
import org.apache.rya.streams.api.queries.QueryChangeLog.QueryChangeLogException;
import org.apache.rya.streams.kafka.serialization.queries.QueryChangeDeserializer;
import org.apache.rya.streams.kafka.serialization.queries.QueryChangeSerializer;
import org.apache.rya.test.kafka.KafkaITBase;
import org.apache.rya.test.kafka.KafkaTestInstanceRule;
import org.apache.rya.test.kafka.KafkaTestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.Lists;

import info.aduna.iteration.CloseableIteration;

/**
 * Integration tests the {@link KafkaQueryChangeLog}.
 */
public class KafkaQueryChangeLogIT extends KafkaITBase {

    private KafkaQueryChangeLog changeLog;
    private Producer<?, QueryChange> producer;
    private Consumer<?, QueryChange> consumer;
    private String topic;

    @Rule
    public KafkaTestInstanceRule rule = new KafkaTestInstanceRule(true);

    @Before
    public void setup() {
        topic = rule.getKafkaTopicName();
        producer = KafkaTestUtil.makeProducer(rule, StringSerializer.class, QueryChangeSerializer.class);
        consumer = KafkaTestUtil.fromStartConsumer(rule, StringDeserializer.class, QueryChangeDeserializer.class);
        changeLog = new KafkaQueryChangeLog(producer, consumer, topic);
    }

    @After
    public void cleanup() {
        producer.close();
        consumer.close();
    }

    @Test
    public void testWrite() throws Exception {
        final String sparql = "SOME QUERY HERE";
        final UUID uuid = UUID.randomUUID();
        final QueryChange newChange = QueryChange.create(uuid, sparql, true, false);
        changeLog.write(newChange);

        consumer.subscribe(Lists.newArrayList(topic));
        final ConsumerRecords<?, QueryChange> records = consumer.poll(2000);
        assertEquals(1, records.count());

        final QueryChange record = records.iterator().next().value();
        assertEquals(newChange, record);
    }

    @Test
    public void readSingleWrite() throws Exception {
        // Write a single change to the log.
        final QueryChange change = QueryChange.create(UUID.randomUUID(), "query", true, false);
        changeLog.write(change);

        // Read that entry from the log.
        final QueryChange readChange = changeLog.readFromStart().next().getEntry();
        assertEquals(change, readChange);
    }

    @Test
    public void readFromBegining() throws Exception {
        final List<QueryChange> expected = write10ChangesToChangeLog();

        final CloseableIteration<ChangeLogEntry<QueryChange>, QueryChangeLogException> iter = changeLog.readFromStart();

        final List<QueryChange> actual = new ArrayList<>();
        while (iter.hasNext()) {
            final ChangeLogEntry<QueryChange> entry = iter.next();
            actual.add(entry.getEntry());
        }
        assertEquals(expected, actual);
    }

    @Test
    public void readFromBegining_positionStartsNotBegining() throws Exception {
        final List<QueryChange> expected = write10ChangesToChangeLog();

        // set the position to some non-0 position
        final TopicPartition partition = new TopicPartition(topic, 0);
        consumer.assign(Lists.newArrayList(partition));
        consumer.seek(partition, 5L);
        final CloseableIteration<ChangeLogEntry<QueryChange>, QueryChangeLogException> iter = changeLog.readFromStart();

        final List<QueryChange> actual = new ArrayList<>();
        while (iter.hasNext()) {
            final ChangeLogEntry<QueryChange> entry = iter.next();
            actual.add(entry.getEntry());
        }
        assertEquals(expected, actual);
    }

    @Test
    public void readFromPosition_positionStartsBegining() throws Exception {
        final List<QueryChange> expected = write10ChangesToChangeLog().subList(5, 10);

        // set the position to some non-0 position
        final TopicPartition partition = new TopicPartition(topic, 0);
        consumer.assign(Lists.newArrayList(partition));
        consumer.seekToBeginning(Lists.newArrayList(partition));
        final CloseableIteration<ChangeLogEntry<QueryChange>, QueryChangeLogException> iter = changeLog.readFromPosition(5L);

        final List<QueryChange> actual = new ArrayList<>();
        while (iter.hasNext()) {
            final ChangeLogEntry<QueryChange> entry = iter.next();
            actual.add(entry.getEntry());
        }
        assertEquals(expected, actual);
    }

    @Test
    public void readFromPosition_positionStartsNotBegining() throws Exception {
        final List<QueryChange> expected = write10ChangesToChangeLog().subList(5, 10);

        // set the position to some non-0 position
        final TopicPartition partition = new TopicPartition(topic, 0);
        consumer.assign(Lists.newArrayList(partition));
        consumer.seekToEnd(Lists.newArrayList(partition));
        final CloseableIteration<ChangeLogEntry<QueryChange>, QueryChangeLogException> iter = changeLog.readFromPosition(5L);

        final List<QueryChange> actual = new ArrayList<>();
        while (iter.hasNext()) {
            final ChangeLogEntry<QueryChange> entry = iter.next();
            actual.add(entry.getEntry());
        }
        assertEquals(expected, actual);
    }

    @Test
    public void readFromPosition_positionStartsEnd() throws Exception {
        write10ChangesToChangeLog();

        // set the position to some non-0 position
        final TopicPartition partition = new TopicPartition(topic, 0);
        consumer.assign(Lists.newArrayList(partition));
        consumer.seekToEnd(Lists.newArrayList(partition));
        final CloseableIteration<ChangeLogEntry<QueryChange>, QueryChangeLogException> iter = changeLog.readFromPosition(10L);
        int count = 0;
        while (iter.hasNext()) {
            // should be empty
            iter.next();
            count++;
        }
        assertEquals(0, count);
    }

    @Test
    public void multipleClients() throws Exception {
        // Create a second KafkaQueryChangeLog objects that connect to the same change log.
        final Producer<?, QueryChange> producer2 = KafkaTestUtil.makeProducer(rule, StringSerializer.class, QueryChangeSerializer.class);
        final Consumer<?, QueryChange> consumer2 = KafkaTestUtil.fromStartConsumer(rule, StringDeserializer.class, QueryChangeDeserializer.class);
        try(final KafkaQueryChangeLog changeLog2 = new KafkaQueryChangeLog(producer2, consumer2, topic)) {
            // Show both of them report empty.
            assertFalse( changeLog.readFromStart().hasNext() );
            assertFalse( changeLog2.readFromStart().hasNext() );

            // Write a change to the first log.
            final QueryChange change = QueryChange.create(UUID.randomUUID(), "query", true, false);
            changeLog.write(change);

            // Show it's in the first log.
            assertEquals(change, changeLog.readFromStart().next().getEntry());

            // Show it is also seen in the second log.
            assertEquals(change, changeLog2.readFromStart().next().getEntry());
        }
    }

    private List<QueryChange> write10ChangesToChangeLog() throws Exception {
        final List<QueryChange> changes = new ArrayList<>();
        for (int ii = 0; ii < 10; ii++) {
            final String sparql = "SOME QUERY HERE_" + ii;
            final UUID uuid = UUID.randomUUID();
            final QueryChange newChange = QueryChange.create(uuid, sparql, true, false);
            changeLog.write(newChange);
            changes.add(newChange);
        }
        return changes;
    }
}