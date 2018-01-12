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

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.rya.streams.api.queries.ChangeLogEntry;
import org.apache.rya.streams.api.queries.QueryChange;
import org.apache.rya.streams.api.queries.QueryChangeLog;

import com.google.common.collect.Lists;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;
import info.aduna.iteration.CloseableIteration;

/**
 * A Kafka implementation of a {@link QueryChangeLog}.
 */
@DefaultAnnotation(NonNull.class)
public class KafkaQueryChangeLog implements QueryChangeLog {
    /*
     * Key is '?' since you cannot have parallel processing over a sequential
     * change log, so there is only one partition.
     */
    private final Producer<?, QueryChange> producer;

    /*
     * Key is '?' since you cannot have parallel processing over a sequential
     * change log, so there is only one partition.
     */
    private final Consumer<?, QueryChange> consumer;

    private final String topic;

    /**
     * Creates a new {@link KafkaQueryChangeLog}.
     *
     * @param producer - The producer to use to add {@link QueryChange}s to a kafka topic. (not null)
     * @param consumer - The consumer to use to read {@link QueryChange}s from a kafka topic. (not null)
     * @param topic - The topic on kafka to read/write from. (not null)
     */
    public KafkaQueryChangeLog(final Producer<?, QueryChange> producer,
            final Consumer<?, QueryChange> consumer,
            final String topic) {
        this.producer = requireNonNull(producer);
        this.consumer = requireNonNull(consumer);
        this.topic = requireNonNull(topic);
    }

    @Override
    public void write(final QueryChange newChange) throws QueryChangeLogException {
        requireNonNull(newChange);

        // Write the change to the log immediately.
        final Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topic, newChange));
        producer.flush();

        // Don't return until the write has been completed.
        try {
            future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new QueryChangeLogException("Could not record a new query change to the Kafka Query Change Log.", e);
        }
    }

    @Override
    public CloseableIteration<ChangeLogEntry<QueryChange>, QueryChangeLogException> readFromStart() throws QueryChangeLogException {
        final TopicPartition part = new TopicPartition(topic, 0);
        consumer.assign(Lists.newArrayList(part));
        consumer.seekToBeginning(Lists.newArrayList(part));
        return new QueryChangeLogEntryIter(consumer);
    }

    @Override
    public CloseableIteration<ChangeLogEntry<QueryChange>, QueryChangeLogException> readFromPosition(final long position) throws QueryChangeLogException {
        final TopicPartition part = new TopicPartition(topic, 0);
        consumer.assign(Lists.newArrayList(part));
        consumer.seek(part, position);
        return new QueryChangeLogEntryIter(consumer);
    }

    /**
     * Closing this class will also close the {@link Producer} and {@link Consumer} that were passed into it.
     */
    @Override
    public void close() throws Exception {
        producer.close();
        consumer.close();
    }

    /**
     * A {@link CloseableIteration} to iterate over a consumer's results. Since
     * the consumer returns in bulk when poll(), a cache of recent polling is
     * maintained.
     *
     * If there are no new results after 3 seconds,
     * {@link QueryChangeLogEntryIter#hasNext()} will return false.
     */
    private class QueryChangeLogEntryIter implements CloseableIteration<ChangeLogEntry<QueryChange>, QueryChangeLogException> {
        private final Consumer<?, QueryChange> consumer;
        private Iterator<ChangeLogEntry<QueryChange>> iterCache;

        /**
         * Creates a new {@link QueryChangeLogEntryIter}.
         *
         * @param consumer - The consumer to iterate over. (not null)
         */
        public QueryChangeLogEntryIter(final Consumer<?, QueryChange> consumer) {
            this.consumer = requireNonNull(consumer);
        }

        @Override
        public boolean hasNext() throws QueryChangeLogException {
            maybePopulateCache();
            return iterCache.hasNext();
        }

        @Override
        public ChangeLogEntry<QueryChange> next() throws QueryChangeLogException {
            maybePopulateCache();
            if (iterCache.hasNext()) {
                return iterCache.next();
            }
            throw new QueryChangeLogException("There are no changes in the change log.");
        }

        @Override
        public void remove() throws QueryChangeLogException {
        }

        @Override
        public void close() throws QueryChangeLogException {
            consumer.unsubscribe();
        }

        private void maybePopulateCache() {
            // If the cache isn't initialized yet, or it is empty, then check to see if there is more to put into it.
            if (iterCache == null || !iterCache.hasNext()) {
                final ConsumerRecords<?, QueryChange> records = consumer.poll(3000L);
                final List<ChangeLogEntry<QueryChange>> changes = new ArrayList<>();
                records.forEach(record -> changes.add(new ChangeLogEntry<>(record.offset(), record.value())));
                iterCache = changes.iterator();
            }
        }
    }
}