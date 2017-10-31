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
package org.apache.rya.streams.kafka.entity;

import static java.util.Objects.requireNonNull;

import java.util.Iterator;
import java.util.UUID;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.streams.api.entity.QueryResultStream;
import org.apache.rya.streams.api.exception.RyaStreamsException;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A Kafka implementation of {@link QueryResultStream}. It delegates the {@link #poll(long)} method to
 * a {@link Consumer}. As a result, the starting point of this stream is whatever position the consumer
 * starts at within the Kafka topic.
 */
@DefaultAnnotation(NonNull.class)
public class KafkaQueryResultStream extends QueryResultStream {

    private final Consumer<?, VisibilityBindingSet> consumer;

    /**
     * Constructs an instance of {@link KafkaQueryResultStream}.
     *
     * @param queryId - The query the results are for. (not null)
     * @param consumer - The consumer that will be polled by this class. (not null)
     */
    public KafkaQueryResultStream(final UUID queryId, final Consumer<?, VisibilityBindingSet> consumer) {
        super(queryId);
        this.consumer = requireNonNull(consumer);
    }

    @Override
    public Iterable<VisibilityBindingSet> poll(final long timeoutMs) throws RyaStreamsException {
        return new RecordEntryIterable<>( consumer.poll(timeoutMs) );
    }

    /**
     * An {@link Iterable} that creates {@link Iterator}s over a {@link ConsumerRecords}' values.
     * This is useful for when you don't care about the key portion of a record.
     *
     * @param <K> - The type of the record's key.
     * @param <T> - The type of the record's value.
     */
    private final class RecordEntryIterable<K, T> implements Iterable<T> {

        private final ConsumerRecords<K, T> records;

        public RecordEntryIterable(final ConsumerRecords<K, T> records) {
            this.records = requireNonNull(records);
        }

        @Override
        public Iterator<T> iterator() {
            return new Iterator<T>() {
                private final Iterator<ConsumerRecord<K, T>> it = records.iterator();

                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public T next() {
                    return it.next().value();
                }
            };
        }
    }

    @Override
    public void close() throws Exception {
        consumer.close();
    }
}