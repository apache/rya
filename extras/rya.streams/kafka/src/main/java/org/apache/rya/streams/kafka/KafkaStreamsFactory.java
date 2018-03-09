/**
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
package org.apache.rya.streams.kafka;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.rya.streams.api.entity.StreamsQuery;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Builds {@link KafkaStreams} objects that are able to process a specific {@link StreamsQuery}.
 */
@DefaultAnnotation(NonNull.class)
public interface KafkaStreamsFactory {

    /**
     * Builds a {@link KafkaStreams} object  that is able to process a specific {@link StreamsQuery}.
     *
     * @param ryaInstance - The Rya Instance the streams job is for. (not null)
     * @param query - Defines the query that will be executed. (not null)
     * @return A {@link KafkaStreams} object that will process the provided query.
     * @throws KafkaStreamsFactoryException Unable to create a {@link KafkaStreams} object from the provided values.
     */
    public KafkaStreams make(String ryaInstance, StreamsQuery query) throws KafkaStreamsFactoryException;

    /**
     * A {@link KafkaStreamsFactory} could not create a {@link KafkaStreams} object.
     */
    public static class KafkaStreamsFactoryException extends Exception {
        private static final long serialVersionUID = 1L;

        public KafkaStreamsFactoryException(final String message) {
            super(message);
        }

        public KafkaStreamsFactoryException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }
}