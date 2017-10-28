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
package org.apache.rya.streams.kafka;

import org.apache.rya.streams.api.queries.QueryChangeLog;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Creates the Kafka topic names that are used for Rya Streams systems.
 */
@DefaultAnnotation(NonNull.class)
public class KafkaTopics {

    /**
     * Creates the Kafka topic that will be used for a specific instance of Rya's {@link QueryChangeLog}.
     *
     * @param ryaInstance - The Rya instance the change log is for. (not null)
     * @return The name of the Kafka topic.
     */
    public static String queryChangeLogTopic(final String ryaInstance) {
        return ryaInstance + "-QueryChangeLog";
    }

    /**
     * Creates the Kafka topic that will be used to load statements into the Rya Streams system for a specific
     * instance of Rya.
     *
     * @param ryaInstance - The Rya instance the statements are for. (not null)
     * @return The name of the Kafka topic.
     */
    public static String statementsTopic(final String ryaInstance) {
        return ryaInstance + "-Statements";
    }
}