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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Optional;

import org.junit.Test;

/**
 * Unit tests the methods of {@link KafkaTopics}.
 */
public class KafkaTopicsTest {

    @Test
    public void getRyaInstance_wellFormattedTopic() {
        // Make a topic name using a Rya instance name.
        final String ryaInstance = "test";
        final String topicName = KafkaTopics.queryChangeLogTopic(ryaInstance);

        // Show the rya instance name is able to be extracted from the topic.
        final Optional<String> resolvedRyaInstance = KafkaTopics.getRyaInstance(topicName);
        assertEquals(ryaInstance, resolvedRyaInstance.get());
    }

    @Test
    public void getRyaInstance_invalidTopicName() {
        // Make up an invalid topic name.
        final String invalidTopic = "thisIsABadTopicName";

        // Show there is no Rya Instance name in it.
        final Optional<String> ryaInstance = KafkaTopics.getRyaInstance(invalidTopic);
        assertFalse( ryaInstance.isPresent() );
    }
}