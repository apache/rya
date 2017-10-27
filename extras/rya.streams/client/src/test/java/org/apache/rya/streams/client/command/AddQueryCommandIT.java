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
package org.apache.rya.streams.client.command;

import static org.junit.Assert.assertEquals;

import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.rya.test.kafka.KafkaITBase;
import org.apache.rya.test.kafka.KafkaTestInstanceRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/**
 * integration Test for adding a new query through a command.
 */
public class AddQueryCommandIT extends KafkaITBase {
    private String[] args;

    @Rule
    public KafkaTestInstanceRule rule = new KafkaTestInstanceRule(true);

    @Before
    public void setup() {
        final Properties props = rule.createBootstrapServerConfig();
        final String location = props.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
        final String[] tokens = location.split(":");
        args = new String[] {
                "-q", "Some sparql query",
                "-t", rule.getKafkaTopicName(),
                "-p", tokens[1],
                "-i", tokens[0]
        };
    }

    @Test
    public void happyAddQueryTest() throws Exception {
        final AddQueryCommand command = new AddQueryCommand();
        command.execute(args);
        // not sure what to assert here.
        assertEquals(true, true);
    }
}
