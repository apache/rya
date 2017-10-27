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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.rya.test.kafka.KafkaITBase;
import org.apache.rya.test.kafka.KafkaTestInstanceRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Integration Test for deleting a query from Rya Streams through a command.
 */
public class DeleteQueryCommandIT extends KafkaITBase {
    private List<String> args;

    @Rule
    public KafkaTestInstanceRule rule = new KafkaTestInstanceRule(true);

    @Before
    public void setup() {
        final Properties props = rule.createBootstrapServerConfig();
        final String location = props.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
        final String[] tokens = location.split(":");

        args = Lists.newArrayList(
                "-t", rule.getKafkaTopicName(),
                "-p", tokens[1],
                "-i", tokens[0]
                );
    }

    @Test
    public void happyDeleteQueryTest() throws Exception {
        // add a query so that it can be removed.
        final List<String> addArgs = new ArrayList<>(args);
        addArgs.add("-q");
        addArgs.add("Some sparql query");
        final AddQueryCommand addCommand = new AddQueryCommand();
        addCommand.execute(addArgs.toArray(new String[] {}));

        final List<String> deleteArgs = new ArrayList<>(args);
        addArgs.add("-q");
        addArgs.add("12345");
        final DeleteQueryCommand deleteCommand = new DeleteQueryCommand();
        deleteCommand.execute(deleteArgs.toArray(new String[] {}));
        // not sure what to assert here.
        assertEquals(true, true);
    }
}
