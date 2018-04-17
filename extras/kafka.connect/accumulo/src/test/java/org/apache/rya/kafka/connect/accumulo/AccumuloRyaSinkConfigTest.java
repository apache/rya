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
package org.apache.rya.kafka.connect.accumulo;

import java.util.HashMap;
import java.util.Map;

import org.apache.rya.kafka.connect.api.sink.RyaSinkConfig;
import org.junit.Test;

/**
 * Unit tests the methods of {@link AccumuloRyaSinkConfig}.
 */
public class AccumuloRyaSinkConfigTest {

    @Test
    public void parses() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(AccumuloRyaSinkConfig.ZOOKEEPERS, "zoo1:2181,zoo2");
        properties.put(AccumuloRyaSinkConfig.CLUSTER_NAME, "test");
        properties.put(AccumuloRyaSinkConfig.USERNAME, "alice");
        properties.put(AccumuloRyaSinkConfig.PASSWORD, "alice1234!@");
        properties.put(RyaSinkConfig.RYA_INSTANCE_NAME, "rya_");
        new AccumuloRyaSinkConfig(properties);
    }
}