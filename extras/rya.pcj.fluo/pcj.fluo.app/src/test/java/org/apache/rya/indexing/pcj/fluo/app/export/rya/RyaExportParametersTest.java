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
package org.apache.rya.indexing.pcj.fluo.app.export.rya;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

/**
 * Tests the methods of {@link RyaExportParameters}.
 */
public class RyaExportParametersTest {

    @Test
    public void writeParams() {
        final Map<String, String> params = new HashMap<>();

        // Load some values into the params using the wrapper.
        final RyaExportParameters ryaParams = new RyaExportParameters(params);
        ryaParams.setUseRyaBindingSetExporter(true);
        ryaParams.setAccumuloInstanceName("demoAccumulo");
        ryaParams.setZookeeperServers("zoo1;zoo2");
        ryaParams.setExporterUsername("fluo");
        ryaParams.setExporterPassword("3xp0rt3r");

        // Ensure the params map has the expected values.
        final Map<String, String> expectedParams = new HashMap<>();
        expectedParams.put(RyaExportParameters.CONF_USE_RYA_BINDING_SET_EXPORTER, "true");
        expectedParams.put(RyaExportParameters.CONF_ACCUMULO_INSTANCE_NAME, "demoAccumulo");
        expectedParams.put(RyaExportParameters.CONF_ZOOKEEPER_SERVERS, "zoo1;zoo2");
        expectedParams.put(RyaExportParameters.CONF_EXPORTER_USERNAME, "fluo");
        expectedParams.put(RyaExportParameters.CONF_EXPORTER_PASSWORD, "3xp0rt3r");

        assertEquals(expectedParams, params);
    }

    @Test
    public void notConfigured() {
        final Map<String, String> params = new HashMap<>();

        // Ensure an unconfigured parameters map will say rya export is disabled.
        final RyaExportParameters ryaParams = new RyaExportParameters(params);
        assertFalse(ryaParams.getUseRyaBindingSetExporter());
    }
}