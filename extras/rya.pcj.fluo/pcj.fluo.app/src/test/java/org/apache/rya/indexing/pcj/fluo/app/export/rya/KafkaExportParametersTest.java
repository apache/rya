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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.rya.indexing.pcj.fluo.app.export.kafka.KafkaExportParameterBase;
import org.apache.rya.indexing.pcj.fluo.app.export.kafka.KafkaBindingSetExporterFactory;
import org.apache.rya.indexing.pcj.fluo.app.export.kafka.KafkaBindingSetExporterParameters;
import org.junit.Test;

/**
 * Tests the methods of {@link KafkaExportParameterBase}.
 */
public class KafkaExportParametersTest {

    @Test
    public void writeParams() {
        final Map<String, String> params = new HashMap<>();

        // Load some values into the params using the wrapper.
        final KafkaBindingSetExporterParameters kafkaParams = new KafkaBindingSetExporterParameters(params);
        kafkaParams.setUseKafkaBindingSetExporter(true);

        // Ensure the params map has the expected values.
        final Map<String, String> expectedParams = new HashMap<>();
        expectedParams.put(KafkaBindingSetExporterParameters.CONF_USE_KAFKA_BINDING_SET_EXPORTER, "true");
        assertTrue(kafkaParams.getUseKafkaBindingSetExporter());
        assertEquals(expectedParams, params);

        // now go the other way.
        expectedParams.put(KafkaBindingSetExporterParameters.CONF_USE_KAFKA_BINDING_SET_EXPORTER, "false");
        kafkaParams.setUseKafkaBindingSetExporter(false);
        assertFalse(kafkaParams.getUseKafkaBindingSetExporter());
        assertEquals(expectedParams, params);
    }
    @Test
    public void writeParamsProps() {
        final String key1 = "key1";
        final String value1First = "value1-preserve-this";
        final String value1Second = "value1prop";
        final String key2 = "歌古事学週文原問業間革社。"; // http://generator.lorem-ipsum.info/_chinese
        final String value2 = "良治鮮猿性社費著併病極験。";

        final Map<String, String> params = new HashMap<>();
        // Make sure export key1 is NOT kept separate from producer config key1
        // This is a change, originally they were kept separate.
        params.put(key1, value1First);
        final KafkaExportParameterBase kafkaParams = new KafkaExportParameterBase(params);
        // Load some values into the properties using the wrapper.
        Properties props = new Properties();
        props.put(key1, value1Second);
        props.put(key2, value2);
        kafkaParams.addAllProducerConfig(props);
        Properties propsAfter = kafkaParams.listAllConfig();
        assertEquals(props, propsAfter);
        assertEquals(params, params);
        assertEquals("Should change identical parameters key", params.get(key1), value1Second);
        assertEquals("Props should have params's key", propsAfter.get(key1), value1Second);
        assertNotNull("Should have props key", params.get(key2));
    }

    @Test
    public void notConfigured() {
        final Map<String, String> params = new HashMap<>();

        // Ensure an unconfigured parameters map will say kafka export is disabled.
        final KafkaBindingSetExporterParameters kafkaParams = new KafkaBindingSetExporterParameters(params);
        assertFalse(kafkaParams.getUseKafkaBindingSetExporter());
    }

    @Test
    public void testKafkaResultExporterFactory() {
        KafkaBindingSetExporterFactory factory = new KafkaBindingSetExporterFactory();
        assertNotNull(factory);
    }
}