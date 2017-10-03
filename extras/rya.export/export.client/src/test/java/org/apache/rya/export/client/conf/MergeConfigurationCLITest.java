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
package org.apache.rya.export.client.conf;

import static org.junit.Assert.assertEquals;

import java.io.File;

import javax.xml.bind.JAXBException;

import org.apache.rya.export.DBType;
import org.apache.rya.export.MergePolicy;
import org.apache.rya.export.MergeToolConfiguration;
import org.apache.rya.export.api.conf.MergeConfigurationException;
import org.junit.Test;

public class MergeConfigurationCLITest {
    @Test
    public void testCreate1ConfigurationFromFile() throws MergeConfigurationException, JAXBException {

        final MergeToolConfiguration conf = MergeConfigurationCLI.createConfigurationFromFile(new File("conf/config.xml"));
        assertEquals("10.10.10.100", conf.getParentHostname());
        assertEquals("accumuloUsername", conf.getParentUsername());
        assertEquals("accumuloPassword", conf.getParentPassword());
        assertEquals("accumuloInstance", conf.getParentRyaInstanceName());
        assertEquals("rya_demo_export_", conf.getParentTablePrefix());
        assertEquals("http://10.10.10.100:8080", conf.getParentTomcatUrl());
        assertEquals(DBType.ACCUMULO, conf.getParentDBType());
        assertEquals(1111, conf.getParentPort());
        assertEquals("10.10.10.101", conf.getChildHostname());
        assertEquals("rya_demo_child", conf.getChildRyaInstanceName());
        assertEquals("rya_demo_export_", conf.getChildTablePrefix());
        assertEquals("http://10.10.10.101:8080", conf.getChildTomcatUrl());
        assertEquals(DBType.MONGO, conf.getChildDBType());
        assertEquals(27017, conf.getChildPort());
        assertEquals(MergePolicy.TIMESTAMP, conf.getMergePolicy());
        assertEquals(Boolean.FALSE, conf.isUseNtpServer());
        assertEquals(null, conf.getNtpServerHost());
    }
}