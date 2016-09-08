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
package org.apache.rya.export.api.conf;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Date;

import org.apache.rya.export.DBType;
import org.apache.rya.export.JAXBAccumuloMergeConfiguration;
import org.apache.rya.export.MergePolicy;
import org.apache.rya.export.accumulo.common.InstanceType;
import org.apache.rya.export.accumulo.conf.AccumuloExportConstants;
import org.apache.rya.export.accumulo.driver.AccumuloDualInstanceDriver;
import org.apache.rya.export.accumulo.util.AccumuloInstanceDriver;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests the methods of {@link AccumuloConfigurationAdapter}.
 */
public class AccumuloConfigurationAdapterTest {
    private static final InstanceType INSTANCE_TYPE = InstanceType.MOCK;

    private static final boolean IS_MOCK = INSTANCE_TYPE.isMock();
    private static final boolean USE_TIME_SYNC = false;

    private static final String PARENT_HOST_NAME = "localhost:1234";
    private static final int PARENT_PORT = 1111;
    private static final String PARENT_USER_NAME = IS_MOCK ? "parent_user" : AccumuloInstanceDriver.ROOT_USER_NAME;
    private static final String PARENT_PASSWORD = AccumuloDualInstanceDriver.PARENT_PASSWORD;
    private static final String PARENT_INSTANCE = AccumuloDualInstanceDriver.PARENT_INSTANCE;
    private static final String PARENT_TABLE_PREFIX = AccumuloDualInstanceDriver.PARENT_TABLE_PREFIX;
    private static final String PARENT_AUTH = AccumuloDualInstanceDriver.PARENT_AUTH;
    private static final String PARENT_TOMCAT_URL = "http://localhost:8080";
    private static final String PARENT_ZOOKEEPERS = "http://rya-example-box:9090";

    private static final String CHILD_HOST_NAME = "localhost:4321";
    private static final int CHILD_PORT = 2222;
    private static final String CHILD_USER_NAME = IS_MOCK ? "child_user" : AccumuloInstanceDriver.ROOT_USER_NAME;
    private static final String CHILD_PASSWORD = AccumuloDualInstanceDriver.CHILD_PASSWORD;
    private static final String CHILD_INSTANCE = AccumuloDualInstanceDriver.CHILD_INSTANCE;
    private static final String CHILD_TABLE_PREFIX = AccumuloDualInstanceDriver.CHILD_TABLE_PREFIX;
    private static final String CHILD_AUTH = AccumuloDualInstanceDriver.CHILD_AUTH;
    private static final String CHILD_TOMCAT_URL = "http://localhost:8080";
    private static final String CHILD_ZOOKEEPERS = "http://localhost:9999";


    private static final String TOOL_START_TIME = AccumuloExportConstants.convertDateToStartTimeString(new Date());
    private static final String TIME_SERVER = "time.nist.gov";

    @Test
    public void testCreateConfig() throws MergeConfigurationException {
        final JAXBAccumuloMergeConfiguration jConfig = mock(JAXBAccumuloMergeConfiguration.class);
        // Parent Properties
        when(jConfig.getParentHostname()).thenReturn(PARENT_HOST_NAME);
        when(jConfig.getParentPort()).thenReturn(PARENT_PORT);
        when(jConfig.getParentRyaInstanceName()).thenReturn(PARENT_INSTANCE);
        when(jConfig.getParentUsername()).thenReturn(PARENT_USER_NAME);
        when(jConfig.getParentPassword()).thenReturn(PARENT_PASSWORD);
        when(jConfig.getParentTablePrefix()).thenReturn(PARENT_TABLE_PREFIX);
        when(jConfig.getParentDBType()).thenReturn(DBType.ACCUMULO);
        when(jConfig.getParentTomcatUrl()).thenReturn(PARENT_TOMCAT_URL);
        // Parent Accumulo Properties
        when(jConfig.getParentInstanceType()).thenReturn(INSTANCE_TYPE.toString());
        when(jConfig.getParentAuths()).thenReturn(PARENT_AUTH);
        when(jConfig.getParentZookeepers()).thenReturn(PARENT_ZOOKEEPERS);

        // Child Properties
        when(jConfig.getChildHostname()).thenReturn(CHILD_HOST_NAME);
        when(jConfig.getChildPort()).thenReturn(CHILD_PORT);
        when(jConfig.getChildRyaInstanceName()).thenReturn(CHILD_INSTANCE);
        when(jConfig.getChildUsername()).thenReturn(CHILD_USER_NAME);
        when(jConfig.getChildPassword()).thenReturn(CHILD_PASSWORD);
        when(jConfig.getChildTablePrefix()).thenReturn(CHILD_TABLE_PREFIX);
        when(jConfig.getChildDBType()).thenReturn(DBType.ACCUMULO);
        when(jConfig.getChildTomcatUrl()).thenReturn(CHILD_TOMCAT_URL);
        // Child Accumulo Properties
        when(jConfig.getChildInstanceType()).thenReturn(INSTANCE_TYPE.toString());
        when(jConfig.getChildAuths()).thenReturn(CHILD_AUTH);
        when(jConfig.getChildZookeepers()).thenReturn(CHILD_ZOOKEEPERS);
        // Other Properties
        when(jConfig.getMergePolicy()).thenReturn(MergePolicy.TIMESTAMP);
        when(jConfig.getNtpServerHost()).thenReturn(TIME_SERVER);
        when(jConfig.isUseNtpServer()).thenReturn(USE_TIME_SYNC);
        when(jConfig.getToolStartTime()).thenReturn(TOOL_START_TIME);


        final AccumuloMergeConfiguration accumuloMergeConfiguration = AccumuloConfigurationAdapter.createConfig(jConfig);

        Assert.assertNotNull(accumuloMergeConfiguration);
        Assert.assertEquals(AccumuloMergeConfiguration.class, accumuloMergeConfiguration.getClass());

        // Parent Properties
        Assert.assertEquals(PARENT_HOST_NAME, accumuloMergeConfiguration.getParentHostname());
        Assert.assertEquals(PARENT_USER_NAME, accumuloMergeConfiguration.getParentUsername());
        Assert.assertEquals(PARENT_PASSWORD, accumuloMergeConfiguration.getParentPassword());
        Assert.assertEquals(PARENT_INSTANCE, accumuloMergeConfiguration.getParentRyaInstanceName());
        Assert.assertEquals(PARENT_TABLE_PREFIX, accumuloMergeConfiguration.getParentTablePrefix());
        Assert.assertEquals(PARENT_TOMCAT_URL, accumuloMergeConfiguration.getParentTomcatUrl());
        Assert.assertEquals(DBType.ACCUMULO, accumuloMergeConfiguration.getParentDBType());
        Assert.assertEquals(PARENT_PORT, accumuloMergeConfiguration.getParentPort());
        // Parent Accumulo Properties
        Assert.assertEquals(PARENT_ZOOKEEPERS, accumuloMergeConfiguration.getParentZookeepers());
        Assert.assertEquals(PARENT_AUTH, accumuloMergeConfiguration.getParentAuths());
        Assert.assertEquals(InstanceType.MOCK, accumuloMergeConfiguration.getParentInstanceType());

        // Child Properties
        Assert.assertEquals(CHILD_HOST_NAME, accumuloMergeConfiguration.getChildHostname());
        Assert.assertEquals(CHILD_USER_NAME, accumuloMergeConfiguration.getChildUsername());
        Assert.assertEquals(CHILD_PASSWORD, accumuloMergeConfiguration.getChildPassword());
        Assert.assertEquals(CHILD_INSTANCE, accumuloMergeConfiguration.getChildRyaInstanceName());
        Assert.assertEquals(CHILD_TABLE_PREFIX, accumuloMergeConfiguration.getChildTablePrefix());
        Assert.assertEquals(CHILD_TOMCAT_URL, accumuloMergeConfiguration.getChildTomcatUrl());
        Assert.assertEquals(DBType.ACCUMULO, accumuloMergeConfiguration.getChildDBType());
        Assert.assertEquals(CHILD_PORT, accumuloMergeConfiguration.getChildPort());
        // Child Properties
        Assert.assertEquals(CHILD_ZOOKEEPERS, accumuloMergeConfiguration.getChildZookeepers());
        Assert.assertEquals(CHILD_AUTH, accumuloMergeConfiguration.getChildAuths());
        Assert.assertEquals(InstanceType.MOCK, accumuloMergeConfiguration.getChildInstanceType());

        // Other Properties
        Assert.assertEquals(MergePolicy.TIMESTAMP, accumuloMergeConfiguration.getMergePolicy());
        Assert.assertEquals(Boolean.FALSE, accumuloMergeConfiguration.getUseNtpServer());
        Assert.assertEquals(TIME_SERVER, accumuloMergeConfiguration.getNtpServerHost());
        Assert.assertEquals(TOOL_START_TIME, accumuloMergeConfiguration.getToolStartTime());
    }
}
