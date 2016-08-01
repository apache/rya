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
package org.apache.rya.export.accumulo;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.rya.export.accumulo.common.InstanceType;
import org.apache.rya.export.accumulo.conf.AccumuloExportConstants;
import org.junit.Test;

import mvm.rya.accumulo.AccumuloRdfConstants;
import mvm.rya.api.RdfCloudTripleStoreConstants;
import mvm.rya.indexing.accumulo.ConfigUtils;

/**
 * Tests the methods of {@link AccumuloMerger}.
 */
public class AccumuloMergerTest {
    private static final InstanceType PARENT_INSTANCE_TYPE = InstanceType.MOCK;
    private static final String PARENT_USER_NAME = "root";
    private static final String PARENT_PASSWORD = "password";
    private static final String PARENT_INSTANCE = "instance";
    private static final String PARENT_ZOOKEEPERS = "zoo";
    private static final Authorizations PARENT_AUTH =  AccumuloRdfConstants.ALL_AUTHORIZATIONS;
    private static final String PARENT_TABLE_PREFIX = RdfCloudTripleStoreConstants.TBL_PRFX_DEF;

    private static final InstanceType CHILD_INSTANCE_TYPE = InstanceType.MOCK;
    private static final String CHILD_USER_NAME = "root";
    private static final String CHILD_PASSWORD = "child_password";
    private static final String CHILD_INSTANCE = "child_instance";
    private static final String CHILD_ZOOKEEPERS = "child_zoo";
    private static final Authorizations CHILD_AUTH =  AccumuloRdfConstants.ALL_AUTHORIZATIONS;
    private static final String CHILD_TABLE_PREFIX = "child_" + RdfCloudTripleStoreConstants.TBL_PRFX_DEF;

    private Configuration parentConfig;
    private Configuration childConfig;

    private void initConfigs() {
        parentConfig = new Configuration();
        parentConfig.set(ConfigUtils.USE_MOCK_INSTANCE, Boolean.toString(PARENT_INSTANCE_TYPE.isMock()));
        parentConfig.set(AccumuloExportConstants.ACCUMULO_INSTANCE_TYPE_PROP, PARENT_INSTANCE_TYPE.toString());
        parentConfig.set(ConfigUtils.CLOUDBASE_INSTANCE, PARENT_INSTANCE);
        parentConfig.set(ConfigUtils.CLOUDBASE_USER, PARENT_USER_NAME);
        parentConfig.set(ConfigUtils.CLOUDBASE_PASSWORD, PARENT_PASSWORD);
        parentConfig.set(ConfigUtils.CLOUDBASE_AUTHS, PARENT_AUTH.toString());
        parentConfig.set(ConfigUtils.CLOUDBASE_TBL_PREFIX, PARENT_TABLE_PREFIX);

        childConfig = new Configuration();
        childConfig.set(ConfigUtils.USE_MOCK_INSTANCE + AccumuloExportConstants.CHILD_SUFFIX, Boolean.toString(CHILD_INSTANCE_TYPE.isMock()));
        childConfig.set(AccumuloExportConstants.ACCUMULO_INSTANCE_TYPE_PROP + AccumuloExportConstants.CHILD_SUFFIX, CHILD_INSTANCE_TYPE.toString());
        childConfig.set(ConfigUtils.CLOUDBASE_INSTANCE + AccumuloExportConstants.CHILD_SUFFIX, CHILD_INSTANCE);
        childConfig.set(ConfigUtils.CLOUDBASE_USER + AccumuloExportConstants.CHILD_SUFFIX, CHILD_USER_NAME);
        childConfig.set(ConfigUtils.CLOUDBASE_PASSWORD + AccumuloExportConstants.CHILD_SUFFIX, CHILD_PASSWORD);
        childConfig.set(ConfigUtils.CLOUDBASE_AUTHS + AccumuloExportConstants.CHILD_SUFFIX, CHILD_AUTH.toString());
        childConfig.set(ConfigUtils.CLOUDBASE_TBL_PREFIX + AccumuloExportConstants.CHILD_SUFFIX, CHILD_TABLE_PREFIX);
    }

    @Test
    public void testRunJob() throws Exception {
        initConfigs();
        AccumuloRyaStatementStore accumuloParentRyaStatementStore = new AccumuloRyaStatementStore(parentConfig);
        AccumuloRyaStatementStore childAccumuloRyaStatementStore = new AccumuloRyaStatementStore(childConfig);
        AccumuloMerger accumuloMerger = new AccumuloMerger(accumuloParentRyaStatementStore, childAccumuloRyaStatementStore);
        accumuloMerger.runJob();
    }
}