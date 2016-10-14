/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.accumulo.mr.merge;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.util.Tool;

import org.apache.rya.accumulo.AccumuloRdfConstants;
import org.apache.rya.accumulo.mr.AbstractAccumuloMRTool;
import org.apache.rya.accumulo.mr.MRUtils;
import org.apache.rya.api.RdfCloudTripleStoreConstants;

/**
 * Handles setting up a map reduce {@link Tool} with a parent and child instance.
 */
public abstract class AbstractDualInstanceAccumuloMRTool extends AbstractAccumuloMRTool implements Tool {
    protected String childUserName = "root";
    protected String childPwd = "root";
    protected String childInstance = "instance";
    protected String childZk = "zoo";
    protected Authorizations childAuthorizations = AccumuloRdfConstants.ALL_AUTHORIZATIONS;
    protected boolean childMock = false;
    protected String childTablePrefix = RdfCloudTripleStoreConstants.TBL_PRFX_DEF;

    @Override
    protected void init() {
        super.init();

        childZk = conf.get(MRUtils.AC_ZK_PROP + MergeTool.CHILD_SUFFIX, childZk);
        childInstance = conf.get(MRUtils.AC_INSTANCE_PROP + MergeTool.CHILD_SUFFIX, childInstance);
        childUserName = conf.get(MRUtils.AC_USERNAME_PROP + MergeTool.CHILD_SUFFIX, childUserName);
        childPwd = conf.get(MRUtils.AC_PWD_PROP + MergeTool.CHILD_SUFFIX, pwd);
        childMock = conf.getBoolean(MRUtils.AC_MOCK_PROP + MergeTool.CHILD_SUFFIX, mock);
        childTablePrefix = conf.get(MRUtils.TABLE_PREFIX_PROPERTY + MergeTool.CHILD_SUFFIX, childTablePrefix);
        if (childTablePrefix != null) {
            RdfCloudTripleStoreConstants.prefixTables(childTablePrefix);
        }
        final String childAuth = conf.get(MRUtils.AC_AUTH_PROP + MergeTool.CHILD_SUFFIX);
        if (childAuth != null) {
            childAuthorizations = new Authorizations(childAuth.split(","));
        }
    }

    @Override
    public abstract int run(String[] args) throws Exception;
}