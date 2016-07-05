package mvm.rya.accumulo.mr;

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



import mvm.rya.accumulo.AccumuloRdfConstants;
import mvm.rya.accumulo.mr.utils.AccumuloHDFSFileInputFormat;
import mvm.rya.accumulo.mr.utils.MRUtils;
import mvm.rya.api.RdfCloudTripleStoreConstants;
import mvm.rya.api.RdfCloudTripleStoreUtils;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.iterators.user.AgeOffFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

/**
 */
public abstract class AbstractAccumuloMRTool {

    protected Configuration conf;
    protected RdfCloudTripleStoreConstants.TABLE_LAYOUT rdfTableLayout = RdfCloudTripleStoreConstants.TABLE_LAYOUT.OSP;
    protected String userName = "root";
    protected String pwd = "root";
    protected String instance = "instance";
    protected String zk = "zoo";
    protected Authorizations authorizations = AccumuloRdfConstants.ALL_AUTHORIZATIONS;
    protected String ttl = null;
    protected boolean mock = false;
    protected boolean hdfsInput = false;
    protected String tablePrefix = RdfCloudTripleStoreConstants.TBL_PRFX_DEF;

    protected void init() {
        zk = conf.get(MRUtils.AC_ZK_PROP, zk);
        ttl = conf.get(MRUtils.AC_TTL_PROP, ttl);
        instance = conf.get(MRUtils.AC_INSTANCE_PROP, instance);
        userName = conf.get(MRUtils.AC_USERNAME_PROP, userName);
        pwd = conf.get(MRUtils.AC_PWD_PROP, pwd);
        mock = conf.getBoolean(MRUtils.AC_MOCK_PROP, mock);
        hdfsInput = conf.getBoolean(MRUtils.AC_HDFS_INPUT_PROP, hdfsInput);
        tablePrefix = conf.get(MRUtils.TABLE_PREFIX_PROPERTY, tablePrefix);
        if (tablePrefix != null)
            RdfCloudTripleStoreConstants.prefixTables(tablePrefix);
        rdfTableLayout = RdfCloudTripleStoreConstants.TABLE_LAYOUT.valueOf(
                conf.get(MRUtils.TABLE_LAYOUT_PROP, RdfCloudTripleStoreConstants.TABLE_LAYOUT.OSP.toString()));
        String auth = conf.get(MRUtils.AC_AUTH_PROP);
        if (auth != null)
            authorizations = new Authorizations(auth.split(","));

        if (!mock) {
            conf.setBoolean("mapred.map.tasks.speculative.execution", false);
            conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
            conf.set("io.sort.mb", "256");
        }

        //set ttl
        ttl = conf.get(MRUtils.AC_TTL_PROP);
    }

    protected void setupInputFormat(Job job) throws AccumuloSecurityException {
        // set up accumulo input
        if (!hdfsInput) {
            job.setInputFormatClass(AccumuloInputFormat.class);
        } else {
            job.setInputFormatClass(AccumuloHDFSFileInputFormat.class);
        }
        AccumuloInputFormat.setConnectorInfo(job, userName, new PasswordToken(pwd));
        AccumuloInputFormat.setInputTableName(job, RdfCloudTripleStoreUtils.layoutPrefixToTable(rdfTableLayout, tablePrefix));
        AccumuloInputFormat.setScanAuthorizations(job, authorizations);
        if (!mock) {
            AccumuloInputFormat.setZooKeeperInstance(job, instance, zk);
        } else {
            AccumuloInputFormat.setMockInstance(job, instance);
        }
        if (ttl != null) {
            IteratorSetting setting = new IteratorSetting(1, "fi", AgeOffFilter.class.getName());
            AgeOffFilter.setTTL(setting, Long.valueOf(ttl));
            AccumuloInputFormat.addIterator(job, setting);
        }
    }

    protected void setupOutputFormat(Job job, String outputTable) throws AccumuloSecurityException {
        AccumuloOutputFormat.setConnectorInfo(job, userName, new PasswordToken(pwd));
        AccumuloOutputFormat.setCreateTables(job, true);
        AccumuloOutputFormat.setDefaultTableName(job, outputTable);
        if (!mock) {
            AccumuloOutputFormat.setZooKeeperInstance(job, instance, zk);
        } else {
            AccumuloOutputFormat.setMockInstance(job, instance);
        }
        job.setOutputFormatClass(AccumuloOutputFormat.class);
    }

    public void setConf(Configuration configuration) {
        this.conf = configuration;
    }

    public Configuration getConf() {
        return conf;
    }

    public String getInstance() {
        return instance;
    }

    public void setInstance(String instance) {
        this.instance = instance;
    }

    public String getPwd() {
        return pwd;
    }

    public void setPwd(String pwd) {
        this.pwd = pwd;
    }

    public String getZk() {
        return zk;
    }

    public void setZk(String zk) {
        this.zk = zk;
    }

    public String getTtl() {
        return ttl;
    }

    public void setTtl(String ttl) {
        this.ttl = ttl;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }
}
