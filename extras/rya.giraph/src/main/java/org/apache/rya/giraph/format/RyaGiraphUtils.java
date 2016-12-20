package org.apache.rya.giraph.format;
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

import java.io.IOException;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.rya.accumulo.AccumuloRdfConstants;
import org.apache.rya.accumulo.mr.MRUtils;
import org.apache.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;
import org.apache.rya.api.RdfCloudTripleStoreUtils;
import org.apache.rya.indexing.accumulo.ConfigUtils;

public class RyaGiraphUtils {

    public static void initializeAccumuloInputFormat(Configuration conf){
        // get accumulo connect information
        boolean mock = MRUtils.getACMock(conf, false);
        String zk = MRUtils.getACZK(conf);
        String instance = MRUtils.getACInstance(conf);
        String userName = MRUtils.getACUserName(conf);
        String pwd = MRUtils.getACPwd(conf);
        String tablePrefix = MRUtils.getTablePrefix(conf);
        TABLE_LAYOUT rdfTableLayout = MRUtils.getTableLayout(conf, TABLE_LAYOUT.SPO);
        String authString = conf.get(MRUtils.AC_AUTH_PROP);
        Authorizations authorizations;
        if (authString != null && !authString.isEmpty()) {
            authorizations = new Authorizations(authString.split(","));
            conf.set(ConfigUtils.CLOUDBASE_AUTHS, authString); // for consistency
        }
        else {
            authorizations = AccumuloRdfConstants.ALL_AUTHORIZATIONS;
        }
        
        
        // set up the accumulo input format so that we know what table to use and everything
        try {
            Job job = new Job(conf);
            AccumuloInputFormat.setConnectorInfo(job, userName, new PasswordToken(pwd));
            String tableName = RdfCloudTripleStoreUtils.layoutPrefixToTable(rdfTableLayout, tablePrefix);
            AccumuloInputFormat.setInputTableName(job, tableName);
            AccumuloInputFormat.setScanAuthorizations(job, authorizations);
            if (mock) {
                AccumuloInputFormat.setMockInstance(job, instance);
            } else {
                ClientConfiguration clientConfig = ClientConfiguration.loadDefault()
                        .withInstance(instance).withZkHosts(zk);
                AccumuloInputFormat.setZooKeeperInstance(job, clientConfig);
            }
        } catch (IOException | AccumuloSecurityException e) {
            // TODO better exception handling here
            e.printStackTrace();
        }

    }
}
