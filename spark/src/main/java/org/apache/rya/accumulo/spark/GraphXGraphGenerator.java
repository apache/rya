package org.apache.rya.accumulo.spark;

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
import org.apache.accumulo.core.client.ClientConfiguration.ClientProperty;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.rya.accumulo.AccumuloRdfConstants;
import org.apache.rya.accumulo.mr.GraphXEdgeInputFormat;
import org.apache.rya.accumulo.mr.GraphXInputFormat;
import org.apache.rya.accumulo.mr.MRUtils;
import org.apache.rya.accumulo.mr.RyaInputFormat;
import org.apache.rya.accumulo.mr.RyaTypeWritable;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.RdfCloudTripleStoreConstants;
import org.apache.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;
import org.apache.rya.api.RdfCloudTripleStoreUtils;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.accumulo.entity.EntityCentricIndex;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.rdd.RDD;
import org.apache.spark.storage.StorageLevel;

import com.google.common.base.Preconditions;

import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class GraphXGraphGenerator {

    public String zk;
    public String instance;
    public String userName;
    public String pwd;
    public boolean mock;
    public String tablePrefix;
    public Authorizations authorizations;

    public RDD<Tuple2<Object, RyaTypeWritable>> getVertexRDD(SparkContext sc, Configuration conf) throws IOException, AccumuloSecurityException{
        // Load configuration parameters
        zk = MRUtils.getACZK(conf);
        instance = MRUtils.getACInstance(conf);
        userName = MRUtils.getACUserName(conf);
        pwd = MRUtils.getACPwd(conf);
        mock = MRUtils.getACMock(conf, false);
        tablePrefix = MRUtils.getTablePrefix(conf);
        // Set authorizations if specified
        String authString = conf.get(MRUtils.AC_AUTH_PROP);
        if (authString != null && !authString.isEmpty()) {
            authorizations = new Authorizations(authString.split(","));
            conf.set(ConfigUtils.CLOUDBASE_AUTHS, authString); // for consistency
        }
        else {
            authorizations = AccumuloRdfConstants.ALL_AUTHORIZATIONS;
        }
        // Set table prefix to the default if not set
        if (tablePrefix == null) {
            tablePrefix = RdfCloudTripleStoreConstants.TBL_PRFX_DEF;
            MRUtils.setTablePrefix(conf, tablePrefix);
        }
        // Check for required configuration parameters
        Preconditions.checkNotNull(instance, "Accumulo instance name [" + MRUtils.AC_INSTANCE_PROP + "] not set.");
        Preconditions.checkNotNull(userName, "Accumulo username [" + MRUtils.AC_USERNAME_PROP + "] not set.");
        Preconditions.checkNotNull(pwd, "Accumulo password [" + MRUtils.AC_PWD_PROP + "] not set.");
        Preconditions.checkNotNull(tablePrefix, "Table prefix [" + MRUtils.TABLE_PREFIX_PROPERTY + "] not set.");
        RdfCloudTripleStoreConstants.prefixTables(tablePrefix);
        // If connecting to real accumulo, set additional parameters and require zookeepers
        if (!mock) conf.set(ConfigUtils.CLOUDBASE_ZOOKEEPERS, zk); // for consistency
        // Ensure consistency between alternative configuration properties
        conf.set(ConfigUtils.CLOUDBASE_INSTANCE, instance);
        conf.set(ConfigUtils.CLOUDBASE_USER, userName);
        conf.set(ConfigUtils.CLOUDBASE_PASSWORD, pwd);
        conf.setBoolean(ConfigUtils.USE_MOCK_INSTANCE, mock);
        conf.set(RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX, tablePrefix);

        Job job = Job.getInstance(conf, sc.appName());

        ClientConfiguration clientConfig = new ClientConfiguration().with(ClientProperty.INSTANCE_NAME, instance).with(ClientProperty.INSTANCE_ZK_HOST, zk);

        GraphXInputFormat.setInputTableName(job, EntityCentricIndex.getTableName(conf));
        GraphXInputFormat.setConnectorInfo(job, userName, new PasswordToken(pwd));
        GraphXInputFormat.setZooKeeperInstance(job, clientConfig);
        GraphXInputFormat.setScanAuthorizations(job, authorizations);

        return sc.newAPIHadoopRDD(job.getConfiguration(), GraphXInputFormat.class, Object.class, RyaTypeWritable.class);
    }

    public RDD<Tuple2<Object, Edge>> getEdgeRDD(SparkContext sc, Configuration conf) throws IOException, AccumuloSecurityException{
        // Load configuration parameters
        zk = MRUtils.getACZK(conf);
        instance = MRUtils.getACInstance(conf);
        userName = MRUtils.getACUserName(conf);
        pwd = MRUtils.getACPwd(conf);
        mock = MRUtils.getACMock(conf, false);
        tablePrefix = MRUtils.getTablePrefix(conf);
        // Set authorizations if specified
        String authString = conf.get(MRUtils.AC_AUTH_PROP);
        if (authString != null && !authString.isEmpty()) {
            authorizations = new Authorizations(authString.split(","));
            conf.set(ConfigUtils.CLOUDBASE_AUTHS, authString); // for consistency
        }
        else {
            authorizations = AccumuloRdfConstants.ALL_AUTHORIZATIONS;
        }
        // Set table prefix to the default if not set
        if (tablePrefix == null) {
            tablePrefix = RdfCloudTripleStoreConstants.TBL_PRFX_DEF;
            MRUtils.setTablePrefix(conf, tablePrefix);
        }
        // Check for required configuration parameters
        Preconditions.checkNotNull(instance, "Accumulo instance name [" + MRUtils.AC_INSTANCE_PROP + "] not set.");
        Preconditions.checkNotNull(userName, "Accumulo username [" + MRUtils.AC_USERNAME_PROP + "] not set.");
        Preconditions.checkNotNull(pwd, "Accumulo password [" + MRUtils.AC_PWD_PROP + "] not set.");
        Preconditions.checkNotNull(tablePrefix, "Table prefix [" + MRUtils.TABLE_PREFIX_PROPERTY + "] not set.");
        RdfCloudTripleStoreConstants.prefixTables(tablePrefix);
        // If connecting to real accumulo, set additional parameters and require zookeepers
        if (!mock) conf.set(ConfigUtils.CLOUDBASE_ZOOKEEPERS, zk); // for consistency
        // Ensure consistency between alternative configuration properties
        conf.set(ConfigUtils.CLOUDBASE_INSTANCE, instance);
        conf.set(ConfigUtils.CLOUDBASE_USER, userName);
        conf.set(ConfigUtils.CLOUDBASE_PASSWORD, pwd);
        conf.setBoolean(ConfigUtils.USE_MOCK_INSTANCE, mock);
        conf.set(RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX, tablePrefix);

        Job job = Job.getInstance(conf, sc.appName());

        ClientConfiguration clientConfig = new ClientConfiguration().with(ClientProperty.INSTANCE_NAME, instance).with(ClientProperty.INSTANCE_ZK_HOST, zk);

        RyaInputFormat.setTableLayout(job, TABLE_LAYOUT.SPO);
        RyaInputFormat.setConnectorInfo(job, userName, new PasswordToken(pwd));
        RyaInputFormat.setZooKeeperInstance(job, clientConfig);
        RyaInputFormat.setScanAuthorizations(job, authorizations);
                String tableName = RdfCloudTripleStoreUtils.layoutPrefixToTable(TABLE_LAYOUT.SPO, tablePrefix);
                InputFormatBase.setInputTableName(job, tableName);
        return sc.newAPIHadoopRDD(job.getConfiguration(), GraphXEdgeInputFormat.class, Object.class, Edge.class);
    }

    public Graph<RyaTypeWritable, RyaTypeWritable> createGraph(SparkContext sc, Configuration conf) throws IOException, AccumuloSecurityException{
        StorageLevel storageLvl1 = StorageLevel.MEMORY_ONLY();
        StorageLevel storageLvl2 = StorageLevel.MEMORY_ONLY();
        ClassTag<RyaTypeWritable> RTWTag = ClassTag$.MODULE$.apply(RyaTypeWritable.class);
        RyaTypeWritable rtw = null;
        RDD<Tuple2<Object, RyaTypeWritable>> vertexRDD = getVertexRDD(sc, conf);

        RDD<Tuple2<Object, Edge>> edgeRDD = getEdgeRDD(sc, conf);
        JavaRDD<Tuple2<Object, Edge>> jrddTuple = edgeRDD.toJavaRDD();
        JavaRDD<Edge<RyaTypeWritable>> jrdd = jrddTuple.map(tuple -> tuple._2);

        RDD<Edge<RyaTypeWritable>> goodERDD = JavaRDD.toRDD(jrdd);

        return Graph.apply(vertexRDD, goodERDD, rtw, storageLvl1, storageLvl2, RTWTag, RTWTag);
    }
}
