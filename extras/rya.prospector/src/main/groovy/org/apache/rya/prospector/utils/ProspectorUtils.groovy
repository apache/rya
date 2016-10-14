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

package org.apache.rya.prospector.utils

import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.client.Instance
import org.apache.accumulo.core.client.ZooKeeperInstance
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.data.Mutation
import org.apache.accumulo.core.security.Authorizations
import org.apache.commons.lang.Validate
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.hadoop.mapreduce.Job

import java.text.SimpleDateFormat
import org.apache.rya.prospector.plans.IndexWorkPlan
import org.apache.accumulo.core.client.security.tokens.PasswordToken

import static org.apache.rya.prospector.utils.ProspectorConstants.*

/**
 * Date: 12/4/12
 * Time: 4:24 PM
 */
class ProspectorUtils {

    public static final long INDEXED_DATE_SORT_VAL = 999999999999999999L; // 18 char long, same length as date format pattern below
    public static final String INDEXED_DATE_FORMAT = "yyyyMMddHHmmsssSSS";

    public static String getReverseIndexDateTime(Date date) {
        Validate.notNull(date);
        String formattedDateString = new SimpleDateFormat(INDEXED_DATE_FORMAT).format(date);
        long diff = INDEXED_DATE_SORT_VAL - Long.valueOf(formattedDateString);

        return Long.toString(diff);
    }

    public static Map<String, IndexWorkPlan> planMap(def plans) {
        plans.inject([:]) { map, plan ->
            map.putAt(plan.indexType, plan)
            map
        }
    }

    public static void initMRJob(Job job, String table, String outtable, String[] auths) {
        Configuration conf = job.configuration
        String username = conf.get(USERNAME)
        String password = conf.get(PASSWORD)
        String instance = conf.get(INSTANCE)
        String zookeepers = conf.get(ZOOKEEPERS)
        String mock = conf.get(MOCK)

        //input
        if (Boolean.parseBoolean(mock)) {
            AccumuloInputFormat.setMockInstance(job, instance)
            AccumuloOutputFormat.setMockInstance(job, instance)
        } else if (zookeepers != null) {
            AccumuloInputFormat.setZooKeeperInstance(job, instance, zookeepers)
            AccumuloOutputFormat.setZooKeeperInstance(job, instance, zookeepers)
        } else {
            throw new IllegalArgumentException("Must specify either mock or zookeepers");
        }

       AccumuloInputFormat.setConnectorInfo(job, username, new PasswordToken(password.getBytes()))
        AccumuloInputFormat.setInputTableName(job, table)
        job.setInputFormatClass(AccumuloInputFormat.class);
		AccumuloInputFormat.setScanAuthorizations(job, new Authorizations(auths))
		
        // OUTPUT
        job.setOutputFormatClass(AccumuloOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Mutation.class);
        AccumuloOutputFormat.setConnectorInfo(job, username, new PasswordToken(password.getBytes()))
        AccumuloOutputFormat.setDefaultTableName(job, outtable)
    }

    public static void addMRPerformance(Configuration conf) {
        conf.setBoolean("mapred.map.tasks.speculative.execution", false);
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
        conf.set("io.sort.mb", "256");
        conf.setBoolean("mapred.compress.map.output", true);
        conf.set("mapred.map.output.compression.codec", GzipCodec.class.getName());
    }

    public static Instance instance(Configuration conf) {
        assert conf != null

        String instance_str = conf.get(INSTANCE)
        String zookeepers = conf.get(ZOOKEEPERS)
        String mock = conf.get(MOCK)
        if (Boolean.parseBoolean(mock)) {
            return new MockInstance(instance_str)
        } else if (zookeepers != null) {
            return new ZooKeeperInstance(instance_str, zookeepers)
        } else {
            throw new IllegalArgumentException("Must specify either mock or zookeepers");
        }
    }

    public static Connector connector(Instance instance, Configuration conf) {
        String username = conf.get(USERNAME)
        String password = conf.get(PASSWORD)
        if (instance == null)
            instance = instance(conf)
        return instance.getConnector(username, password)
    }

    public static void writeMutations(Connector connector, String tableName, def mutations) {
        def bw = connector.createBatchWriter(tableName, 10000l, 10000l, 4);
        mutations.each { m ->
            bw.addMutation(m)
        }
        bw.flush()
        bw.close()
    }

}
