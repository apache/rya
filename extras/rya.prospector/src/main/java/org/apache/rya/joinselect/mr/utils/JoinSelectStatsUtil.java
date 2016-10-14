package org.apache.rya.joinselect.mr.utils;

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



import static org.apache.rya.accumulo.AccumuloRdfConstants.EMPTY_CV;
import static org.apache.rya.accumulo.AccumuloRdfConstants.EMPTY_VALUE;
import static org.apache.rya.api.RdfCloudTripleStoreConstants.EMPTY_TEXT;
import static org.apache.rya.joinselect.mr.utils.JoinSelectConstants.INSTANCE;
import static org.apache.rya.joinselect.mr.utils.JoinSelectConstants.PASSWORD;
import static org.apache.rya.joinselect.mr.utils.JoinSelectConstants.USERNAME;
import static org.apache.rya.joinselect.mr.utils.JoinSelectConstants.ZOOKEEPERS;

import java.io.IOException;

import org.apache.rya.api.resolver.triple.TripleRow;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class JoinSelectStatsUtil {

  public static void initSumMRJob(Job job, String inputPath, String outtable, String auths) throws AccumuloSecurityException, IOException {
    Configuration conf = job.getConfiguration();
    String username = conf.get(USERNAME);
    String password = conf.get(PASSWORD);
    String instance = conf.get(INSTANCE);
    String zookeepers = conf.get(ZOOKEEPERS);

    if (zookeepers != null) {
      AccumuloOutputFormat.setConnectorInfo(job, username, new PasswordToken(password));
      AccumuloOutputFormat.setZooKeeperInstance(job, instance, zookeepers);
    } else {
      throw new IllegalArgumentException("Must specify zookeepers");
    }

    SequenceFileInputFormat.addInputPath(job, new Path(inputPath));
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setMapOutputKeyClass(TripleEntry.class);
    job.setMapOutputValueClass(CardList.class);

    AccumuloOutputFormat.setDefaultTableName(job, outtable);
    job.setOutputFormatClass(AccumuloOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Mutation.class);

  }

  public static void initTableMRJob(Job job, String intable, String outtable, String auths) throws AccumuloSecurityException {
    Configuration conf = job.getConfiguration();
    String username = conf.get(USERNAME);
    String password = conf.get(PASSWORD);
    String instance = conf.get(INSTANCE);
    String zookeepers = conf.get(ZOOKEEPERS);

    System.out.println("Zookeepers are " + auths);

    if (zookeepers != null) {
      AccumuloInputFormat.setZooKeeperInstance(job, instance, zookeepers);
      AccumuloOutputFormat.setZooKeeperInstance(job, instance, zookeepers);
    } else {
      throw new IllegalArgumentException("Must specify either mock or zookeepers");
    }

    AccumuloInputFormat.setConnectorInfo(job, username, new PasswordToken(password));
    AccumuloInputFormat.setScanAuthorizations(job, new Authorizations(auths));
    AccumuloInputFormat.setInputTableName(job, intable);
    job.setInputFormatClass(AccumuloInputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    // OUTPUT
    AccumuloOutputFormat.setConnectorInfo(job, username, new PasswordToken(password));
    AccumuloOutputFormat.setDefaultTableName(job, outtable);
    job.setOutputFormatClass(AccumuloOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Mutation.class);

  }

  public static void initTabToSeqFileJob(Job job, String intable, String outpath, String auths) throws AccumuloSecurityException {

    Configuration conf = job.getConfiguration();
    String username = conf.get(USERNAME);
    String password = conf.get(PASSWORD);
    String instance = conf.get(INSTANCE);
    String zookeepers = conf.get(ZOOKEEPERS);

    System.out.println("Zookeepers are " + auths);

    if (zookeepers != null) {
      AccumuloInputFormat.setZooKeeperInstance(job, instance, zookeepers);
    } else {
      throw new IllegalArgumentException("Must specify either mock or zookeepers");
    }

    AccumuloInputFormat.setConnectorInfo(job, username, new PasswordToken(password));
    AccumuloInputFormat.setScanAuthorizations(job, new Authorizations(auths));
    AccumuloInputFormat.setInputTableName(job, intable);
    job.setInputFormatClass(AccumuloInputFormat.class);
    job.setMapOutputKeyClass(CompositeType.class);
    job.setMapOutputValueClass(TripleCard.class);

    // OUTPUT
    SequenceFileOutputFormat.setOutputPath(job, new Path(outpath));
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(CompositeType.class);
    job.setOutputValueClass(TripleCard.class);

  }

  public static void initJoinMRJob(Job job, String prospectsPath, String spoPath, Class<? extends Mapper<CompositeType,TripleCard,?,?>> mapperClass,
      String outPath, String auths) throws AccumuloSecurityException {

    MultipleInputs.addInputPath(job, new Path(prospectsPath), SequenceFileInputFormat.class, mapperClass);
    MultipleInputs.addInputPath(job, new Path(spoPath), SequenceFileInputFormat.class, mapperClass);
    job.setMapOutputKeyClass(CompositeType.class);
    job.setMapOutputValueClass(TripleCard.class);

    SequenceFileOutputFormat.setOutputPath(job, new Path(outPath));
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(TripleEntry.class);
    job.setOutputValueClass(CardList.class);

  }

  public static Mutation createMutation(TripleRow tripleRow) {
    Mutation mutation = new Mutation(new Text(tripleRow.getRow()));
    byte[] columnVisibility = tripleRow.getColumnVisibility();
    ColumnVisibility cv = columnVisibility == null ? EMPTY_CV : new ColumnVisibility(columnVisibility);
    Long timestamp = tripleRow.getTimestamp();
    boolean hasts = timestamp != null;
    timestamp = timestamp == null ? 0l : timestamp;
    byte[] value = tripleRow.getValue();
    Value v = value == null ? EMPTY_VALUE : new Value(value);
    byte[] columnQualifier = tripleRow.getColumnQualifier();
    Text cqText = columnQualifier == null ? EMPTY_TEXT : new Text(columnQualifier);
    byte[] columnFamily = tripleRow.getColumnFamily();
    Text cfText = columnFamily == null ? EMPTY_TEXT : new Text(columnFamily);

    if (hasts) {
      mutation.put(cfText, cqText, cv, timestamp, v);
    } else {
      mutation.put(cfText, cqText, cv, v);

    }
    return mutation;
  }

}
