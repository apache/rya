package org.apache.rya.joinselect.mr;

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



import static org.apache.rya.joinselect.mr.utils.JoinSelectConstants.AUTHS;
import static org.apache.rya.joinselect.mr.utils.JoinSelectConstants.SELECTIVITY_TABLE;
import static org.apache.rya.joinselect.mr.utils.JoinSelectConstants.SPO_TABLE;

import java.io.IOException;

import org.apache.rya.joinselect.mr.utils.JoinSelectStatsUtil;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FullTableSize extends Configured implements Tool {

  private static final String DELIM = "\u0000";

  
  
  
  
  public static void main(String[] args) throws Exception {
      ToolRunner.run(new FullTableSize(), args);
  }
  
  
  
  
  
  
  public static class FullTableMapper extends Mapper<Key,Value,Text,IntWritable> {
    private static final IntWritable ONE = new IntWritable(1);


    @Override
    public void map(Key key, Value value, Context context) throws IOException, InterruptedException {
      context.write(new Text("COUNT"), ONE);
    }
  }

  public static class FullTableReducer extends Reducer<Text,IntWritable,Text,Mutation> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int count = 0;

      for (IntWritable i : values) {
        count += i.get();
      }

      String countStr = Integer.toString(count);

      Mutation m = new Mutation(new Text("subjectpredicateobject" + DELIM + "FullTableCardinality"));
      m.put(new Text("FullTableCardinality"), new Text(countStr), new Value(new byte[0]));

      context.write(new Text(""), m);
    }
  }

  public static class FullTableCombiner extends Reducer<Text,IntWritable,Text,IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

      int count = 0;

      for (IntWritable i : values) {
        count += i.get();
      }

      context.write(key, new IntWritable(count));
    }
  }

  @Override
  public int run(String[] args) throws Exception {

    Configuration conf = getConf();
    String inTable = conf.get(SPO_TABLE);
    String outTable = conf.get(SELECTIVITY_TABLE);
    String auths = conf.get(AUTHS);

    assert inTable != null && outTable != null;

    Job job = new Job(getConf(), this.getClass().getSimpleName() + "_" + System.currentTimeMillis());
    job.setJarByClass(this.getClass());
    JoinSelectStatsUtil.initTableMRJob(job, inTable, outTable, auths);
    job.setMapperClass(FullTableMapper.class);
    job.setCombinerClass(FullTableCombiner.class);
    job.setReducerClass(FullTableReducer.class);
    job.setNumReduceTasks(1);

    job.waitForCompletion(true);

    return job.isSuccessful() ? 0 : 1;
  }

}
