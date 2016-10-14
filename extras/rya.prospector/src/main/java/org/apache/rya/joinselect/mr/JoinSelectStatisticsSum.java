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
import static org.apache.rya.joinselect.mr.utils.JoinSelectConstants.INPUTPATH;
import static org.apache.rya.joinselect.mr.utils.JoinSelectConstants.SELECTIVITY_TABLE;

import java.io.IOException;

import org.apache.rya.joinselect.mr.utils.CardList;
import org.apache.rya.joinselect.mr.utils.JoinSelectStatsUtil;
import org.apache.rya.joinselect.mr.utils.TripleEntry;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;

public class JoinSelectStatisticsSum extends Configured implements Tool {

  // TODO need to tweak this class to compute join cardinalities over more than one variable

  public static class CardinalityIdentityMapper extends Mapper<TripleEntry,CardList,TripleEntry,CardList> {

    public void map(TripleEntry key, CardList value, Context context) throws IOException, InterruptedException {

      // System.out.println("Keys are " + key + " and values are " + value);

      if (key.getSecond().toString().length() != 0 && key.getSecondPos().toString().length() != 0) {
        TripleEntry te1 = new TripleEntry(key.getFirst(), new Text(""), key.getFirstPos(), new Text(""), key.getKeyPos());
        TripleEntry te2 = new TripleEntry(key.getSecond(), new Text(""), key.getSecondPos(), new Text(""), key.getKeyPos());

        context.write(te1, value);
        context.write(te2, value);
        context.write(key, value);
        // System.out.println("Output key values from mapper are " + te1 + " and " + value + "\n"
        // + te2 + " and " + value + "\n" + key + " and " + value + "\n");
      } else if (key.getSecond().toString().length() == 0 && key.getSecondPos().toString().length() == 0) {

        context.write(key, value);
        // System.out.println("Output key values from mapper are " + "\n" + key + " and " + value + "\n" + "\n");
      }

    }

  }

  public static class CardinalityIdentityReducer extends Reducer<TripleEntry,CardList,Text,Mutation> {

    private static final String DELIM = "\u0000";

    public void reduce(TripleEntry te, Iterable<CardList> values, Context context) throws IOException, InterruptedException {

      CardList cl = new CardList();
      LongWritable s = new LongWritable(0);
      LongWritable p = new LongWritable(0);
      LongWritable o = new LongWritable(0);
      LongWritable sp = new LongWritable(0);
      LongWritable po = new LongWritable(0);
      LongWritable so = new LongWritable(0);

      // System.out.println("***********************************************************\n"
      // + "key is " + te);

      for (CardList val : values) {
        // System.out.println("Value is " + val);
        s.set(s.get() + val.getcardS().get());
        p.set(p.get() + val.getcardP().get());
        o.set(o.get() + val.getcardO().get());
        sp.set(sp.get() + val.getcardSP().get());
        po.set(po.get() + val.getcardPO().get());
        so.set(so.get() + val.getcardSO().get());
      }
      cl.setCard(s, p, o, sp, po, so);

      Text row;

      if (te.getSecond().toString().length() > 0) {
        row = new Text(te.getFirstPos().toString() + te.getSecondPos().toString() + DELIM + te.getFirst().toString() + DELIM + te.getSecond());
      } else {
        row = new Text(te.getFirstPos().toString() + DELIM + te.getFirst().toString());
      }

      Mutation m1, m2, m3;

      if (te.getKeyPos().toString().equals("subject") || te.getKeyPos().toString().equals("predicate") || te.getKeyPos().toString().equals("object")) {
        m1 = new Mutation(row);
        m1.put(new Text(te.getKeyPos().toString() + "subject"), new Text(cl.getcardS().toString()), new Value(new byte[0]));
        m2 = new Mutation(row);
        m2.put(new Text(te.getKeyPos().toString() + "predicate"), new Text(cl.getcardP().toString()), new Value(new byte[0]));
        m3 = new Mutation(row);
        m3.put(new Text(te.getKeyPos().toString() + "object"), new Text(cl.getcardO().toString()), new Value(new byte[0]));

      } else if (te.getKeyPos().toString().equals("predicatesubject") || te.getKeyPos().toString().equals("objectpredicate")
          || te.getKeyPos().toString().equals("subjectobject")) {

        String jOrder = reverseJoinOrder(te.getKeyPos().toString());

        m1 = new Mutation(row);
        m1.put(new Text(jOrder + "predicatesubject"), new Text(cl.getcardSP().toString()), new Value(new byte[0]));
        m2 = new Mutation(row);
        m2.put(new Text(jOrder + "objectpredicate"), new Text(cl.getcardPO().toString()), new Value(new byte[0]));
        m3 = new Mutation(row);
        m3.put(new Text(jOrder + "subjectobject"), new Text(cl.getcardSO().toString()), new Value(new byte[0]));

      } else {

        m1 = new Mutation(row);
        m1.put(new Text(te.getKeyPos().toString() + "subjectpredicate"), new Text(cl.getcardSP().toString()), new Value(new byte[0]));
        m2 = new Mutation(row);
        m2.put(new Text(te.getKeyPos().toString() + "predicateobject"), new Text(cl.getcardPO().toString()), new Value(new byte[0]));
        m3 = new Mutation(row);
        m3.put(new Text(te.getKeyPos().toString() + "objectsubject"), new Text(cl.getcardSO().toString()), new Value(new byte[0]));

      }

      // TODO add the appropriate table name here
      context.write(new Text(""), m1);
      context.write(new Text(""), m2);
      context.write(new Text(""), m3);
    }

    private String reverseJoinOrder(String s) {

      if (s.equals("predicatesubject")) {
        return "subjectpredicate";
      } else if (s.equals("objectpredicate")) {
        return "predicateobject";
      } else if (s.equals("subjectobject")) {
        return "objectsubject";
      } else {
        throw new IllegalArgumentException("Invalid join type.");
      }

    }

  }

  public static class CardinalityIdentityCombiner extends Reducer<TripleEntry,CardList,TripleEntry,CardList> {

    @Override
    public void reduce(TripleEntry key, Iterable<CardList> values, Context context) throws IOException, InterruptedException {

      CardList cl = new CardList();
      LongWritable s = new LongWritable(0);
      LongWritable p = new LongWritable(0);
      LongWritable o = new LongWritable(0);
      LongWritable sp = new LongWritable(0);
      LongWritable po = new LongWritable(0);
      LongWritable so = new LongWritable(0);

      for (CardList val : values) {
        s.set(s.get() + val.getcardS().get());
        p.set(p.get() + val.getcardP().get());
        o.set(o.get() + val.getcardO().get());
        sp.set(sp.get() + val.getcardSP().get());
        po.set(po.get() + val.getcardPO().get());
        so.set(so.get() + val.getcardSO().get());
      }

      cl.setCard(s, p, o, sp, po, so);
      context.write(key, cl);

    }

  }

  @Override
  public int run(String[] args) throws AccumuloSecurityException, IOException, ClassNotFoundException, InterruptedException {

    Configuration conf = getConf();
    String outTable = conf.get(SELECTIVITY_TABLE);
    String auths = conf.get(AUTHS);
    String inPath = conf.get(INPUTPATH);

    assert outTable != null && inPath != null;

    Job job = new Job(getConf(), this.getClass().getSimpleName() + "_" + System.currentTimeMillis());
    job.setJarByClass(this.getClass());
    JoinSelectStatsUtil.initSumMRJob(job, inPath, outTable, auths);

    job.setMapperClass(CardinalityIdentityMapper.class);
    job.setCombinerClass(CardinalityIdentityCombiner.class);
    job.setReducerClass(CardinalityIdentityReducer.class);
    job.setNumReduceTasks(32);

    job.waitForCompletion(true);

    return job.isSuccessful() ? 0 : 1;

  }

}
