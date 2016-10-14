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
import static org.apache.rya.joinselect.mr.utils.JoinSelectConstants.PROSPECTS_OUTPUTPATH;
import static org.apache.rya.joinselect.mr.utils.JoinSelectConstants.PROSPECTS_TABLE;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.rya.joinselect.mr.utils.CardinalityType;
import org.apache.rya.joinselect.mr.utils.CompositeType;
import org.apache.rya.joinselect.mr.utils.JoinSelectStatsUtil;
import org.apache.rya.joinselect.mr.utils.TripleCard;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;

public class JoinSelectProspectOutput extends Configured implements Tool {

  public static class CardinalityMapper extends Mapper<Key,Value,CompositeType,TripleCard> {

    private static final String DELIM = "\u0000";

    Text inText = new Text();
    Pattern splitPattern = Pattern.compile(DELIM);

    public void map(Key key, Value data, Context context) throws IOException, InterruptedException {

      key.getRow(inText);
      String[] cardData = splitPattern.split(inText.toString().trim(), 4);
      // System.out.println("Card data is " + cardData[0] + ", "+ cardData[1] + ", "+ cardData[2]);
      if (cardData.length == 3 && ((cardData[0].equals("subject")) || (cardData[0].equals("object")) || (cardData[0].equals("predicate")))) {
        Text tripleValType = new Text(cardData[0]);
        Text cardKey = new Text(cardData[1]);
        LongWritable ts = new LongWritable(Long.valueOf(cardData[2]));

        String s = new String(data.get());
        LongWritable card = new LongWritable(Long.parseLong(s));

        CompositeType cType = new CompositeType(cardKey, new IntWritable(1));
        TripleCard tCard = new TripleCard(new CardinalityType(card, tripleValType, ts));

        context.write(new CompositeType(cardKey, new IntWritable(1)), new TripleCard(new CardinalityType(card, tripleValType, ts)));
        // System.out.println("Card mapper output key is " + cType + " and value is " + tCard );

      } else if (cardData.length == 4
          && ((cardData[0].equals("subjectpredicate")) || (cardData[0].equals("subjectobject")) || (cardData[0].equals("predicateobject")))) {

        Text tripleValType = new Text(cardData[0]);
        Text cardKey = new Text(cardData[1] + DELIM + cardData[2]);
        LongWritable ts = new LongWritable(Long.valueOf(cardData[3]));

        String s = new String(data.get());
        LongWritable card = new LongWritable(Long.parseLong(s));

        CompositeType cType = new CompositeType(cardKey, new IntWritable(1));
        TripleCard tCard = new TripleCard(new CardinalityType(card, tripleValType, ts));

        context.write(new CompositeType(cardKey, new IntWritable(1)), new TripleCard(new CardinalityType(card, tripleValType, ts)));
        // System.out.println("Card mapper output key is " + cType + " and value is " + tCard );

      }

    }

  }

  @Override
  public int run(String[] args) throws AccumuloSecurityException, IOException, ClassNotFoundException, InterruptedException {

    Configuration conf = getConf();
    String inTable = conf.get(PROSPECTS_TABLE);
    String auths = conf.get(AUTHS);
    String outPath = conf.get(PROSPECTS_OUTPUTPATH);

    assert inTable != null && outPath != null;

    Job job = new Job(conf, this.getClass().getSimpleName() + "_" + System.currentTimeMillis());
    job.setJarByClass(this.getClass());
    conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

    JoinSelectStatsUtil.initTabToSeqFileJob(job, inTable, outPath, auths);
    job.setMapperClass(CardinalityMapper.class);

    job.setNumReduceTasks(0);

    job.waitForCompletion(true);

    return job.isSuccessful() ? 0 : 1;

  }

}
