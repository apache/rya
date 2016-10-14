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
import static org.apache.rya.joinselect.mr.utils.JoinSelectConstants.SPO_OUTPUTPATH;
import static org.apache.rya.joinselect.mr.utils.JoinSelectConstants.SPO_TABLE;

import java.io.IOException;

import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.api.RdfCloudTripleStoreConstants;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.resolver.RyaTripleContext;
import org.apache.rya.api.resolver.triple.TripleRow;
import org.apache.rya.api.resolver.triple.TripleRowResolverException;
import org.apache.rya.joinselect.mr.utils.CompositeType;
import org.apache.rya.joinselect.mr.utils.JoinSelectStatsUtil;
import org.apache.rya.joinselect.mr.utils.TripleCard;
import org.apache.rya.joinselect.mr.utils.TripleEntry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;

public class JoinSelectSpoTableOutput extends Configured implements Tool {

  public static class JoinSelectMapper extends Mapper<Key,Value,CompositeType,TripleCard> {

    private RyaTripleContext ryaContext;
    private static final String DELIM = "\u0000";

    public void map(Key row, Value data, Context context) throws IOException, InterruptedException {
      try {
    	  ryaContext = RyaTripleContext.getInstance(new AccumuloRdfConfiguration(context.getConfiguration()));
        RyaStatement ryaStatement = ryaContext.deserializeTriple(RdfCloudTripleStoreConstants.TABLE_LAYOUT.SPO, new TripleRow(row.getRow().getBytes(), row
            .getColumnFamily().getBytes(), row.getColumnQualifier().getBytes(), row.getTimestamp(), row.getColumnVisibility().getBytes(), data.get()));

        Text s = new Text(ryaStatement.getSubject().getData());
        Text p = new Text(ryaStatement.getPredicate().getData());
        Text o = new Text(ryaStatement.getObject().getData());
        Text sp = new Text(ryaStatement.getSubject().getData() + DELIM + ryaStatement.getPredicate().getData());
        Text po = new Text(ryaStatement.getPredicate().getData() + DELIM + ryaStatement.getObject().getData());
        Text so = new Text(ryaStatement.getSubject().getData() + DELIM + ryaStatement.getObject().getData());
        Text ps = new Text(ryaStatement.getPredicate().getData() + DELIM + ryaStatement.getSubject().getData());
        Text op = new Text(ryaStatement.getObject().getData() + DELIM + ryaStatement.getPredicate().getData());
        Text os = new Text(ryaStatement.getObject().getData() + DELIM + ryaStatement.getSubject().getData());

        TripleEntry t1 = new TripleEntry(s, p, new Text("subject"), new Text("predicate"), new Text("object"));
        TripleEntry t2 = new TripleEntry(p, o, new Text("predicate"), new Text("object"), new Text("subject"));
        TripleEntry t3 = new TripleEntry(o, s, new Text("object"), new Text("subject"), new Text("predicate"));
        TripleEntry t4 = new TripleEntry(s, new Text(""), new Text("subject"), new Text(""), new Text("predicateobject"));
        TripleEntry t5 = new TripleEntry(p, new Text(""), new Text("predicate"), new Text(""), new Text("objectsubject"));
        TripleEntry t6 = new TripleEntry(o, new Text(""), new Text("object"), new Text(""), new Text("subjectpredicate"));
        TripleEntry t7 = new TripleEntry(s, new Text(""), new Text("subject"), new Text(""), new Text("objectpredicate"));
        TripleEntry t8 = new TripleEntry(p, new Text(""), new Text("predicate"), new Text(""), new Text("subjectobject"));
        TripleEntry t9 = new TripleEntry(o, new Text(""), new Text("object"), new Text(""), new Text("predicatesubject"));

        context.write(new CompositeType(o, new IntWritable(2)), new TripleCard(t1));
        context.write(new CompositeType(s, new IntWritable(2)), new TripleCard(t2));
        context.write(new CompositeType(p, new IntWritable(2)), new TripleCard(t3));
        context.write(new CompositeType(po, new IntWritable(2)), new TripleCard(t4));
        context.write(new CompositeType(so, new IntWritable(2)), new TripleCard(t5));
        context.write(new CompositeType(sp, new IntWritable(2)), new TripleCard(t6));
        context.write(new CompositeType(op, new IntWritable(2)), new TripleCard(t7));
        context.write(new CompositeType(os, new IntWritable(2)), new TripleCard(t8));
        context.write(new CompositeType(ps, new IntWritable(2)), new TripleCard(t9));

      } catch (TripleRowResolverException e) {
        e.printStackTrace();
      }

    }

  }

  @Override
  public int run(String[] args) throws Exception {

    Configuration conf = getConf();
    String inTable = conf.get(SPO_TABLE);
    String auths = conf.get(AUTHS);
    String outPath = conf.get(SPO_OUTPUTPATH);

    assert inTable != null && outPath != null;

    Job job = new Job(conf, this.getClass().getSimpleName() + "_" + System.currentTimeMillis());
    job.setJarByClass(this.getClass());
    conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

    JoinSelectStatsUtil.initTabToSeqFileJob(job, inTable, outPath, auths);
    job.setMapperClass(JoinSelectMapper.class);
    job.setNumReduceTasks(0);
    job.waitForCompletion(true);

    return job.isSuccessful() ? 0 : 1;

  }

}
