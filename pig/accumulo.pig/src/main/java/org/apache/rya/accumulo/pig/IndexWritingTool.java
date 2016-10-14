package org.apache.rya.accumulo.pig;

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



import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.ProjectionElem;
import org.openrdf.query.algebra.ProjectionElemList;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.parser.sparql.SPARQLParser;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class IndexWritingTool extends Configured implements Tool {
    
    private static final String sparql_key = "SPARQL.VALUE";
    private static String cardCounter = "count";
    
    
    public static void main(String[] args) throws Exception {
        
      ToolRunner.run(new Configuration(), new IndexWritingTool(), args);
       
    }

    @Override
    public int run(final String[] args) throws Exception {
        Preconditions.checkArgument(args.length == 7, "java " + IndexWritingTool.class.getCanonicalName()
                + " hdfsSaveLocation sparqlFile cbinstance cbzk cbuser cbpassword rdfTablePrefix.");

        final String inputDir = args[0];
        final String sparqlFile = args[1];
        final String instStr = args[2];
        final String zooStr = args[3];
        final String userStr = args[4];
        final String passStr = args[5];
        final String tablePrefix = args[6];

        String sparql = FileUtils.readFileToString(new File(sparqlFile));

        Job job = new Job(getConf(), "Write HDFS Index to Accumulo");
        job.setJarByClass(this.getClass());

        Configuration jobConf = job.getConfiguration();
        jobConf.setBoolean("mapred.map.tasks.speculative.execution", false);
        setVarOrders(sparql, jobConf);

        TextInputFormat.setInputPaths(job, inputDir);
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Mutation.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Mutation.class);

        job.setNumReduceTasks(0);

        String tableName;
        if (zooStr.equals("mock")) {
            tableName = tablePrefix;
        } else {
            tableName = tablePrefix + "INDEX_" + UUID.randomUUID().toString().replace("-", "").toUpperCase();
        }
        setAccumuloOutput(instStr, zooStr, userStr, passStr, job, tableName);

        jobConf.set(sparql_key, sparql);
        
        int complete = job.waitForCompletion(true) ? 0 : -1;

        if (complete == 0) {
            
            String[] varOrders = jobConf.getStrings("varOrders");
            String orders = Joiner.on("\u0000").join(varOrders);
            Instance inst;
            
            if (zooStr.equals("mock")) {
                inst = new MockInstance(instStr);
            } else {
                inst = new ZooKeeperInstance(instStr, zooStr);
            }
           
            Connector conn = inst.getConnector(userStr, passStr.getBytes());
            BatchWriter bw = conn.createBatchWriter(tableName, 10, 5000, 1);

            Counters counters = job.getCounters();
            Counter c1 = counters.findCounter(cardCounter, cardCounter);
            
            Mutation m = new Mutation("~SPARQL");
            Value v = new Value(sparql.getBytes());
            m.put(new Text("" + c1.getValue()), new Text(orders), v);
            bw.addMutation(m);

            bw.close();

            return complete;
        } else {
            return complete;
        }


    }
    
    
    public void setVarOrders(String s, Configuration conf) throws MalformedQueryException {

        SPARQLParser parser = new SPARQLParser();
        TupleExpr query = parser.parseQuery(s, null).getTupleExpr();

        List<String> projList = Lists.newArrayList(((Projection) query).getProjectionElemList().getTargetNames());
        String projElems = Joiner.on(";").join(projList);
        conf.set("projElems", projElems);

        Pattern splitPattern1 = Pattern.compile("\n");
        Pattern splitPattern2 = Pattern.compile(",");
        String[] lines = splitPattern1.split(s);

        List<String> varOrders = Lists.newArrayList();
        List<String> varOrderPos = Lists.newArrayList();

        int orderNum = 0;
        int projSizeSq = projList.size()*projList.size();
        
        for (String t : lines) {


            if(orderNum > projSizeSq){
                break;
            }
            
            String[] order = null;
            if (t.startsWith("#prefix")) {
                t = t.substring(7).trim();
                order = splitPattern2.split(t, projList.size());
            }

            
            String tempVarOrder = "";
            String tempVarOrderPos = "";

            if (order != null) {
                for (String u : order) {
                    if (tempVarOrder.length() == 0) {
                        tempVarOrder = u.trim();
                    } else {
                        tempVarOrder = tempVarOrder + ";" + u.trim();
                    }
                    int pos = projList.indexOf(u.trim());
                    if (pos < 0) {
                        throw new IllegalArgumentException("Invalid variable order!");
                    } else {
                        if (tempVarOrderPos.length() == 0) {
                            tempVarOrderPos = tempVarOrderPos + pos;
                        } else {
                            tempVarOrderPos = tempVarOrderPos + ";" + pos;
                        }
                    }
                }

                varOrders.add(tempVarOrder);
                varOrderPos.add(tempVarOrderPos);
            }
            
            if(tempVarOrder.length() > 0) {
                orderNum++;
            }

        }
        
        if(orderNum ==  0) {
            varOrders.add(projElems);
            String tempVarPos = "";
            
            for(int i = 0; i < projList.size(); i++) {
                if(i == 0) {
                    tempVarPos = Integer.toString(0);
                } else {
                    tempVarPos = tempVarPos + ";" + i;
                }
            }
            varOrderPos.add(tempVarPos);
            
        }
        
        String[] vOrders = varOrders.toArray(new String[varOrders.size()]);
        String[] vOrderPos = varOrderPos.toArray(new String[varOrderPos.size()]);
        
        
        
        conf.setStrings("varOrders", vOrders);
        conf.setStrings("varOrderPos", vOrderPos);

    }
    

    private static void setAccumuloOutput(String instStr, String zooStr, String userStr, String passStr, Job job, String tableName)
            throws AccumuloSecurityException {

        AuthenticationToken token = new PasswordToken(passStr);
        AccumuloOutputFormat.setConnectorInfo(job, userStr, token);
        AccumuloOutputFormat.setDefaultTableName(job, tableName);
        AccumuloOutputFormat.setCreateTables(job, true);
        //TODO best way to do this?
        
        if (zooStr.equals("mock")) {
            AccumuloOutputFormat.setMockInstance(job, instStr);
        } else {
            AccumuloOutputFormat.setZooKeeperInstance(job, instStr, zooStr);
        }

        job.setOutputFormatClass(AccumuloOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Mutation.class);
    }

    public static class MyMapper extends Mapper<LongWritable, Text, Text, Mutation> {
        
        private static final Logger logger = Logger.getLogger(MyMapper.class);
        final static Text EMPTY_TEXT = new Text();
        final static Value EMPTY_VALUE = new Value(new byte[] {});
        private String[] varOrderPos = null;
        private String[] projElem = null;
        private Pattern splitPattern = null;
        private List<List<Integer>> varPositions = Lists.newArrayList();
        
        

        @Override
        protected void setup(Mapper<LongWritable, Text, Text, Mutation>.Context context) throws IOException,
                InterruptedException {
           
            Configuration conf = context.getConfiguration();
            
            varOrderPos = conf.getStrings("varOrderPos");
            splitPattern = Pattern.compile("\t");
            
            for (String s : varOrderPos) {
                String[] pos = s.split(";");
                List<Integer> intPos = Lists.newArrayList();
                int i = 0;
                for(String t: pos) {
                    i = Integer.parseInt(t);
                    intPos.add(i);
                }
                
                varPositions.add(intPos);
                
            }
            
            projElem = conf.get("projElems").split(";");
            
            super.setup(context);
        }






        @Override
        public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {

            String[] result = splitPattern.split(value.toString());

           
            for (List<Integer> list : varPositions) {

                String values = "";
                String vars = "";

                for (Integer i : list) {

                    if (values.length() == 0) {
                        values = result[i];
                        vars = projElem[i];
                    } else {
                        values = values + "\u0000" + result[i];
                        vars = vars + "\u0000" + projElem[i];
                    }

                }
                Mutation m = new Mutation(new Text(values));
                m.put(new Text(vars), EMPTY_TEXT, EMPTY_VALUE);
                output.write(EMPTY_TEXT, m);

            }
            output.getCounter(cardCounter, cardCounter).increment(1);

        }
    }

}
