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



import static org.apache.rya.joinselect.mr.utils.JoinSelectConstants.INPUTPATH;
import static org.apache.rya.joinselect.mr.utils.JoinSelectConstants.INSTANCE;
import static org.apache.rya.joinselect.mr.utils.JoinSelectConstants.OUTPUTPATH;
import static org.apache.rya.joinselect.mr.utils.JoinSelectConstants.PASSWORD;
import static org.apache.rya.joinselect.mr.utils.JoinSelectConstants.PROSPECTS_OUTPUTPATH;
import static org.apache.rya.joinselect.mr.utils.JoinSelectConstants.PROSPECTS_TABLE;
import static org.apache.rya.joinselect.mr.utils.JoinSelectConstants.SELECTIVITY_TABLE;
import static org.apache.rya.joinselect.mr.utils.JoinSelectConstants.SPO_OUTPUTPATH;
import static org.apache.rya.joinselect.mr.utils.JoinSelectConstants.SPO_TABLE;
import static org.apache.rya.joinselect.mr.utils.JoinSelectConstants.USERNAME;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.resolver.RyaTripleContext;
import org.apache.rya.api.resolver.triple.TripleRow;
import org.apache.rya.joinselect.mr.JoinSelectAggregate.JoinReducer;
import org.apache.rya.joinselect.mr.JoinSelectAggregate.JoinSelectAggregateMapper;
import org.apache.rya.joinselect.mr.JoinSelectAggregate.JoinSelectGroupComparator;
import org.apache.rya.joinselect.mr.JoinSelectAggregate.JoinSelectPartitioner;
import org.apache.rya.joinselect.mr.JoinSelectAggregate.JoinSelectSortComparator;
import org.apache.rya.joinselect.mr.JoinSelectProspectOutput.CardinalityMapper;
import org.apache.rya.joinselect.mr.JoinSelectSpoTableOutput.JoinSelectMapper;
import org.apache.rya.joinselect.mr.JoinSelectStatisticsSum.CardinalityIdentityCombiner;
import org.apache.rya.joinselect.mr.JoinSelectStatisticsSum.CardinalityIdentityMapper;
import org.apache.rya.joinselect.mr.JoinSelectStatisticsSum.CardinalityIdentityReducer;
import org.apache.rya.joinselect.mr.utils.CardList;
import org.apache.rya.joinselect.mr.utils.CompositeType;
import org.apache.rya.joinselect.mr.utils.JoinSelectStatsUtil;
import org.apache.rya.joinselect.mr.utils.TripleCard;
import org.apache.rya.joinselect.mr.utils.TripleEntry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JoinSelectStatisticsTest {
    
    private static final String PREFIX = JoinSelectStatisticsTest.class.getSimpleName();
  
    private static final String DELIM = "\u0000";
    private static final String uri = "uri:";
    private List<String> cardList = Arrays.asList("subject", "predicate", "object");
    private List<String> aggCardList = Arrays.asList("subjectobject", "subjectpredicate", "subjectsubject", "predicateobject", "predicatepredicate", "predicatesubject");
    private static File SPOOUT;
    private static File PROSPECTSOUT;
    private static File tempDir;
    private Connector c;
    private RyaTripleContext ryaContext;
    private static final String INSTANCE_NAME = "mapreduce_instance";
    
    private static class JoinSelectTester1 extends Configured implements Tool {

       
        
        @Override
        public int run(String[] args) throws Exception {

            Configuration conf = getConf();
            
            String inTable = conf.get(SPO_TABLE);
            String outPath = conf.get(SPO_OUTPUTPATH);
           
            
            assert inTable != null && outPath != null;

            Job job = new Job(conf, this.getClass().getSimpleName() + "_" + System.currentTimeMillis());
            job.setJarByClass(this.getClass());
            conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);
            
            initTabToSeqFileJob(job, inTable, outPath);
            job.setMapperClass(JoinSelectMapper.class);
            job.setNumReduceTasks(0);
            job.waitForCompletion(true);
           
            return job.isSuccessful() ? 0 : 1;
        }
    }

    private static class JoinSelectTester2 extends Configured implements Tool {

        
        
        @Override
        public int run(String[] args) throws Exception {

            Configuration conf = getConf();
            
            String inTable = conf.get(PROSPECTS_TABLE);
            System.out.println("Table is " + inTable);
            String outPath = conf.get(PROSPECTS_OUTPUTPATH);
           
            
            assert inTable != null && outPath != null;

            Job job = new Job(conf, this.getClass().getSimpleName() + "_" + System.currentTimeMillis());
            job.setJarByClass(this.getClass());
            conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);
            
            initTabToSeqFileJob(job, inTable, outPath);
            job.setMapperClass(CardinalityMapper.class);
            job.setNumReduceTasks(0);
            job.waitForCompletion(true);
           
            return job.isSuccessful() ? 0 : 1;          
        }
    }
    
    
 private static class JoinSelectTester4 extends Configured implements Tool {

        
        
        @Override
        public int run(String[] args) throws Exception {

            Configuration conf = getConf();
            String outpath = conf.get(OUTPUTPATH);
    
            Job job = new Job(conf, this.getClass().getSimpleName() + "_" + System.currentTimeMillis());
            job.setJarByClass(this.getClass());
            conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);
            
            MultipleInputs.addInputPath(job, new Path(PROSPECTSOUT.getAbsolutePath()), 
                    SequenceFileInputFormat.class, JoinSelectAggregateMapper.class);
            MultipleInputs.addInputPath(job,new Path(SPOOUT.getAbsolutePath()) , 
                    SequenceFileInputFormat.class, JoinSelectAggregateMapper.class);
            job.setMapOutputKeyClass(CompositeType.class);
            job.setMapOutputValueClass(TripleCard.class);

            tempDir = new File(File.createTempFile(outpath, "txt").getParentFile(), System.currentTimeMillis() + "");
            SequenceFileOutputFormat.setOutputPath(job, new Path(tempDir.getAbsolutePath()));
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            job.setOutputKeyClass(TripleEntry.class);
            job.setOutputValueClass(CardList.class);


            job.setSortComparatorClass(JoinSelectSortComparator.class);
            job.setGroupingComparatorClass(JoinSelectGroupComparator.class);
            job.setPartitionerClass(JoinSelectPartitioner.class);
            job.setReducerClass(JoinReducer.class);
            job.setNumReduceTasks(32);
            job.waitForCompletion(true);
            
            return job.isSuccessful() ? 0 : 1;          
        }
     }
    
    
    
    private static class JoinSelectTester3 extends Configured implements Tool {

        
        
        @Override
        public int run(String[] args) throws Exception {
            
            Configuration conf = getConfig();
            
            String outTable = conf.get(SELECTIVITY_TABLE);
            String inPath = conf.get(INPUTPATH);
            
             
            assert outTable != null && inPath != null;

            Job job = new Job(getConf(), this.getClass().getSimpleName() + "_" + System.currentTimeMillis());
            job.setJarByClass(this.getClass());
            initSumMRJob(job, inPath, outTable);

            job.setMapperClass(CardinalityIdentityMapper.class);
            job.setCombinerClass(CardinalityIdentityCombiner.class);
            job.setReducerClass(CardinalityIdentityReducer.class);
            job.setNumReduceTasks(32);
            job.waitForCompletion(true);
            
           return job.isSuccessful() ? 0 : 1;
           
        }
        
       

    }
    
   
    
    
    
    
    
    public class JoinSelectTestDriver extends Configured implements Tool {
        
        Configuration conf = getConfig();

        @Override
        public int run(String[] args) throws Exception {
            
            int res0 = ToolRunner.run(conf, new JoinSelectTester1(), args);
            int res1 = 1;
            int res2 = 1;
            int res3 = 1;
            
            
           
            if(res0 == 0) {
                res1 = ToolRunner.run(conf, new JoinSelectTester2(), args);
            }
            if(res1 == 0) {
                res2 = ToolRunner.run(conf, new JoinSelectTester4(), args);
            }
            if(res2 == 0) {
                res3 = ToolRunner.run(conf, new JoinSelectTester3(), args);
            }
            
            return res3;
        }

    }
    
    
    
    
    private static Configuration getConfig() {
        
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "file:///");
        conf.set("mapreduce.framework.name", "local");
        conf.set("spo.table", "rya_spo");
        conf.set("prospects.table", "rya_prospects");
        conf.set("selectivity.table", "rya_selectivity");
        conf.set("auths", "");
        conf.set("instance",INSTANCE_NAME);
        conf.set("username","root");
        conf.set("password", "");
        conf.set("inputpath","temp");
        conf.set("outputpath","temp");
        conf.set("prospects.outputpath","prospects");
        conf.set("spo.outputpath", "spo");
        
        
        return conf;
        
    }
    
    
   
    
    
    
    
    
    
    
    public static void initTabToSeqFileJob(Job job, String intable, String outpath) throws AccumuloSecurityException, IOException {
        
        Configuration conf = job.getConfiguration();
       
        String username = conf.get(USERNAME);
        System.out.println("Username is " + username);
        String password = conf.get(PASSWORD);
        String instance = conf.get(INSTANCE);
        System.out.println("Instance is " + instance);
        
       
        AccumuloInputFormat.setMockInstance(job, instance);
        AccumuloInputFormat.setConnectorInfo(job, username, new PasswordToken(password));
        AccumuloInputFormat.setInputTableName(job, intable);
        
        job.setInputFormatClass(AccumuloInputFormat.class);
        job.setMapOutputKeyClass(CompositeType.class);
        job.setMapOutputValueClass(TripleCard.class);

        System.out.println("Outpath is " + outpath);
        
        // OUTPUT
        if(outpath.equals("spo")) {
            SPOOUT = new File(File.createTempFile(outpath, "txt").getParentFile(), System.currentTimeMillis() + "spo");
            SequenceFileOutputFormat.setOutputPath(job, new Path(SPOOUT.getAbsolutePath()));
        } else {
            PROSPECTSOUT = new File(File.createTempFile(outpath, "txt").getParentFile(), System.currentTimeMillis() + "prospects");
            SequenceFileOutputFormat.setOutputPath(job, new Path(PROSPECTSOUT.getAbsolutePath()));
        }
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(CompositeType.class);
        job.setOutputValueClass(TripleCard.class);
    
     }
    
    public static void initSumMRJob(Job job, String inputPath, String outtable) throws AccumuloSecurityException, IOException {
      
        Configuration conf = job.getConfiguration();
        
        String username = conf.get(USERNAME);
        String password = conf.get(PASSWORD);
        String instance = conf.get(INSTANCE);
       
        

        AccumuloOutputFormat.setConnectorInfo(job, username, new PasswordToken(password));
        AccumuloOutputFormat.setMockInstance(job, instance);
        AccumuloOutputFormat.setDefaultTableName(job, outtable);

        
        SequenceFileInputFormat.addInputPath(job, new Path(tempDir.getAbsolutePath()));
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapOutputKeyClass(TripleEntry.class);
        job.setMapOutputValueClass(CardList.class);

       
        job.setOutputFormatClass(AccumuloOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Mutation.class);
         
      
    }
    
    
    
   
    
    @Before
    public void init() throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
      
        MockInstance mockInstance = new MockInstance(INSTANCE_NAME);
        c = mockInstance.getConnector("root", new PasswordToken(""));
        
        if (c.tableOperations().exists("rya_prospects")) {
            c.tableOperations().delete("rya_prospects");
        } 
        if (c.tableOperations().exists("rya_selectivity")) {
            c.tableOperations().delete("rya_selectivity");
        }
        if (c.tableOperations().exists("rya_spo")) {
            c.tableOperations().delete("rya_spo");
        } 
        
        
        c.tableOperations().create("rya_spo");
        c.tableOperations().create("rya_prospects");
        c.tableOperations().create("rya_selectivity");
        ryaContext = RyaTripleContext.getInstance(new AccumuloRdfConfiguration(getConfig()));
    }
    
    
    
    
    
    
    
    

    @Test
    public void testMap1() throws Exception {
        init();
        
        System.out.println("*****************************Test1**************************** ");
        
        BatchWriter bw_table1 = c.createBatchWriter("rya_spo", new BatchWriterConfig());
        for (int i = 1; i < 3; i++) {

            RyaStatement rs = new RyaStatement(new RyaURI(uri + i), new RyaURI(uri + 5), new RyaType(uri + (i + 2)));
            Map<TABLE_LAYOUT, TripleRow> tripleRowMap = ryaContext.serializeTriple(rs);
            TripleRow tripleRow = tripleRowMap.get(TABLE_LAYOUT.SPO);
            Mutation m = JoinSelectStatsUtil.createMutation(tripleRow);
            bw_table1.addMutation(m);
            

        }
        bw_table1.close();

        BatchWriter bw_table2 = c.createBatchWriter("rya_prospects", new BatchWriterConfig());
        for (int i = 1; i < 6; i++) {

            int j = 1;
            
            for (String s : cardList) {
                Mutation m = new Mutation(new Text(s + DELIM + uri + i + DELIM + i));
                m.put(new Text(), new Text(), new Value(new IntWritable(i + j).toString().getBytes()));
                bw_table2.addMutation(m);
                j++;
            }

        }
        bw_table2.close();
        
        
        
        

        Assert.assertEquals(0, ToolRunner.run(new JoinSelectTestDriver(), new String[]{""}));     
        Scanner scan = c.createScanner("rya_selectivity", new Authorizations());
        scan.setRange(new Range());

        for (Map.Entry<Key, Value> entry : scan) {
            System.out.println("Key row string is " + entry.getKey().getRow().toString());
            System.out.println("Join type is " + entry.getKey().getColumnFamily().toString());
            System.out.println("Value is " + entry.getKey().getColumnQualifier().toString());
        }

        Scanner scan1 = c.createScanner("rya_selectivity" , new Authorizations());
        scan1.setRange(Range.prefix("predicate" +DELIM + uri + 5));
        int i = 5;
        
        for (Map.Entry<Key, Value> entry : scan1) {
            
            int val1 = 5 + 2*i;
            int val2 = 5 + 2*(i-1);
            int val = Integer.parseInt(entry.getKey().getColumnQualifier().toString());
            
            if(i < 3) {
                Assert.assertTrue( val == val1);
            }
            if(i >= 3  && i < 6) {
                Assert.assertTrue(val == val2);
            }
            i--;
        }
        Assert.assertTrue(i == -1);
        
        
        
        Scanner scan2 = c.createScanner("rya_selectivity" , new Authorizations());
        scan2.setRange(Range.prefix("object" +DELIM + uri + 3));
        int j = 5;
        
        for (Map.Entry<Key, Value> entry : scan2) {
            
            int val1 = 5 + (j-2);
            int val2 = 2+j;
            int val = Integer.parseInt(entry.getKey().getColumnQualifier().toString());
            
            if(j < 3) {
                Assert.assertTrue( val == val2);
            }
            if(j >= 3  && j < 6) {
                Assert.assertTrue(val == val1);
            }
            j--;
        }
        Assert.assertTrue(j == -1);
        
        
        
        
        Scanner scan3 = c.createScanner("rya_selectivity", new Authorizations());
        scan3.setRange(Range.prefix("objectsubject" + DELIM + uri + 3 +DELIM +uri +1 ));
        int k = 8;

        for (Map.Entry<Key, Value> entry : scan3) {

            int val = Integer.parseInt(entry.getKey().getColumnQualifier().toString());

            Assert.assertTrue(val == k);
            k--;
        }
        Assert.assertTrue(k == 5);
        
        
        
       
        
        

    }


    
    
    
    @Test
    public void testMap2() throws Exception {
        
        System.out.println("*********************Test2******************* ");

        init();

        BatchWriter bw_table1 = c.createBatchWriter("rya_spo", new BatchWriterConfig());
        for (int i = 1; i < 4; i++) {

            RyaStatement rs = new RyaStatement(new RyaURI(uri + 1), new RyaURI(uri + 2), new RyaType(uri + i));
            Map<TABLE_LAYOUT, TripleRow> tripleRowMap = ryaContext.serializeTriple(rs);
            TripleRow tripleRow = tripleRowMap.get(TABLE_LAYOUT.SPO);
            Mutation m = JoinSelectStatsUtil.createMutation(tripleRow);
            bw_table1.addMutation(m);

        }
        bw_table1.close();

        BatchWriter bw_table2 = c.createBatchWriter("rya_prospects", new BatchWriterConfig());
        for (int i = 1; i < 4; i++) {

            for (String s : cardList) {
                Mutation m = new Mutation(new Text(s + DELIM + uri + i + DELIM + i));
                m.put(new Text(), new Text(), new Value(new IntWritable(i + 2).toString().getBytes()));
                bw_table2.addMutation(m);
            }

        }
        bw_table2.close();

        Assert.assertEquals(0, ToolRunner.run(new JoinSelectTestDriver(), new String[]{""}));
        Scanner scan1 = c.createScanner("rya_selectivity" , new Authorizations());
        scan1.setRange(Range.prefix("subject" +DELIM + uri + 1));
        int i = 0;
        
        for (Map.Entry<Key, Value> entry : scan1) {
            
            Assert.assertTrue(entry.getKey().getColumnQualifier().toString().equals("12")); 
            i++;
        }
        Assert.assertTrue(i == 6);
        
        Scanner scan2 = c.createScanner("rya_selectivity" , new Authorizations());
        scan2.setRange(Range.prefix("predicate" +DELIM + uri + 2));
        int j = 0;
        
        for (Map.Entry<Key, Value> entry : scan2) {
            
            if(j < 3) {
                Assert.assertTrue(entry.getKey().getColumnQualifier().toString().equals("12"));
            }
            if(j > 3  && j < 6) {
                Assert.assertTrue(entry.getKey().getColumnQualifier().toString().equals("9"));
            }
            j++;
        }
        Assert.assertTrue(j == 6);
        
        Scanner scan3 = c.createScanner("rya_selectivity" , new Authorizations());
        scan3.setRange(Range.prefix("predicateobject" +DELIM + uri + 2 +DELIM + uri + 2));
        int k = 0;
        
        for (Map.Entry<Key, Value> entry : scan3) {
            Assert.assertTrue(entry.getKey().getColumnQualifier().toString().equals("3")); 
            k++;
        }
        Assert.assertTrue(k == 3);
        

    }

    
    
    
    @Test
    public void testMap3() throws Exception {
        init();
        
        System.out.println("*************************Test3**************************** ");
        
        BatchWriter bw_table1 = c.createBatchWriter("rya_spo", new BatchWriterConfig());
        for (int i = 1; i < 3; i++) {
            for (int j = 1; j < 3; j++) {
                for (int k = 1; k < 3; k++) {

                    RyaStatement rs = new RyaStatement(new RyaURI(uri + i), new RyaURI(uri + (j)), new RyaType(uri + k));
                    Map<TABLE_LAYOUT, TripleRow> tripleRowMap = ryaContext.serializeTriple(rs);
                    TripleRow tripleRow = tripleRowMap.get(TABLE_LAYOUT.SPO);
                    Mutation m = JoinSelectStatsUtil.createMutation(tripleRow);
                    bw_table1.addMutation(m);

                }
            }

        }
        bw_table1.close();

        BatchWriter bw_table2 = c.createBatchWriter("rya_prospects", new BatchWriterConfig());
        for (int i = 1; i < 3; i++) {

            int k = 1;
            for (String s : cardList) {
                Mutation m = new Mutation(new Text(s + DELIM + uri + i + DELIM + i));
                m.put(new Text(), new Text(), new Value(new IntWritable(i + k).toString().getBytes()));
                bw_table2.addMutation(m);
                k++;
            }

            for (int j = 1; j < 3; j++) {
                k = 1;
                for (String s : aggCardList) {
                    Mutation m = new Mutation(new Text(s + DELIM + uri + i + DELIM + uri + j + DELIM + i));
                    m.put(new Text(), new Text(), new Value(new IntWritable(i + k +j).toString().getBytes()));
                    bw_table2.addMutation(m);
                    k++;
                }
            }

        }
        bw_table2.close();
        
        
        
        

        Assert.assertEquals(0, ToolRunner.run(new JoinSelectTestDriver(), new String[]{""}));
        Scanner scan = c.createScanner("rya_selectivity", new Authorizations());
        scan.setRange(new Range());

        for (Map.Entry<Key, Value> entry : scan) {
            System.out.println("Key row string is " + entry.getKey().getRow().toString());
            System.out.println("Join type is " + entry.getKey().getColumnFamily().toString());
            System.out.println("Value is " + entry.getKey().getColumnQualifier().toString());
        }

        
        
        Scanner scan1 = c.createScanner("rya_selectivity" , new Authorizations());
        scan1.setRange(Range.prefix("subject" +DELIM + uri + 1));
        int i = 0;
        
        for (Map.Entry<Key, Value> entry : scan1) {
            
            Key key = entry.getKey();
            String s = key.getColumnFamily().toString();
            int val = Integer.parseInt(key.getColumnQualifier().toString());
            
            if(s.equals("predicatepredicate")) {
                Assert.assertTrue(val == 14);
            } 
            if(s.equals("objectobject")) {
                Assert.assertTrue(val == 18);
            } 
            if(s.equals("predicateobjectpredicateobject")) {
                Assert.assertTrue(val == 28);
            } 
            if(s.equals("predicateobjectsubjectpredicate")) {
                Assert.assertTrue(val == 20);
            }
            if(s.equals("predicateobjectobjectsubject")) {
                Assert.assertTrue(val == 16);
            }
            
            i++;
        }
        Assert.assertTrue(i == 12);
        
        
        
       
        
        

    }


    
    
    
    @Test
    public void testMap4() throws Exception {
        init();
        
        System.out.println("*************************Test4**************************** ");
        System.out.println("*************************Test4**************************** ");
        
        BatchWriter bw_table1 = c.createBatchWriter("rya_spo", new BatchWriterConfig());
        for (int i = 1; i < 3; i++) {
            for (int j = 1; j < 3; j++) {
                for (int k = 1; k < 3; k++) {

                    if(j == 1 && k ==2) {
                        break;
                    }
                    
                    RyaStatement rs = new RyaStatement(new RyaURI(uri + i), new RyaURI(uri + (j)), new RyaType(uri + k));
                    Map<TABLE_LAYOUT, TripleRow> tripleRowMap = ryaContext.serializeTriple(rs);
                    TripleRow tripleRow = tripleRowMap.get(TABLE_LAYOUT.SPO);
                    Mutation m = JoinSelectStatsUtil.createMutation(tripleRow);
                    bw_table1.addMutation(m);

                }
            }

        }
        bw_table1.close();

        BatchWriter bw_table2 = c.createBatchWriter("rya_prospects", new BatchWriterConfig());
        for (int i = 1; i < 3; i++) {

            int k = 1;
            for (String s : cardList) {
                Mutation m = new Mutation(new Text(s + DELIM + uri + i + DELIM + i));
                m.put(new Text(), new Text(), new Value(new IntWritable(i + k).toString().getBytes()));
                bw_table2.addMutation(m);
                k++;
            }

            for (int j = 1; j < 3; j++) {
                k = 1;
                for (String s : aggCardList) {
                    Mutation m = new Mutation(new Text(s + DELIM + uri + i + DELIM + uri + j + DELIM + i));
                    m.put(new Text(), new Text(), new Value(new IntWritable(i + k + 2*j).toString().getBytes()));
                    bw_table2.addMutation(m);
                    k++;
                }
            }

        }
        bw_table2.close();
        
        
        
        

        Assert.assertEquals(0, ToolRunner.run(new JoinSelectTestDriver(), new String[]{""}));
        Scanner scan = c.createScanner("rya_selectivity", new Authorizations());
        scan.setRange(new Range());

        for (Map.Entry<Key, Value> entry : scan) {
            System.out.println("Key row string is " + entry.getKey().getRow().toString());
            System.out.println("Join type is " + entry.getKey().getColumnFamily().toString());
            System.out.println("Value is " + entry.getKey().getColumnQualifier().toString());
        }

        
        
        Scanner scan1 = c.createScanner("rya_selectivity" , new Authorizations());
        scan1.setRange(Range.prefix("subject" +DELIM + uri + 1));
        int i = 0;
        
        for (Map.Entry<Key, Value> entry : scan1) {
            
            Key key = entry.getKey();
            String s = key.getColumnFamily().toString();
            int val = Integer.parseInt(key.getColumnQualifier().toString());
            
            if(s.equals("predicatepredicate")) {
                Assert.assertTrue(val == 11);
            } 
            if(s.equals("objectobject")) {
                Assert.assertTrue(val == 13);
            } 
            if(s.equals("predicateobjectobjectpredicate")) {
                Assert.assertTrue(val == 26);
            } 
            if(s.equals("predicateobjectpredicateobject")) {
                Assert.assertTrue(val == 25);
            } 
            if(s.equals("predicateobjectsubjectpredicate")) {
                Assert.assertTrue(val == 19);
            }
            if(s.equals("predicateobjectpredicatesubject")) {
                Assert.assertTrue(val == 20);
            }
            
            i++;
        }
        Assert.assertTrue(i == 12);
        
        
        
        Scanner scan2 = c.createScanner("rya_selectivity" , new Authorizations());
        scan2.setRange(Range.prefix("predicate" +DELIM + uri + 1));
        int j = 0;
        
        for (Map.Entry<Key, Value> entry : scan2) {
            
            Key key = entry.getKey();
            String s = key.getColumnFamily().toString();
            int val = Integer.parseInt(key.getColumnQualifier().toString());
            
            if(s.equals("subjectsubject")) {
                Assert.assertTrue(val == 5);
            } 
            if(s.equals("objectobject")) {
                Assert.assertTrue(val == 8);
            } 
            if(s.equals("objectsubjectsubjectpredicate")) {
                Assert.assertTrue(val == 11);
            } 
            if(s.equals("objectsubjectpredicateobject")) {
                Assert.assertTrue(val == 15);
            } 
            if(s.equals("objectsubjectobjectsubject")) {
                Assert.assertTrue(val == 9);
            }
            if(s.equals("objectsubjectsubjectobject")) {
                Assert.assertTrue(val == 10);
            }
            
            j++;
        }
        Assert.assertTrue(j == 12);
        
        
        
        
       
        
        

    }

    
    
    
    
    
    
    
    
    
    

}   
    
    
    
    
    
    
   
