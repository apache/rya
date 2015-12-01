package mvm.rya.accumulo.mr.fileinput;

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



import static mvm.rya.accumulo.AccumuloRdfConstants.EMPTY_CV;

import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.accumulo.RyaTableMutationsFactory;
import mvm.rya.accumulo.mr.AbstractAccumuloMRTool;
import mvm.rya.accumulo.mr.utils.MRUtils;
import mvm.rya.api.RdfCloudTripleStoreConstants;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.domain.utils.RyaStatementWritable;
import mvm.rya.api.resolver.RyaTripleContext;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.openrdf.rio.RDFFormat;

/**
 * Do bulk import of rdf files
 * Class RdfFileInputTool
 * Date: May 16, 2011
 * Time: 3:12:16 PM
 */
public class RdfFileInputTool extends AbstractAccumuloMRTool implements Tool {

    private String format = RDFFormat.RDFXML.getName();

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(), new RdfFileInputTool(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public long runJob(String[] args) throws IOException, ClassNotFoundException, InterruptedException, AccumuloSecurityException {
        conf.set(MRUtils.JOB_NAME_PROP, "Rdf File Input");
        //faster
        init();
        format = conf.get(MRUtils.FORMAT_PROP, format);
        conf.set(MRUtils.FORMAT_PROP, format);
        
        String inputPath = conf.get(MRUtils.INPUT_PATH, args[0]);

        Job job = new Job(conf);
        job.setJarByClass(RdfFileInputTool.class);

        // set up cloudbase input
        job.setInputFormatClass(RdfFileInputFormat.class);
        RdfFileInputFormat.addInputPath(job, new Path(inputPath));

        // set input output of the particular job
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(RyaStatementWritable.class);

        setupOutputFormat(job, tablePrefix + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX);

        // set mapper and reducer classes
        job.setMapperClass(StatementToMutationMapper.class);
        job.setNumReduceTasks(0);

        // Submit the job
        Date startTime = new Date();
        System.out.println("Job started: " + startTime);
        int exitCode = job.waitForCompletion(true) ? 0 : 1;

        if (exitCode == 0) {
            Date end_time = new Date();
            System.out.println("Job ended: " + end_time);
            System.out.println("The job took "
                    + (end_time.getTime() - startTime.getTime()) / 1000
                    + " seconds.");
            return job
                    .getCounters()
                    .findCounter("org.apache.hadoop.mapred.Task$Counter",
                            "REDUCE_OUTPUT_RECORDS").getValue();
        } else {
            System.out.println("Job Failed!!!");
        }

        return -1;
    }

    @Override
    public int run(String[] args) throws Exception {
        runJob(args);
        return 0;
    }

    public static class StatementToMutationMapper extends Mapper<LongWritable, RyaStatementWritable, Text, Mutation> {
        protected String tablePrefix;
        protected Text spo_table;
        protected Text po_table;
        protected Text osp_table;
        private byte[] cv = EMPTY_CV.getExpression();
        RyaTableMutationsFactory mut;

        public StatementToMutationMapper() {
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            mut = new RyaTableMutationsFactory(RyaTripleContext.getInstance(new AccumuloRdfConfiguration(conf)));
            tablePrefix = conf.get(MRUtils.TABLE_PREFIX_PROPERTY, RdfCloudTripleStoreConstants.TBL_PRFX_DEF);
            spo_table = new Text(tablePrefix + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX);
            po_table = new Text(tablePrefix + RdfCloudTripleStoreConstants.TBL_PO_SUFFIX);
            osp_table = new Text(tablePrefix + RdfCloudTripleStoreConstants.TBL_OSP_SUFFIX);

            final String cv_s = conf.get(MRUtils.AC_CV_PROP);
            if (cv_s != null)
                cv = cv_s.getBytes();
        }

        @Override
        protected void map(LongWritable key, RyaStatementWritable value, Context context) throws IOException, InterruptedException {
            RyaStatement statement = value.getRyaStatement();
            if (statement.getColumnVisibility() == null) {
                statement.setColumnVisibility(cv);
            }
            Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> mutationMap =
                    mut.serialize(statement);
            Collection<Mutation> spo = mutationMap.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.SPO);
            Collection<Mutation> po = mutationMap.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.PO);
            Collection<Mutation> osp = mutationMap.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.OSP);

            for (Mutation m : spo) {
                context.write(spo_table, m);
            }
            for (Mutation m : po) {
                context.write(po_table, m);
            }
            for (Mutation m : osp) {
                context.write(osp_table, m);
            }
        }

    }
}

