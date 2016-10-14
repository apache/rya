package mvm.rya.accumulo.mr.tools;

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

import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.openrdf.rio.RDFFormat;

import mvm.rya.accumulo.mr.AbstractAccumuloMRTool;
import mvm.rya.accumulo.mr.MRUtils;

/**
 * Reads RDF data from one or more file(s) and inserts statements into Rya.
 * <p>
 * Uses {@link mvm.rya.accumulo.mr.RdfFileInputFormat} to read data.
 * <p>
 * Takes one argument: the file or directory to read (from HDFS).
 * <p>
 * Expects configuration:
 * <p>
 * - RDF format, named by parameter "rdf.format"; see {@link RDFFormat}.
 *   Defaults to rdf/xml. If using multiple files, all must be the same format.
 * <p>
 * - Accumulo and Rya configuration parameters as named in {@link MRUtils}
 *   (username, password, instance name, zookeepers, and Rya prefix)
 * <p>
 * - Indexing configuration parameters as named in
 *   {@link mvm.rya.indexing.accumulo.ConfigUtils} (enable or disable freetext,
 *   geo, temporal, and entity indexing, and specify predicates for each
 *   indexer). If not given, no secondary indexing is done.
 */
public class RdfFileInputTool extends AbstractAccumuloMRTool implements Tool {
    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(), new RdfFileInputTool(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        init();
        Job job = Job.getInstance(conf, "Rdf File Input");
        job.setJarByClass(RdfFileInputTool.class);

        String inputPath = conf.get(MRUtils.INPUT_PATH, args[0]);
        setupFileInput(job, inputPath, RDFFormat.RDFXML);
        setupRyaOutput(job);
        job.setNumReduceTasks(0);

        Date startTime = new Date();
        System.out.println("Job started: " + startTime);
        int exitCode = job.waitForCompletion(true) ? 0 : 1;

        if (exitCode == 0) {
            Date end_time = new Date();
            System.out.println("Job ended: " + end_time);
            System.out.println("The job took "
                    + (end_time.getTime() - startTime.getTime()) / 1000
                    + " seconds.");
            long n = job.getCounters()
                    .findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_OUTPUT_RECORDS").getValue();
            System.out.println(n + " statement(s) inserted to Rya.");
        } else {
            System.out.println("Job Failed!!!");
        }
        return exitCode;
    }
}
