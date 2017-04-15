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
package org.apache.rya.prospector.mr;

import static org.apache.rya.prospector.utils.ProspectorConstants.DEFAULT_VIS;
import static org.apache.rya.prospector.utils.ProspectorConstants.EMPTY;
import static org.apache.rya.prospector.utils.ProspectorConstants.METADATA;
import static org.apache.rya.prospector.utils.ProspectorConstants.PERFORMANT;
import static org.apache.rya.prospector.utils.ProspectorConstants.PROSPECT_TIME;
import static org.apache.rya.prospector.utils.ProspectorUtils.connector;
import static org.apache.rya.prospector.utils.ProspectorUtils.getReverseIndexDateTime;
import static org.apache.rya.prospector.utils.ProspectorUtils.instance;
import static org.apache.rya.prospector.utils.ProspectorUtils.writeMutations;

import java.util.Calendar;
import java.util.Collections;
import java.util.Date;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.rya.prospector.domain.IntermediateProspect;
import org.apache.rya.prospector.utils.ProspectorUtils;

/**
 * Configures and runs the Hadoop Map Reduce job that executes the Prospector's work.
 */
public class Prospector extends Configured implements Tool {

    private static long NOW = System.currentTimeMillis();

    private Date truncatedDate;

    public static void main(String[] args) throws Exception {
        final int res = ToolRunner.run(new Prospector(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        final Configuration conf = getConf();

        truncatedDate = DateUtils.truncate(new Date(NOW), Calendar.MINUTE);

        final Path configurationPath = new Path(args[0]);
        conf.addResource(configurationPath);

        final String inTable = conf.get("prospector.intable");
        final String outTable = conf.get("prospector.outtable");
        final String auths_str = conf.get("prospector.auths");
        assert inTable != null;
        assert outTable != null;
        assert auths_str != null;

        final Job job = new Job(getConf(), this.getClass().getSimpleName() + "_" + System.currentTimeMillis());
        job.setJarByClass(this.getClass());

        final String[] auths = auths_str.split(",");
        ProspectorUtils.initMRJob(job, inTable, outTable, auths);

        job.getConfiguration().setLong("DATE", NOW);

        final String performant = conf.get(PERFORMANT);
        if (Boolean.parseBoolean(performant)) {
            /**
             * Apply some performance tuning
             */
            ProspectorUtils.addMRPerformance(job.getConfiguration());
        }

        job.setMapOutputKeyClass(IntermediateProspect.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setMapperClass(ProspectorMapper.class);
        job.setCombinerClass(ProspectorCombiner.class);
        job.setReducerClass(ProspectorReducer.class);
        job.waitForCompletion(true);

        final int success = job.isSuccessful() ? 0 : 1;

        if (success == 0) {
            final Mutation m = new Mutation(METADATA);
            m.put(PROSPECT_TIME, getReverseIndexDateTime(truncatedDate), new ColumnVisibility(DEFAULT_VIS), truncatedDate.getTime(), new Value(EMPTY));
            writeMutations(connector(instance(conf), conf), outTable, Collections.singleton(m));
        }

        return success;
    }
}