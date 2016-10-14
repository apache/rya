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

package org.apache.rya.prospector.mr

import org.apache.rya.prospector.utils.ProspectorUtils
import org.apache.accumulo.core.data.Mutation
import org.apache.accumulo.core.data.Value
import org.apache.accumulo.core.security.ColumnVisibility
import org.apache.hadoop.conf.Configured
import org.apache.hadoop.util.Tool
import org.apache.hadoop.util.ToolRunner
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job

import org.apache.hadoop.io.LongWritable
import org.apache.commons.lang.time.DateUtils

import org.apache.rya.prospector.domain.IntermediateProspect

import com.google.common.collect.Lists

import static org.apache.rya.prospector.utils.ProspectorConstants.*
import static org.apache.rya.prospector.utils.ProspectorUtils.*

/**
 * Date: 12/3/12
 * Time: 10:57 AM
 */
class Prospector extends Configured implements Tool {

    private static long NOW = System.currentTimeMillis();

    private Date truncatedDate;

    public static void main(String[] args) {
        int res = ToolRunner.run(new Prospector(), args);
        System.exit(res);
    }

    @Override
    int run(String[] args) {
        Configuration conf = getConf();

        truncatedDate = DateUtils.truncate(new Date(NOW), Calendar.MINUTE);

        Path configurationPath = new Path(args[0]);
        conf.addResource(configurationPath);

        def inTable = conf.get("prospector.intable")
        def outTable = conf.get("prospector.outtable")
        def auths_str = conf.get("prospector.auths")
        assert inTable != null
        assert outTable != null 
        assert auths_str != null

        Job job = new Job(getConf(), this.getClass().getSimpleName() + "_" + System.currentTimeMillis());
        job.setJarByClass(this.getClass());

        String[] auths = auths_str.split(",")
        ProspectorUtils.initMRJob(job, inTable, outTable, auths)

        job.getConfiguration().setLong("DATE", NOW);

        def performant = conf.get(PERFORMANT)
        if (Boolean.parseBoolean(performant)) {
            /**
             * Apply some performance tuning
             */
            ProspectorUtils.addMRPerformance(job.configuration)
        }

        job.setMapOutputKeyClass(IntermediateProspect.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setMapperClass(ProspectorMapper.class);
        job.setCombinerClass(ProspectorCombiner.class);
        job.setReducerClass(ProspectorReducer.class);
        job.waitForCompletion(true);

        int success = job.isSuccessful() ? 0 : 1;

        if (success == 0) {
            Mutation m = new Mutation(METADATA)
            m.put(PROSPECT_TIME, getReverseIndexDateTime(truncatedDate), new ColumnVisibility(DEFAULT_VIS), truncatedDate.time, new Value(EMPTY))
            writeMutations(connector(instance(conf), conf), outTable, [m])
        }

        return success
    }
}
