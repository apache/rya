package org.apache.rya.reasoning.mr;

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

import java.io.IOException;

import org.apache.rya.reasoning.Derivation;
import org.apache.rya.reasoning.Fact;
import org.apache.rya.reasoning.Schema;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.ToolRunner;

/**
 * Collect inferred triples and detected inconsistencies as text.
 */
public class OutputTool extends AbstractReasoningTool {
    @Override
    protected void configureReasoningJob(String[] args) throws Exception {
        MRReasoningUtils.deleteIfExists(job.getConfiguration(), "final");
        configureFileInput(FactMapper.class, InconsistencyMapper.class, false);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        job.setReducerClass(OutputReducer.class);
        configureTextOutput("final");
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new OutputTool(), args));
    }

    public static class FactMapper extends Mapper<Fact, NullWritable,
            Text, Text> {
        Text k = new Text();
        Text v = new Text();
        private boolean debug = false;
        @Override
        public void setup(Context context) {
            debug = MRReasoningUtils.debug(context.getConfiguration());
        }
        @Override
        public void map(Fact fact, NullWritable nw, Context context)
                throws IOException, InterruptedException {
            k.set(getOutputName(fact, true));
            v.set(fact.toString());
            context.write(k, v);
            if (debug) {
                k.set(MRReasoningUtils.DEBUG_OUT);
                v.set(fact.explain(true));
                context.write(k, v);
            }
        }
    }

    public static class InconsistencyMapper extends Mapper<Derivation,
            NullWritable, Text, Text> {
        Text k = new Text();
        Text v = new Text();
        Schema schema;
        @Override
        public void setup(Context context) {
            schema = MRReasoningUtils.loadSchema(context.getConfiguration());
        }
        @Override
        public void map(Derivation inconsistency, NullWritable nw, Context context)
                throws IOException, InterruptedException {
            k.set(getOutputName(inconsistency));
            v.set("Inconsistency:\n" + inconsistency.explain(true, schema) + "\n");
            context.write(k, v);
        }
    }

    public static class OutputReducer extends Reducer<Text, Text, NullWritable, Text> {
        private MultipleOutputs<NullWritable, Text> mout;
        @Override
        public void setup(Context context) {
            mout = new MultipleOutputs<>(context);
        }
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String out = key.toString();
            for (Text value : values) {
                mout.write(out, NullWritable.get(), value);
            }
        }
    }
}
