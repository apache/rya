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

import org.apache.rya.accumulo.mr.RyaStatementWritable;
import org.apache.rya.reasoning.Fact;
import org.apache.rya.reasoning.Schema;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * Collects the schema information stored in the table and outputs the schema
 * (TBox) to a file.
 */
public class SchemaFilter extends AbstractReasoningTool {
    @Override
    protected void configureReasoningJob(String[] args) throws Exception {
        configureMultipleInput(SchemaTableMapper.class, SchemaRdfMapper.class,
            SchemaFileMapper.class, true);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Fact.class);
        job.setReducerClass(SchemaFilterReducer.class);
        job.setNumReduceTasks(1);
        configureSchemaOutput();
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new SchemaFilter(), args));
    }

    public static class SchemaTableMapper extends Mapper<Key, Value,
            NullWritable, Fact> {
        private Fact fact = new Fact();
        /**
         * Output a triple if it is schema information.
         */
        @Override
        public void map(Key row, Value data, Context context)
                throws IOException, InterruptedException {
            fact.setTriple(MRReasoningUtils.getStatement(row, data,
                context.getConfiguration()));
            boolean isSchemaTriple = Schema.isSchemaTriple(fact.getTriple());
            if (isSchemaTriple) {
                context.write(NullWritable.get(), fact);
            }
            countInput(isSchemaTriple, context);
        }
    }

    public static class SchemaFileMapper extends Mapper<Fact,
            NullWritable, NullWritable, Fact> {
        /**
         * For a given fact, output it if it's a schema triple.
         */
        @Override
        public void map(Fact fact, NullWritable nw, Context context)
            throws IOException, InterruptedException {
            if (Schema.isSchemaTriple(fact.getTriple())) {
                context.write(NullWritable.get(), fact);
            }
        }
    }

    public static class SchemaRdfMapper extends Mapper<LongWritable,
            RyaStatementWritable, NullWritable, Fact> {
        private Fact fact = new Fact();
        /**
         * For a given fact, output it if it's a schema triple.
         */
        @Override
        public void map(LongWritable key, RyaStatementWritable rsw, Context context)
            throws IOException, InterruptedException {
            fact.setTriple(rsw.getRyaStatement());
            boolean isSchemaTriple = Schema.isSchemaTriple(fact.getTriple());
            if (isSchemaTriple) {
                context.write(NullWritable.get(), fact);
            }
            countInput(isSchemaTriple, context);
        }
    }

    public static class SchemaFilterReducer extends Reducer<NullWritable,
            Fact, NullWritable, SchemaWritable> {
        private SchemaWritable schema;
        private Logger log = Logger.getLogger(SchemaFilterReducer.class);
        private static int LOG_INTERVAL = 1000;
        private boolean debug = false;
        private MultipleOutputs<?, ?> debugOut;
        private Text debugKey = new Text();
        private Text debugValue = new Text();

        @Override
        protected void setup(Context context) {
            schema = new SchemaWritable();
            debug = MRReasoningUtils.debug(context.getConfiguration());
            debugOut = new MultipleOutputs<>(context);
        }

        /**
         * Collect all schema information into a Schema object, use it to derive
         * as much additional schema information as we can, and serialize it to
         * an HDFS file.
         */
        @Override
        protected void reduce(NullWritable key, Iterable<Fact> triples,
                Context context) throws IOException, InterruptedException {
            long count = 0;
            for (Fact fact : triples) {
                schema.processTriple(fact.getTriple());
                count++;
                if (count % LOG_INTERVAL == 0) {
                    log.debug("After " + count + " schema triples...");
                    log.debug(schema.getSummary());
                }
                if (debug) {
                    debugKey.set("SCHEMA TRIPLE " + count);
                    debugValue.set(fact.explain(false));
                    debugOut.write(MRReasoningUtils.DEBUG_OUT, debugKey, debugValue);
                }
            }
            log.debug("Total: " + count + " schema triples");
            log.debug(schema.getSummary());
        }

        @Override
        protected void cleanup(Context context) throws IOException,
                InterruptedException {
            if (debugOut != null) {
                debugOut.close();
            }
            // Perform schema-level reasoning
            schema.closure();
            // Output the complete schema
            context.write(NullWritable.get(), schema);
        }
    }
}
