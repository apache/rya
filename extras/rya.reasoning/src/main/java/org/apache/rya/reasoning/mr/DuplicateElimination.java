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
import org.apache.rya.reasoning.Derivation;
import org.apache.rya.reasoning.Fact;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class DuplicateElimination extends AbstractReasoningTool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new DuplicateElimination(), args));
    }

    @Override
    protected void configureReasoningJob(String[] args) throws Exception {
        configureMultipleInput(DuplicateTableMapper.class,
            DuplicateRdfMapper.class, DuplicateFileMapper.class,
            InconsistencyMapper.class, false);
        job.setMapOutputKeyClass(Fact.class);
        job.setMapOutputValueClass(Derivation.class);
        job.setReducerClass(DuplicateEliminationReducer.class);
        configureDerivationOutput();
    }

    public static class DuplicateEliminationMapper<K, V> extends Mapper<K, V,
            Fact, Derivation> {
        private MultipleOutputs<?, ?> debugOut;
        private boolean debug;
        private Text debugK = new Text();
        private Text debugV = new Text();
        private Fact emptyFact = new Fact();
        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            debug = MRReasoningUtils.debug(conf);
            if (debug) {
                debugOut = new MultipleOutputs<>(context);
            }
        }
        @Override
        public void cleanup(Context context) throws IOException,
                InterruptedException {
            if (debugOut != null) {
                debugOut.close();
            }
        }
        protected void process(Context context, Fact fact, Derivation d,
                String source) throws IOException, InterruptedException {
            context.write(fact, d);
        }

        protected void process(Context context, Fact fact,
                String source) throws IOException, InterruptedException {
            if (debug) {
                debugK.set("MAP_" + source + ": " + fact.explain(false));
                debugV.set("iteration=" + fact.getIteration()
                    + ", size=" + fact.span());
                debugOut.write(MRReasoningUtils.DEBUG_OUT, debugK, debugV);
            }
            Derivation d = fact.unsetDerivation();
            process(context, fact, d, source);
        }

        protected void process(Context context, Derivation d,
                String source) throws IOException, InterruptedException {
            if (debug) {
                debugK.set("MAP_" + source + ": inconsistency : "
                    + d.explain(false));
                debugV.set("iteration=" + d.getIteration()
                    + ", size=" + d.span());
                debugOut.write(MRReasoningUtils.DEBUG_OUT, debugK, debugV);
            }
            emptyFact.setDerivation(d);
            process(context, emptyFact, d, source);
        }
    }

    public static class DuplicateTableMapper extends DuplicateEliminationMapper<
            Key, Value> {
        private Fact inputTriple = new Fact();
        @Override
        public void map(Key row, Value data, Context context)
                throws IOException, InterruptedException {
            inputTriple.setTriple(MRReasoningUtils.getStatement(row, data,
                context.getConfiguration()));
            process(context, inputTriple, "TABLE");
        }
    }

    public static class DuplicateFileMapper extends DuplicateEliminationMapper<
            Fact, NullWritable> {
        @Override
        public void map(Fact key, NullWritable value, Context context)
                throws IOException, InterruptedException {
            process(context, key, "STEP-" + key.getIteration());
        }
    }

    public static class DuplicateRdfMapper extends DuplicateEliminationMapper<
            LongWritable, RyaStatementWritable> {
        private Fact inputTriple = new Fact();
        @Override
        public void map(LongWritable key, RyaStatementWritable value,
                Context context) throws IOException, InterruptedException {
            inputTriple.setTriple(value.getRyaStatement());
            process(context, inputTriple, "RDF");
        }
    }

    public static class InconsistencyMapper extends DuplicateEliminationMapper<
            Derivation, NullWritable> {
        @Override
        public void map(Derivation key, NullWritable value, Context context)
                throws IOException, InterruptedException {
            process(context, key, "INCONSISTENCY-STEP-" + key.getIteration());
        }
    }

    public static class DuplicateEliminationReducer extends Reducer<
            Fact, Derivation, Fact, NullWritable> {
        protected MultipleOutputs<?, ?> mout;
        protected int current;
        protected boolean debug;
        protected Logger log = Logger.getLogger(DuplicateEliminationReducer.class);
        protected long totalInput = 0;
        protected long totalFacts = 0;
        protected long totalOutput = 0;
        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            mout = new MultipleOutputs<>(context);
            current = MRReasoningUtils.getCurrentIteration(conf);
            debug = MRReasoningUtils.debug(conf);
        }
        @Override
        public void cleanup(Context context) throws IOException,
                InterruptedException {
            mout.close();
            log.info("Input records processed: " + totalInput);
            log.info("Distinct facts: " + totalFacts);
            log.info("Output facts: " + totalOutput);
        }
        @Override
        public void reduce(Fact fact, Iterable<Derivation> derivations,
                Context context) throws IOException, InterruptedException {
            log.debug(fact.toString() + ":");
            totalFacts++;
            // We only need to output this fact if it hasn't been derived
            // before this step (and wasn't in the original data, marked
            // as iteration 0). If we do want to output it, prefer the simplest
            // derivation.
            Derivation best = null;
            boolean newFact = true;
            int count = 0;
            for (Derivation derivation : derivations) {
                count++;
                if (newFact) {
                    if (derivation.getIteration() >= current) {
                        // Valid so far; check if this is the best derivation:
                        if (best == null || best.span() > derivation.span()) {
                            best = derivation.clone();
                        }
                    }
                    else if (debug) {
                        newFact = false;
                    }
                    else {
                        return;
                    }
                }
                if (debug) {
                    mout.write(MRReasoningUtils.DEBUG_OUT,
                        new Text("DE " + fact.toString() + derivation.explain(false)),
                        new Text(Integer.toString(count) + "\t" + newFact));
                }
            }
            totalInput += count;
            if (newFact) {
                totalOutput++;
                if (fact.isEmpty()) {
                    // If there's no triple, it must be an inconsistency
                    mout.write(getOutputName(best), best, NullWritable.get());
                }
                else {
                    // Output a triple
                    fact.setDerivation(best);
                    mout.write(getOutputName(fact), fact, NullWritable.get());
                }
            }
            log.debug(totalFacts + " facts, " + totalInput + " input records, "
                + totalOutput + " output records");
        }
    }
}
