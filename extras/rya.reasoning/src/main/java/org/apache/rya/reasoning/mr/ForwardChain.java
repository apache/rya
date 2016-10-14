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
import org.apache.rya.reasoning.LocalReasoner;
import org.apache.rya.reasoning.LocalReasoner.Relevance;
import org.apache.rya.reasoning.Fact;
import org.apache.rya.reasoning.Schema;

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
import org.openrdf.model.Resource;

public class ForwardChain extends AbstractReasoningTool {
    @Override
    protected void configureReasoningJob(String[] args) throws Exception {
        distributeSchema();
        Configuration conf = job.getConfiguration();
        // We can ignore irrelevant triples, unless the schema has just changed
        // and therefore we can't rely on previous determinations of relevance.
        configureMultipleInput(TableMapper.class, RdfMapper.class,
            FileMapper.class, !MRReasoningUtils.isSchemaNew(conf));
        job.setMapOutputKeyClass(ResourceWritable.class);
        job.setMapOutputValueClass(Fact.class);
        job.setReducerClass(ReasoningReducer.class);
        job.setSortComparatorClass(ResourceWritable.SecondaryComparator.class);
        job.setGroupingComparatorClass(ResourceWritable.PrimaryComparator.class);
        configureDerivationOutput(true);
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new ForwardChain(), args));
    }

    /**
     * Decide whether to output facts and with what keys. Subclasses handle
     * different sources of input.
     */
    public static class ForwardChainMapper<K, V> extends Mapper<K, V,
            ResourceWritable, Fact> {
        protected Schema schema;
        protected ResourceWritable node = new ResourceWritable();
        protected MultipleOutputs<?, ?> debugOut;
        protected boolean debug;
        private Text debugKey = new Text();
        private Text debugValue = new Text();
        public ForwardChainMapper(Schema s) {
            this.schema = s;
        }
        public ForwardChainMapper() {}

        @Override
        protected void setup(Context context) {
            debugOut = new MultipleOutputs<>(context);
            Configuration conf = context.getConfiguration();
            if (schema == null) {
                schema = MRReasoningUtils.loadSchema(context.getConfiguration());
            }
            debug = MRReasoningUtils.debug(conf);
        }
        @Override
        public void cleanup(Context context) throws IOException,
                InterruptedException {
            if (debugOut != null) {
                debugOut.close();
            }
        }

        protected void process(Context context, Fact inputTriple)
                throws IOException, InterruptedException {
            Relevance rel = LocalReasoner.relevantFact(inputTriple, schema);
            if (rel.subject()) {
                node.set(inputTriple.getSubject(), 1);
                context.write(node, inputTriple);
                if (debug) {
                    int i = inputTriple.getIteration();
                    debugKey.set("MAP_OUT" + node.toString());
                    debugValue.set(inputTriple.explain(false) + "[" + i + "]");
                    debugOut.write(MRReasoningUtils.DEBUG_OUT, debugKey,
                        debugValue);
                }
            }
            if (rel.object()) {
                node.set((Resource) inputTriple.getObject(), -1);
                context.write(node, inputTriple);
                if (debug) {
                    int i = inputTriple.getIteration();
                    debugKey.set("MAP_OUT" + node.toString());
                    debugValue.set(inputTriple.explain(false) + "[" + i + "]");
                    debugOut.write(MRReasoningUtils.DEBUG_OUT, debugKey,
                        debugValue);
                }
            }
        }
    }

    /**
     * Get input data from the database
     */
    public static class TableMapper extends ForwardChainMapper<Key, Value> {
        private Fact inputTriple = new Fact();
        public TableMapper() { super(); }
        public TableMapper(Schema s) { super(s); }
        @Override
        public void map(Key row, Value data, Context context)
                throws IOException, InterruptedException {
            inputTriple.setTriple(MRReasoningUtils.getStatement(row, data,
                context.getConfiguration()));
            process(context, inputTriple);
        }
    }

    /**
     * Get intermediate data from a sequence file
     */
    public static class FileMapper extends ForwardChainMapper<Fact,
            NullWritable> {
        public FileMapper() { super(); }
        public FileMapper(Schema s) { super(s); }
        @Override
        public void map(Fact inputTriple, NullWritable nw,
                Context context) throws IOException, InterruptedException {
            process(context, inputTriple);
        }
    }

    /**
     * Get input data from an RDF file
     */
    public static class RdfMapper extends ForwardChainMapper<LongWritable,
            RyaStatementWritable> {
        private Fact inputTriple = new Fact();
        public RdfMapper() { super(); }
        public RdfMapper(Schema s) { super(s); }
        @Override
        public void map(LongWritable key, RyaStatementWritable rsw,
                Context context) throws IOException, InterruptedException {
            inputTriple.setTriple(rsw.getRyaStatement());
            process(context, inputTriple);
        }
    }

    public static class ReasoningReducer extends Reducer<ResourceWritable,
            Fact, Fact, NullWritable> {
        private static final int LOG_INTERVAL = 5000;
        private Logger log = Logger.getLogger(ReasoningReducer.class);
        private MultipleOutputs<?, ?> mout;
        private Schema schema;
        private boolean debug;
        private Text debugK = new Text();
        private Text debugV = new Text();
        private int maxStored = 0;
        private String maxNode = "";
        public ReasoningReducer(Schema s) {
            this.schema = s;
        }
        public ReasoningReducer() {}
        @Override
        public void setup(Context context) {
            mout = new MultipleOutputs<>(context);
            Configuration conf = context.getConfiguration();
            if (schema == null) {
                schema = MRReasoningUtils.loadSchema(conf);
            }
            debug = MRReasoningUtils.debug(conf);
        }
        @Override
        public void cleanup(Context context) throws IOException,
                InterruptedException {
            if (mout != null) {
                mout.close();
            }
            log.info("Most input triples stored at one time by any reasoner: "
                + maxStored + " (reasoner for node: " + maxNode + ")");
        }
        @Override
        public void reduce(ResourceWritable key, Iterable<Fact> facts,
                Context context) throws IOException, InterruptedException {
            log.debug("Reasoning for node " + key.toString());
            // If the schema was just updated, all facts are potentially
            // meaningful again. Otherwise, any new derivation must use at
            // least one fact from the previous (or this) iteration.
            Configuration conf = context.getConfiguration();
            LocalReasoner reasoner = new LocalReasoner(key.get(), schema,
                MRReasoningUtils.getCurrentIteration(conf),
                MRReasoningUtils.lastSchemaUpdate(conf));
            long numInput = 0;
            long numOutput = 0;
            for (Fact fact : facts) {
                if (debug) {
                    debugK.set("INPUT<" + key.get().stringValue() + ">");
                    debugV.set(fact.toString());
                    mout.write(MRReasoningUtils.DEBUG_OUT, debugK, debugV);
                }
                // We actually need separate fact objects, as the reasoner might
                // store them (default is to reuse the same object each time)
                reasoner.processFact(fact.clone());
                numInput++;
                numOutput += handleResults(reasoner, context);
                if (numInput % LOG_INTERVAL == 0) {
                    log.debug(reasoner.getDiagnostics());
                    log.debug(numInput + " input triples so far");
                    log.debug(numOutput + " output triples/inconsistencies so far");
                }
            }
            reasoner.getTypes();
            numOutput += handleResults(reasoner, context);
            int numStored = reasoner.getNumStored();
            if (numStored > maxStored) {
                maxStored = numStored;
                maxNode = key.toString();
            }
            log.debug("..." + numStored + " input facts stored in memory");
        }

        /**
         * Process any new results from a reasoner.
         */
        private long handleResults(LocalReasoner reasoner, Context context)
                throws IOException, InterruptedException {
            long numOutput = 0;
            if (reasoner.hasNewFacts()) {
                for (Fact fact : reasoner.getFacts()) {
                    mout.write(getOutputName(fact), fact, NullWritable.get());
                    numOutput++;
                    if (debug) {
                        debugK.set("OUTPUT<" + reasoner.getNode().stringValue() + ">");
                        debugV.set(fact.explain(false));
                        mout.write(MRReasoningUtils.DEBUG_OUT, debugK, debugV);
                    }
                }
            }
            if (reasoner.hasInconsistencies()) {
                for (Derivation inconsistency : reasoner.getInconsistencies()) {
                    mout.write(getOutputName(inconsistency), inconsistency,
                        NullWritable.get());
                    numOutput++;
                    if (debug) {
                        debugK.set("OUTPUT<" + inconsistency.getNode().stringValue() + ">");
                        debugV.set(inconsistency.explain(false));
                        mout.write(MRReasoningUtils.DEBUG_OUT, debugK, debugV);
                    }
                }
            }
            return numOutput;
        }
    }
}
