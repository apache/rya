package mvm.rya.reasoning.mr;

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

import mvm.rya.accumulo.mr.RyaStatementWritable;
import mvm.rya.accumulo.mr.RdfFileInputFormat;
import mvm.rya.accumulo.mr.MRUtils;
import mvm.rya.reasoning.Derivation;
import mvm.rya.reasoning.Fact;
import mvm.rya.reasoning.Schema;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.CombineSequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.openrdf.rio.RDFFormat;

/**
 * Contains common functionality for MapReduce jobs involved in reasoning. A
 * subclass should implement configureReasoningJob and its own mappers and
 * reducers.
 */
abstract public class AbstractReasoningTool extends Configured implements Tool {
    // Keep track of statistics about the input
    protected static enum COUNTERS { ABOX, TBOX, USEFUL };

    // MapReduce job, to be configured by subclasses
    protected Job job;

    /**
     * Configure the job's inputs, outputs, mappers, and reducers.
     */
    abstract protected void configureReasoningJob(String[] args) throws Exception;

    /**
     * Configure and run a MapReduce job.
     */
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        job = Job.getInstance(conf);
        job.setJobName(getJobName());
        job.setJarByClass(this.getClass());
        configureReasoningJob(args);
        boolean success = job.waitForCompletion(!MRReasoningUtils.stats(conf));
        if (success) {
            return 0;
        }
        else {
            return 1;
        }
    }

    /**
     * Cumulative CPU time taken by all mappers/reducers.
     */
    public long getCumulativeTime() throws IOException {
        return getCounter(TaskCounter.CPU_MILLISECONDS);
    }

    /**
     * Default name for the MapReduce job:
     */
    protected String getJobName() {
        return "Rya reasoning, pass " + MRReasoningUtils.getCurrentIteration(getConf())
            + ": " + this.getClass().getSimpleName() + "_" + System.currentTimeMillis();
    }

    /**
     * Number of inconsistencies detected by this job.
     */
    public long getNumInconsistencies() throws IOException {
        return getCounter(MultipleOutputs.class.getName(),
            MRReasoningUtils.INCONSISTENT_OUT);
    }

    /**
     * Number of new schema triples derived during this job.
     */
    public long getNumSchemaTriples() throws IOException {
        return getCounter(MultipleOutputs.class.getName(),
            MRReasoningUtils.SCHEMA_OUT);
    }

    /**
     * Number of new instance triples that might be used for future reasoning
     */
    public long getNumUsefulOutput() throws IOException {
        return getCounter(MultipleOutputs.class.getName(),
            MRReasoningUtils.INTERMEDIATE_OUT);
    }

    /**
     * Number of new instance triples that will not be used for future reasoning
     */
    public long getNumTerminalOutput() throws IOException {
        return getCounter(MultipleOutputs.class.getName(),
            MRReasoningUtils.TERMINAL_OUT);
    }

    /**
     * Total number of new instance triples derived during this job.
     */
    public long getNumInstanceTriples() throws IOException {
        return getNumUsefulOutput() + getNumTerminalOutput();
    }

    /**
     * Number of instance triples seen as input during this job.
     */
    public long getNumInstanceInput() throws IOException {
        return getCounter(COUNTERS.ABOX);
    }

    /**
     * Number of schema triples seen as input during this job.
     */
    public long getNumSchemaInput() throws IOException {
        return getCounter(COUNTERS.TBOX);
    }

    /**
     * Increment the schema or instance triple counter, as appropriate.
     */
    protected static void countInput(boolean schema, TaskAttemptContext context) {
        if (schema) {
            context.getCounter(COUNTERS.TBOX).increment(1);
        }
        else {
            context.getCounter(COUNTERS.ABOX).increment(1);
        }
    }

    /**
     * Add the schema file (TBox) to the distributed cache for the current job.
     */
    protected void distributeSchema() {
        Path schemaPath = MRReasoningUtils.getSchemaPath(job.getConfiguration());
        job.addCacheFile(schemaPath.toUri());
    }

    /**
     * Set up the MapReduce job to use as inputs both an Accumulo table and the
     * files containing previously derived information, excluding
     * inconsistencies.  Looks for a file for every iteration number so far,
     * preferring final cleaned up output from that iteration but falling back
     * on intermediate data if necessary.
     * @param tableMapper   Mapper class to use for database input
     * @param rdfMapper     Mapper class to use for direct RDF input
     * @param fileMapper    Mapper class to use for derived triples input
     * @param filter        True to exclude previously derived data that couldn't be
     *                      used to derive anything new at this point.
     */
    protected void configureMultipleInput(
            Class<? extends Mapper<Key, Value, ?, ?>> tableMapper,
            Class<? extends Mapper<LongWritable, RyaStatementWritable, ?, ?>> rdfMapper,
            Class<? extends Mapper<Fact, NullWritable, ?, ?>> fileMapper,
            boolean filter) throws IOException, AccumuloSecurityException {
        Path inputPath = MRReasoningUtils.getInputPath(job.getConfiguration());
        if (inputPath != null) {
            configureRdfInput(inputPath, rdfMapper);
        }
        else {
            configureAccumuloInput(tableMapper);
        }
        configureFileInput(fileMapper, filter);
    }

    /**
     * Set up the MapReduce job to use as inputs both an Accumulo table and the
     * files containing previously derived information. Looks for a file for
     * every iteration number so far, preferring final cleaned up output from
     * that iteration but falling back on intermediate data if necessary.
     * @param tableMapper   Mapper class to use for database input
     * @param rdfMapper     Mapper class to use for direct RDF input
     * @param fileMapper    Mapper class to use for derived triples input
     * @param incMapper     Mapper class to use for derived inconsistencies input
     * @param filter        True to exclude previously derived data that couldn't be
     *                      used to derive anything new at this point.
     */
    protected void configureMultipleInput(
            Class<? extends Mapper<Key, Value, ?, ?>> tableMapper,
            Class<? extends Mapper<LongWritable, RyaStatementWritable, ?, ?>> rdfMapper,
            Class<? extends Mapper<Fact, NullWritable, ?, ?>> fileMapper,
            Class<? extends Mapper<Derivation, NullWritable, ?, ?>> incMapper,
            boolean filter)
            throws IOException, AccumuloSecurityException {
        Path inputPath = MRReasoningUtils.getInputPath(job.getConfiguration());
        if (inputPath != null) {
            configureRdfInput(inputPath, rdfMapper);
        }
        else {
            configureAccumuloInput(tableMapper);
        }
        configureFileInput(fileMapper, incMapper, filter);
    }

    /**
     * Set up the MapReduce job to use file inputs from previous iterations,
     * excluding inconsistencies found.
     * @param   fileMapper  Mapper class to use for generated triples
     * @param   filter      Exclude facts that aren't helpful for inference
     */
    protected void configureFileInput(
            Class <? extends Mapper<Fact, NullWritable, ?, ?>> fileMapper,
            final boolean filter) throws IOException {
        configureFileInput(fileMapper, null, filter);
    }

    /**
     * Set up the MapReduce job to use file inputs from previous iterations.
     * @param   fileMapper  Mapper class for generated triples
     * @param   incMapper   Mapper class for generated inconsistenies
     * @param   filter      Exclude facts that aren't helpful for inference
     */
    protected void configureFileInput(
            Class <? extends Mapper<Fact, NullWritable, ?, ?>> fileMapper,
            Class <? extends Mapper<Derivation, NullWritable, ?, ?>> incMapper,
            final boolean filter) throws IOException {
        // Set up file input for all iterations up to this one
        Configuration conf = job.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        Path inputPath;
        int iteration = MRReasoningUtils.getCurrentIteration(conf);
        // Set min/max split, if not already provided:
        long blocksize = Long.parseLong(conf.get("dfs.blocksize"));
        String minSplitProp = "mapreduce.input.fileinputformat.split.minsize";
        String maxSplitProp = "mapreduce.input.fileinputformat.split.maxsize";
        conf.set(minSplitProp, conf.get(minSplitProp, String.valueOf(blocksize)));
        conf.set(maxSplitProp, conf.get(maxSplitProp, String.valueOf(blocksize*8)));
        for (int i = 1; i <= iteration; i++) {
            // Prefer cleaned output...
            inputPath = MRReasoningUtils.getOutputPath(conf,
                MRReasoningUtils.OUTPUT_BASE + i);
            // But if there isn't any, try intermediate data:
            if (!fs.isDirectory(inputPath)) {
                inputPath = MRReasoningUtils.getOutputPath(conf,
                    MRReasoningUtils.OUTPUT_BASE + i
                    + MRReasoningUtils.TEMP_SUFFIX);
            }
            // And only proceed if we found one or the other.
            if (fs.isDirectory(inputPath)) {
                // Never include debug output. If filter is true, select only
                // intermediate and schema data, otherwise include everything.
                PathFilter f = new PathFilter() {
                    public boolean accept(Path path) {
                        String s = path.getName();
                        if (s.startsWith(MRReasoningUtils.DEBUG_OUT)) {
                            return false;
                        }
                        else {
                            return !filter
                                || s.startsWith(MRReasoningUtils.INTERMEDIATE_OUT)
                                || s.startsWith(MRReasoningUtils.SCHEMA_OUT);
                        }
                    }
                };
                for (FileStatus status : fs.listStatus(inputPath, f)) {
                    if (status.getLen() > 0) {
                        Path p = status.getPath();
                        String s = p.getName();
                        if (s.startsWith(MRReasoningUtils.INCONSISTENT_OUT)) {
                            if (incMapper != null) {
                                MultipleInputs.addInputPath(job, p,
                                    CombineSequenceFileInputFormat.class, incMapper);
                            }
                        }
                        else {
                            MultipleInputs.addInputPath(job, status.getPath(),
                                CombineSequenceFileInputFormat.class, fileMapper);
                        }
                    }
                }
            }
        }
    }

    /**
     * Set up the MapReduce job to use Accumulo as an input.
     * @param tableMapper Mapper class to use
     */
    protected void configureAccumuloInput(Class<? extends Mapper<Key,Value,?,?>> tableMapper)
            throws AccumuloSecurityException {
        MRReasoningUtils.configureAccumuloInput(job);
        MultipleInputs.addInputPath(job, new Path("/tmp/input"),
            AccumuloInputFormat.class, tableMapper);
    }

    /**
     * Set up the MapReduce job to use an RDF file as an input.
     * @param rdfMapper class to use
     */
    protected void configureRdfInput(Path inputPath,
            Class<? extends Mapper<LongWritable, RyaStatementWritable, ?, ?>> rdfMapper) {
        Configuration conf = job.getConfiguration();
        String format = conf.get(MRUtils.FORMAT_PROP, RDFFormat.RDFXML.getName());
        conf.set(MRUtils.FORMAT_PROP, format);
        MultipleInputs.addInputPath(job, inputPath,
            RdfFileInputFormat.class, rdfMapper);
    }

    /**
     * Set up the MapReduce job to output a schema (TBox).
     */
    protected void configureSchemaOutput() {
        Path outPath = MRReasoningUtils.getSchemaPath(job.getConfiguration());
        SequenceFileOutputFormat.setOutputPath(job, outPath);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(SchemaWritable.class);
        LazyOutputFormat.setOutputFormatClass(job, SequenceFileOutputFormat.class);
        MultipleOutputs.addNamedOutput(job, "schemaobj",
            SequenceFileOutputFormat.class, NullWritable.class, SchemaWritable.class);
        MultipleOutputs.addNamedOutput(job, MRReasoningUtils.DEBUG_OUT,
            TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.setCountersEnabled(job, true);
    }

    /**
     * Set up the MapReduce job to output newly derived triples. Outputs to
     * directory [base]-[iteration].
     */
    protected void configureDerivationOutput() {
        configureDerivationOutput(false);
    }

    /**
     * Set up a MapReduce job to output newly derived triples.
     * @param   intermediate    True if this is intermediate data. Outputs
     *                          to [base]-[iteration]-[temp].
     */
    protected void configureDerivationOutput(boolean intermediate) {
        Path outPath;
        Configuration conf = job.getConfiguration();
        int iteration = MRReasoningUtils.getCurrentIteration(conf);
        if (intermediate) {
            outPath = MRReasoningUtils.getOutputPath(conf,
                MRReasoningUtils.OUTPUT_BASE + iteration
                + MRReasoningUtils.TEMP_SUFFIX);
        }
        else {
            outPath = MRReasoningUtils.getOutputPath(conf,
                MRReasoningUtils.OUTPUT_BASE + iteration);
        }
        SequenceFileOutputFormat.setOutputPath(job, outPath);
        LazyOutputFormat.setOutputFormatClass(job, SequenceFileOutputFormat.class);
        MultipleOutputs.addNamedOutput(job, MRReasoningUtils.INTERMEDIATE_OUT,
            SequenceFileOutputFormat.class, Fact.class, NullWritable.class);
        MultipleOutputs.addNamedOutput(job, MRReasoningUtils.TERMINAL_OUT,
            SequenceFileOutputFormat.class, Fact.class, NullWritable.class);
        MultipleOutputs.addNamedOutput(job, MRReasoningUtils.SCHEMA_OUT,
            SequenceFileOutputFormat.class, Fact.class, NullWritable.class);
        MultipleOutputs.addNamedOutput(job, MRReasoningUtils.INCONSISTENT_OUT,
            SequenceFileOutputFormat.class, Derivation.class, NullWritable.class);
        MultipleOutputs.setCountersEnabled(job, true);
        // Set up an output for diagnostic info, if needed
        MultipleOutputs.addNamedOutput(job, MRReasoningUtils.DEBUG_OUT,
            TextOutputFormat.class, Text.class, Text.class);
    }

    /**
     * Set up a MapReduce job to output human-readable text.
     */
    protected void configureTextOutput(String destination) {
        Path outPath;
        outPath = MRReasoningUtils.getOutputPath(job.getConfiguration(), destination);
        TextOutputFormat.setOutputPath(job, outPath);
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        MultipleOutputs.addNamedOutput(job, MRReasoningUtils.INTERMEDIATE_OUT,
            TextOutputFormat.class, NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, MRReasoningUtils.TERMINAL_OUT,
            TextOutputFormat.class, NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, MRReasoningUtils.SCHEMA_OUT,
            TextOutputFormat.class, NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, MRReasoningUtils.INCONSISTENT_OUT,
            TextOutputFormat.class, NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, MRReasoningUtils.DEBUG_OUT,
            TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.setCountersEnabled(job, true);
    }

    /**
     * Get the name of the output to send an inconsistency to.
     * @return  The name of the output file(s) to send inconsistencies to
     */
    protected static String getOutputName(Derivation inconsistency) {
        return MRReasoningUtils.INCONSISTENT_OUT;
    }

    /**
     * Get the name of the output to send a fact to.
     * @param   fact    The fact itself
     * @param   finalOut    True if this is for final output, not intermediate
     * @return  The name of the output file(s) to send this fact to
     */
    protected static String getOutputName(Fact fact, boolean finalOut) {
        if (Schema.isSchemaTriple(fact.getTriple())) {
            return MRReasoningUtils.SCHEMA_OUT;
        }
        else if (!finalOut && fact.isUseful()) {
            return MRReasoningUtils.INTERMEDIATE_OUT;
        }
        else {
            return MRReasoningUtils.TERMINAL_OUT;
        }
    }

    /**
     * Get the name of the output to send a fact to.
     */
    protected static String getOutputName(Fact fact) {
        return getOutputName(fact, false);
    }

    /**
     * Retrieve an arbitrary counter's value.
     * @param   group Counter's group name
     * @param   counter Name of the counter itself
     */
    public long getCounter(String group, String counter) throws IOException {
        return job.getCounters().findCounter(group, counter).getValue();
    }

    /**
     * Retrieve an arbitrary counter's value.
     * @param   key     The Enum tied to this counter
     */
    public long getCounter(Enum<?> key) throws IOException {
        return job.getCounters().findCounter(key).getValue();
    }

    /**
     * Get the current iteration according to this job's configuration.
     */
    public int getIteration() {
        return MRReasoningUtils.getCurrentIteration(getConf());
    }

    /**
     * Get the job's JobID.
     */
    public JobID getJobID() {
        return job.getJobID();
    }

    /**
     * Get the elapsed wall-clock time, assuming the job is done.
     */
    public long getElapsedTime() throws IOException, InterruptedException {
        return job.getFinishTime() - job.getStartTime();
    }
}
