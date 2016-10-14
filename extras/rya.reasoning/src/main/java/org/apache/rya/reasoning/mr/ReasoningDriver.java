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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Runs a forward-chaining reasoner until no new facts can be derived.
 */
public class ReasoningDriver extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new ReasoningDriver(), args);
        System.exit(result);
    }

    private boolean reportStats = false;
    long numInconsistencies = 0;

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        reportStats = MRReasoningUtils.stats(conf);
        int iteration = 0;
        long newInconsistencies;
        long newInstance;
        long newSchema;
        long usefulOutput;
        int result = 0;
        boolean productive = true;
        boolean findings = false;
        SchemaFilter filter;
        ForwardChain fc;
        DuplicateElimination de;
        RunStatistics runStats = new RunStatistics(MRReasoningUtils.getTableName(conf));

        // If running against a local file, upload it
        MRReasoningUtils.uploadIfNecessary(conf);

        // Extract schema information from the database and save it to a file,
        // unless the file already exists
        Path schemaPath = MRReasoningUtils.getSchemaPath(conf);
        if (!FileSystem.get(conf).isDirectory(schemaPath)) {
            filter = new SchemaFilter();
            result = ToolRunner.run(conf, filter, args);
            if (result != 0) {
                productive = false;
            }
            // Record basic information about the run
            runStats.collect(filter, "SchemaFilter");
        }

        // Perform forward-chaining reasoning:
        while (productive) {
            MRReasoningUtils.nextIteration(conf);
            // Attempt to derive new information
            fc = new ForwardChain();
            result = ToolRunner.run(conf, fc, args);
            runStats.collect(fc, "ForwardChain");
            if (result != 0) {
                break;
            }

            // Only keep unique, newly generated facts
            newInstance = fc.getNumInstanceTriples();
            newSchema = fc.getNumSchemaTriples();
            newInconsistencies = fc.getNumInconsistencies();
            usefulOutput = fc.getNumUsefulOutput();
            if (newInstance + newInconsistencies > 0) {
                de = new DuplicateElimination();
                result = ToolRunner.run(conf, de, args);
                runStats.collect(de, "DuplicateElimination");
                if (result != 0) {
                    break;
                }
                newInstance = de.getNumInstanceTriples();
                newSchema = de.getNumSchemaTriples();
                newInconsistencies = de.getNumInconsistencies();
                usefulOutput = de.getNumUsefulOutput();
            }

            // If schema triples were just deduced, regenerate the whole schema
            if (newSchema > 0) {
                MRReasoningUtils.schemaUpdated(conf);
                filter = new SchemaFilter();
                result = ToolRunner.run(conf, filter, args);
                runStats.collect(filter, "SchemaFilter");
                if (result != 0) {
                    break;
                }
            }

            iteration = MRReasoningUtils.getCurrentIteration(conf);
            if (!reportStats) {
                System.out.println("Iteration " + iteration + ":");
                System.out.println("\t" + newInstance + " new instance triples (" +
                    usefulOutput + " useful for reasoning)");
                System.out.println("\t" + newSchema + " new schema triples");
                System.out.println("\t" + newInconsistencies + " new inconsistencies");
            }
            if (newInconsistencies + newInstance + newSchema > 0) {
                findings = true;
            }
            numInconsistencies += newInconsistencies;
            // Repeat if we're still generating information
            productive = usefulOutput + newSchema > 0;
        }

        // Generate final output, if appropriate
        if (result == 0 && findings && MRReasoningUtils.shouldOutput(conf)) {
            OutputTool out = new OutputTool();
            result = ToolRunner.run(conf, out, args);
            runStats.collect(out, "OutputTool");
        }

        // Clean up intermediate data, if appropriate
        MRReasoningUtils.clean(conf);

        // Print stats, if specified
        if (reportStats) {
            System.out.println(runStats.report());
        }

        return result;
    }

    /**
     * True if we've detected at least one inconsistency.
    */
    boolean hasInconsistencies() {
        return numInconsistencies > 0;
    }
}
