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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapreduce.FileSystemCounter;
import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.TaskCounter;

/**
 * Collect and report a variety of statistics for a run. Prints MapReduce
 * metrics for each individual job, and overall totals, using Hadoop counters.
 */
public class RunStatistics {
    static Map<Stat, TaskCounter> taskCounters = new HashMap<>();
    static Map<Stat, JobCounter> jobCounters = new HashMap<>();

    private enum Stat {
        TABLE("tableName"),
        RUN("run"),
        ITERATION("iteration"),
        JOB("job"),

        ELAPSED_TIME("elapsed (ms)"),

        TBOX_IN("tbox in"),
        ABOX_IN("abox in"),
        INCONSISTENCIES_OUT("inconsistencies out"),
        TRIPLES_OUT("triples out"),

        MAP_INPUT_RECORDS("map input records", TaskCounter.MAP_INPUT_RECORDS),
        MAP_OUTPUT_RECORDS("map output records", TaskCounter.MAP_OUTPUT_RECORDS),
        REDUCE_INPUT_GROUPS("reduce input groups", TaskCounter.REDUCE_INPUT_GROUPS),

        MAPS("maps", JobCounter.TOTAL_LAUNCHED_MAPS),
        REDUCES("reduces", JobCounter.TOTAL_LAUNCHED_REDUCES),
        MAP_TIME("map time (ms)", JobCounter.MILLIS_MAPS),

        REDUCE_TIME("reduce time (ms)", JobCounter.MILLIS_REDUCES),
        MAP_TIME_VCORES("map time (ms) * cores", JobCounter.VCORES_MILLIS_MAPS),
        REDUCE_TIME_VCORES("reduce time (ms) * cores", JobCounter.VCORES_MILLIS_REDUCES),

        GC_TIME("gc time (ms)", TaskCounter.GC_TIME_MILLIS),
        CPU_TIME("total cpu time (ms)", TaskCounter.CPU_MILLISECONDS),

        MAP_TIME_MB("map time (ms) * memory (mb)", JobCounter.MB_MILLIS_MAPS),
        REDUCE_TIME_MB("reduce time (ms) * memory (mb)", JobCounter.MB_MILLIS_REDUCES),
        PHYSICAL_MEMORY_BYTES("physical memory (bytes)", TaskCounter.PHYSICAL_MEMORY_BYTES),
        VIRTUAL_MEMORY_BYTES("virtual memory (bytes)", TaskCounter.VIRTUAL_MEMORY_BYTES),

        DATA_LOCAL_MAPS("data-local maps", JobCounter.DATA_LOCAL_MAPS),
        MAP_OUTPUT_BYTES("map output bytes", TaskCounter.MAP_OUTPUT_BYTES),

        FILE_BYTES_READ("file bytes read"),
        HDFS_BYTES_READ("hdfs bytes read"),
        FILE_BYTES_WRITTEN("file bytes written"),
        HDFS_BYTES_WRITTEN("hdfs bytes written"),

        FRACTION_TIME_GC("proportion time in gc"),
        FRACTION_MEMORY_USAGE("proportion allocated memory used"),
        FRACTION_CPU_USAGE("proportion allocated cpu used");

        String name;
        Stat(String name) {
            this.name = name;
        }
        Stat(String key, JobCounter jc) {
            this.name = key;
            jobCounters.put(this, jc);
        }
        Stat(String key, TaskCounter tc) {
            this.name = key;
            taskCounters.put(this, tc);
        }
    }

    private class JobResult {
        Map<Stat, String> info = new HashMap<>();
        Map<Stat, Long> stats = new HashMap<>();

        void add(JobResult other) {
            for (Stat key : other.stats.keySet()) {
                if (this.stats.containsKey(key)) {
                    stats.put(key, this.stats.get(key) + other.stats.get(key));
                }
                else {
                    stats.put(key, other.stats.get(key));
                }
            }
        }

        void computeMetrics() {
            long t = stats.get(Stat.MAP_TIME) + stats.get(Stat.REDUCE_TIME);
            long b = stats.get(Stat.PHYSICAL_MEMORY_BYTES);
            long timeMbAllocated = stats.get(Stat.MAP_TIME_MB) + stats.get(Stat.REDUCE_TIME_MB);
            long timeVcores = stats.get(Stat.MAP_TIME_VCORES) + stats.get(Stat.REDUCE_TIME_VCORES);
            long gcTime = stats.get(Stat.GC_TIME);
            long cpuTime = stats.get(Stat.CPU_TIME);
            long tasks = stats.get(Stat.MAPS) + stats.get(Stat.REDUCES);
            double mb = b / 1024.0 / 1024.0;
            double avgMb = mb / tasks;
            double timeMbUsed = t * avgMb;
            info.put(Stat.FRACTION_TIME_GC, String.valueOf((double) gcTime / t));
            info.put(Stat.FRACTION_MEMORY_USAGE, String.valueOf(timeMbUsed / timeMbAllocated));
            info.put(Stat.FRACTION_CPU_USAGE, String.valueOf((double) cpuTime / timeVcores));
        }

        public String toString() {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < Stat.values().length; i++) {
                Stat s = Stat.values()[i];
                if (i > 0) {
                    sb.append(",");
                }
                if (info.containsKey(s)) {
                    sb.append(info.get(s));
                }
                else if (stats.containsKey(s)) {
                    sb.append(stats.get(s));
                }
                else {
                    sb.append("--");
                }
            }
            return sb.toString();
        }
    }

    List<JobResult> jobResults = new LinkedList<>();
    Map<String, JobResult> jobTypeResults = new HashMap<>();
    JobResult totals = new JobResult();

    String runId;
    String tableName;

    /**
     * Instantiate a RunStatistics object with respect to an overall run
     * (which can consist of many jobs). Runs are identified by their first
     * job.
     * @param   tableName   Name of the input table
     */
    RunStatistics(String tableName) {
        this.tableName = tableName;
        totals.info.put(Stat.TABLE, tableName);
        totals.info.put(Stat.ITERATION, "total");
        totals.info.put(Stat.JOB, "all");
    }

    /**
     * Collect all the statistics we're interested in for a single job.
     * @param   jobType     Name of job type (ForwardChain, etc.)
     */
    void collect(AbstractReasoningTool tool, String name) throws IOException,
            InterruptedException {
        // ID is ID of the first job run
        if (runId == null) {
            runId = tool.getJobID().toString();
            totals.info.put(Stat.RUN, runId);
        }
        JobResult jobValues = new JobResult();
        jobValues.info.put(Stat.TABLE, tableName);
        jobValues.info.put(Stat.RUN, runId);
        jobValues.info.put(Stat.ITERATION, String.valueOf(tool.getIteration()));
        jobValues.info.put(Stat.JOB, name);

        jobValues.stats.put(Stat.ELAPSED_TIME, tool.getElapsedTime());
        for (Stat key : taskCounters.keySet()) {
            jobValues.stats.put(key, tool.getCounter(taskCounters.get(key)));
        }
        for (Stat key : jobCounters.keySet()) {
            jobValues.stats.put(key, tool.getCounter(jobCounters.get(key)));
        }
        jobValues.stats.put(Stat.TBOX_IN, tool.getNumSchemaInput());
        jobValues.stats.put(Stat.ABOX_IN, tool.getNumInstanceInput());
        jobValues.stats.put(Stat.INCONSISTENCIES_OUT,
            tool.getNumInconsistencies());
        jobValues.stats.put(Stat.TRIPLES_OUT, tool.getNumSchemaTriples()
            + tool.getNumInstanceTriples());
        jobValues.stats.put(Stat.FILE_BYTES_READ, tool.getCounter(
            FileSystemCounter.class.getName(), "FILE_BYTES_READ"));
        jobValues.stats.put(Stat.FILE_BYTES_WRITTEN, tool.getCounter(
            FileSystemCounter.class.getName(), "FILE_BYTES_WRITTEN"));
        jobValues.stats.put(Stat.HDFS_BYTES_READ, tool.getCounter(
            FileSystemCounter.class.getName(), "HDFS_BYTES_READ"));
        jobValues.stats.put(Stat.HDFS_BYTES_WRITTEN, tool.getCounter(
            FileSystemCounter.class.getName(), "HDFS_BYTES_WRITTEN"));
        jobResults.add(jobValues);
        // Add to the running total for this job type (initialize if needed)
        if (!jobTypeResults.containsKey(name)) {
            JobResult typeResult = new JobResult();
            typeResult.info.put(Stat.TABLE, tableName);
            typeResult.info.put(Stat.RUN, runId);
            typeResult.info.put(Stat.ITERATION, "total");
            typeResult.info.put(Stat.JOB, name);
            jobTypeResults.put(name, typeResult);
        }
        jobTypeResults.get(name).add(jobValues);
        totals.add(jobValues);
    }

    /**
     * Report statistics for all jobs.
     */
    String report() {
        StringBuilder sb = new StringBuilder();
        // Header
        for (int i = 0; i < Stat.values().length; i++) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append(Stat.values()[i].name);
        }
        // One line per job
        for (JobResult result : jobResults) {
            result.computeMetrics();
            sb.append("\n").append(result);
        }
        // Include aggregates for jobs and overall
        if (jobTypeResults.containsKey("ForwardChain")) {
            jobTypeResults.get("ForwardChain").computeMetrics();
            sb.append("\n").append(jobTypeResults.get("ForwardChain"));
        }
        if (jobTypeResults.containsKey("DuplicateElimination")) {
            jobTypeResults.get("DuplicateElimination").computeMetrics();
            sb.append("\n").append(jobTypeResults.get("DuplicateElimination"));
        }
        totals.computeMetrics();
        sb.append("\n").append(totals);
        return sb.toString();
    }
}
