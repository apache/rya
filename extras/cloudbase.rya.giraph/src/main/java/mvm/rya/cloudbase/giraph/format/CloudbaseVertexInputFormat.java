/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package mvm.rya.cloudbase.giraph.format;

import cloudbase.core.client.mapreduce.CloudbaseInputFormat;
import cloudbase.core.data.Key;
import cloudbase.core.data.Value;
import org.apache.giraph.graph.VertexInputFormat;
import org.apache.giraph.graph.VertexReader;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;

/**
 * Class which wraps the CloudbaseInputFormat. It's designed
 * as an extension point to VertexInputFormat subclasses who wish
 * to read from Cloudbase Tables.
 *
 * @param <I> vertex id type
 * @param <V> vertex value type
 * @param <E> edge type
 * @param <M> message type
 */
public abstract class CloudbaseVertexInputFormat<
        I extends WritableComparable,
        V extends Writable,
        E extends Writable,
        M extends Writable>
        extends VertexInputFormat<I, V, E, M> implements Configurable {
    /**
     * delegate input format for all cloudbase operations.
     */
    protected CloudbaseInputFormat cloudbaseInputFormat =
            new CloudbaseInputFormat();

    /**
     * Configured and injected by the job
     */
    private Configuration conf;

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    /**
     * Abstract class which provides a template for instantiating vertices
     * from Cloudbase Key/Value pairs.
     *
     * @param <I> vertex id type
     * @param <V> vertex value type
     * @param <E> edge type
     * @param <M> message type
     */
    public abstract static class CloudbaseVertexReader<
            I extends WritableComparable,
            V extends Writable, E extends Writable, M extends Writable>
            implements VertexReader<I, V, E, M> {

        /**
         * Used by subclasses to read key/value pairs.
         */
        private final RecordReader<Key, Value> reader;
        /**
         * Context passed to initialize
         */
        private TaskAttemptContext context;

        /**
         * Constructor used to pass Record Reader instance
         *
         * @param reader Cloudbase record reader
         */
        public CloudbaseVertexReader(RecordReader<Key, Value> reader) {
            this.reader = reader;
        }

        @Override
        public void initialize(InputSplit inputSplit,
                               TaskAttemptContext context)
                throws IOException, InterruptedException {
            reader.initialize(inputSplit, context);
            this.context = context;
        }

        /**
         * close
         *
         * @throws IOException
         */
        public void close() throws IOException {
            reader.close();
        }

        /**
         * getProgress
         *
         * @return progress
         * @throws IOException
         * @throws InterruptedException
         */
        public float getProgress() throws IOException, InterruptedException {
            return reader.getProgress();
        }

        /**
         * Get the result record reader
         *
         * @return Record reader to be used for reading.
         */
        protected RecordReader<Key, Value> getRecordReader() {
            return reader;
        }

        /**
         * getContext
         *
         * @return Context passed to initialize.
         */
        protected TaskAttemptContext getContext() {
            return context;
        }

    }

    /**
     * getSplits
     *
     * @param context    Context of the job
     * @param numWorkers Number of workers used for this job
     * @return tablet splits
     * @throws IOException
     * @throws InterruptedException
     */
    public List<InputSplit> getSplits(
            JobContext context, int numWorkers)
            throws IOException, InterruptedException {
        List<InputSplit> splits = null;
        try {
            splits = cloudbaseInputFormat.getSplits(context);
        } catch (IOException e) {
            if (e.getMessage().contains("Input info has not been set")) {
                throw new IOException(e.getMessage() +
                        " Make sure you initialized" +
                        " CloudbaseInputFormat static setters " +
                        "before passing the config to GiraphJob.");
            }
        }
        return splits;
    }
}
