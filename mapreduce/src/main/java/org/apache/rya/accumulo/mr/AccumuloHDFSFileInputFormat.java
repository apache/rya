package org.apache.rya.accumulo.mr;

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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.rfile.RFileOperations;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * {@link FileInputFormat} that finds the Accumulo tablet files on the HDFS
 * disk, and uses that as the input for MapReduce jobs.
 */
public class AccumuloHDFSFileInputFormat extends FileInputFormat<Key, Value> {

    public static final Range ALLRANGE = new Range(new Text("\u0000"), new Text("\uFFFD"));

    @Override
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException {
        //read the params from AccumuloInputFormat
        Configuration conf = jobContext.getConfiguration();
        Instance instance = MRUtils.AccumuloProps.getInstance(jobContext);
        String user = MRUtils.AccumuloProps.getUsername(jobContext);
        AuthenticationToken password = MRUtils.AccumuloProps.getPassword(jobContext);
        String table = MRUtils.AccumuloProps.getTablename(jobContext);
        ArgumentChecker.notNull(instance);
        ArgumentChecker.notNull(table);

        //find the files necessary
        try {
            Connector connector = instance.getConnector(user, password);
            TableOperations tos = connector.tableOperations();
            String tableId = tos.tableIdMap().get(table);
            Scanner scanner = connector.createScanner("accumulo.metadata", Authorizations.EMPTY); //TODO: auths?
            scanner.setRange(new Range(new Text(tableId + "\u0000"), new Text(tableId + "\uFFFD")));
            scanner.fetchColumnFamily(new Text("file"));
            List<String> files = new ArrayList<String>();
            List<InputSplit> fileSplits = new ArrayList<InputSplit>();
            for (Map.Entry<Key, Value> entry : scanner) {
                String file = entry.getKey().getColumnQualifier().toString();
                Path path = new Path(file);
                FileSystem fs = path.getFileSystem(conf);
                FileStatus fileStatus = fs.getFileStatus(path);
                long len = fileStatus.getLen();
                BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(fileStatus, 0, len);
                files.add(file);
                fileSplits.add(new FileSplit(path, 0, len, fileBlockLocations[0].getHosts()));
            }
            System.out.println(files);
            return fileSplits;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public RecordReader<Key, Value> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new RecordReader<Key, Value>() {

            private FileSKVIterator fileSKVIterator;
            private boolean started = false;

            @Override
            public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
                FileSplit split = (FileSplit) inputSplit;
                Configuration job = taskAttemptContext.getConfiguration();
                Path file = split.getPath();
                FileSystem fs = file.getFileSystem(job);
                Instance instance = MRUtils.AccumuloProps.getInstance(taskAttemptContext);

                fileSKVIterator = RFileOperations.getInstance().openReader(file.toString(), ALLRANGE,
                        new HashSet<ByteSequence>(), false, fs, job, instance.getConfiguration());
            }

            @Override
            public boolean nextKeyValue() throws IOException, InterruptedException {
                if (started) {
                    fileSKVIterator.next();
                }
                else {
                    started = true; // don't move past the first record yet
                }
                return fileSKVIterator.hasTop();
            }

            @Override
            public Key getCurrentKey() throws IOException, InterruptedException {
                return fileSKVIterator.getTopKey();
            }

            @Override
            public Value getCurrentValue() throws IOException, InterruptedException {
                return fileSKVIterator.getTopValue();
            }

            @Override
            public float getProgress() throws IOException, InterruptedException {
                return 0;
            }

            @Override
            public void close() throws IOException {
            }
        };
    }

    /**
     * Mapper that has no effect.
     */
    @SuppressWarnings("rawtypes")
    public static class NullMapper extends Mapper {
        @Override
        protected void map(Object key, Object value, Context context) throws IOException, InterruptedException {
        }
    }
}
