package mvm.rya.accumulo.mr.utils;

/*
 * #%L
 * mvm.rya.accumulo.rya
 * %%
 * Copyright (C) 2014 Rya
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.rfile.RFileOperations;
import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

/**
 * Finds the accumulo tablet files on the hdfs disk, and uses that as the input for MR jobs
 * Date: 5/11/12
 * Time: 2:04 PM
 */
public class AccumuloHDFSFileInputFormat extends FileInputFormat<Key, Value> {

    public static final Range ALLRANGE = new Range(new Text("\u0000"), new Text("\uFFFD"));

    @Override
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException {
        //read the params from AccumuloInputFormat
        Configuration conf = jobContext.getConfiguration();
        Instance instance = AccumuloProps.getInstance(jobContext);
        String user = AccumuloProps.getUsername(jobContext);
        AuthenticationToken password = AccumuloProps.getPassword(jobContext);
        String table = AccumuloProps.getTablename(jobContext);
        ArgumentChecker.notNull(instance);
        ArgumentChecker.notNull(table);

        //find the files necessary
        try {
        	AccumuloConfiguration acconf = instance.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            Connector connector = instance.getConnector(user, password);
            TableOperations tos = connector.tableOperations();
            String tableId = tos.tableIdMap().get(table);
            String filePrefix = acconf.get(Property.INSTANCE_DFS_DIR) + "/tables/" + tableId;
            System.out.println(filePrefix);

            Scanner scanner = connector.createScanner("!METADATA", Constants.NO_AUTHS); //TODO: auths?
            scanner.setRange(new Range(new Text(tableId + "\u0000"), new Text(tableId + "\uFFFD")));
            scanner.fetchColumnFamily(new Text("file"));
            List<String> files = new ArrayList<String>();
            List<InputSplit> fileSplits = new ArrayList<InputSplit>();
            Job job = new Job(conf);
            for (Map.Entry<Key, Value> entry : scanner) {
                String file = filePrefix + entry.getKey().getColumnQualifier().toString();
                files.add(file);
                Path path = new Path(file);
                FileStatus fileStatus = fs.getFileStatus(path);
                long len = fileStatus.getLen();
                BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(fileStatus, 0, len);
                fileSplits.add(new FileSplit(path, 0, len, fileBlockLocations[0].getHosts()));
//                FileInputFormat.addInputPath(job, path);
            }
            System.out.println(files);
            return fileSplits;
//            return super.getSplits(job);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public RecordReader<Key, Value> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new RecordReader<Key, Value>() {

            private FileSKVIterator fileSKVIterator;

            @Override
            public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
                FileSplit split = (FileSplit) inputSplit;
                Configuration job = taskAttemptContext.getConfiguration();
                Path file = split.getPath();
//                long start = split.getStart();
//                long length = split.getLength();
                FileSystem fs = file.getFileSystem(job);
//                FSDataInputStream fileIn = fs.open(file);
//                System.out.println(start);
//                if (start != 0L) {
//                    fileIn.seek(start);
//                }
                Instance instance = AccumuloProps.getInstance(taskAttemptContext);

                fileSKVIterator = RFileOperations.getInstance().openReader(file.toString(), ALLRANGE,
                        new HashSet<ByteSequence>(), false, fs, job, instance.getConfiguration());
//                fileSKVIterator = new RFileOperations2().openReader(fileIn, length - start, job);
            }

            @Override
            public boolean nextKeyValue() throws IOException, InterruptedException {
                fileSKVIterator.next();
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
                //To change body of implemented methods use File | Settings | File Templates.
            }
        };
    }

    public static void main(String[] args) {
        try {
            Job job = new Job(new Configuration());
            job.setJarByClass(AccumuloHDFSFileInputFormat.class);
            Configuration conf = job.getConfiguration();
            conf.setBoolean("mapred.map.tasks.speculative.execution", false);
            conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
            AccumuloInputFormat.setConnectorInfo(job, "root", new PasswordToken("secret"));
            AccumuloInputFormat.setInputTableName(job, "l_spo");
            AccumuloInputFormat.setScanAuthorizations(job, Constants.NO_AUTHS);
            AccumuloInputFormat.setZooKeeperInstance(job, "acu13", "stratus25:2181");
            AccumuloInputFormat.setRanges(job, Collections.singleton(ALLRANGE));
            job.setMapperClass(NullMapper.class);
            job.setNumReduceTasks(0);
            job.setOutputFormatClass(NullOutputFormat.class);
            if (args.length == 0) {
                job.setInputFormatClass(AccumuloHDFSFileInputFormat.class);
            } else {
                job.setInputFormatClass(AccumuloInputFormat.class);
            }
            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @SuppressWarnings("rawtypes")
    public static class NullMapper extends Mapper {
        @Override
        protected void map(Object key, Object value, Context context) throws IOException, InterruptedException {

        }
    }
}

