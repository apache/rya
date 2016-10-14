package org.apache.rya.accumulo.mr.tools;

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
import java.util.Date;

import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRdfConstants;
import org.apache.rya.accumulo.mr.AbstractAccumuloMRTool;
import org.apache.rya.accumulo.mr.MRUtils;
import org.apache.rya.api.RdfCloudTripleStoreConstants;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.resolver.RyaTripleContext;
import org.apache.rya.api.resolver.triple.TripleRow;
import org.apache.rya.api.resolver.triple.TripleRowResolverException;

import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

import com.google.common.collect.Lists;
import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;

/**
 * Count subject, predicate, object. Save in table
 * Class RdfCloudTripleStoreCountTool
 * Date: Apr 12, 2011
 * Time: 10:39:40 AM
 * @deprecated
 */
public class AccumuloRdfCountTool extends AbstractAccumuloMRTool implements Tool {

    public static void main(String[] args) {
        try {

            ToolRunner.run(new Configuration(), new AccumuloRdfCountTool(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * cloudbase props
     */

    @Override
    public int run(String[] strings) throws Exception {
        conf.set(MRUtils.JOB_NAME_PROP, "Gather Evaluation Statistics");

        //initialize
        init();

        Job job = new Job(conf);
        job.setJarByClass(AccumuloRdfCountTool.class);
        setupAccumuloInput(job);

        AccumuloInputFormat.setRanges(job, Lists.newArrayList(new Range(new Text(new byte[]{}), new Text(new byte[]{Byte.MAX_VALUE}))));
        // set input output of the particular job
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Mutation.class);

        // set mapper and reducer classes
        job.setMapperClass(CountPiecesMapper.class);
        job.setCombinerClass(CountPiecesCombiner.class);
        job.setReducerClass(CountPiecesReducer.class);

        String outputTable = MRUtils.getTablePrefix(conf) + RdfCloudTripleStoreConstants.TBL_EVAL_SUFFIX;
        setupAccumuloOutput(job, outputTable);

        // Submit the job
        Date startTime = new Date();
        System.out.println("Job started: " + startTime);
        int exitCode = job.waitForCompletion(true) ? 0 : 1;

        if (exitCode == 0) {
            Date end_time = new Date();
            System.out.println("Job ended: " + end_time);
            System.out.println("The job took "
                    + (end_time.getTime() - startTime.getTime()) / 1000
                    + " seconds.");
            return 0;
        } else {
            System.out.println("Job Failed!!!");
        }

        return -1;
    }

    public static class CountPiecesMapper extends Mapper<Key, Value, Text, LongWritable> {

        public static final byte[] EMPTY_BYTES = new byte[0];
        private RdfCloudTripleStoreConstants.TABLE_LAYOUT tableLayout = RdfCloudTripleStoreConstants.TABLE_LAYOUT.OSP;

        ValueFactoryImpl vf = new ValueFactoryImpl();

        private Text keyOut = new Text();
        private LongWritable valOut = new LongWritable(1);
        private RyaTripleContext ryaContext;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            tableLayout = RdfCloudTripleStoreConstants.TABLE_LAYOUT.valueOf(
                    conf.get(MRUtils.TABLE_LAYOUT_PROP, RdfCloudTripleStoreConstants.TABLE_LAYOUT.OSP.toString()));
            ryaContext = RyaTripleContext.getInstance(new AccumuloRdfConfiguration(conf));
        }

        @Override
        protected void map(Key key, Value value, Context context) throws IOException, InterruptedException {
            try {
                RyaStatement statement = ryaContext.deserializeTriple(tableLayout, new TripleRow(key.getRow().getBytes(), key.getColumnFamily().getBytes(), key.getColumnQualifier().getBytes()));
                //count each piece subject, pred, object

                String subj = statement.getSubject().getData();
                String pred = statement.getPredicate().getData();
//                byte[] objBytes = tripleFormat.getValueFormat().serialize(statement.getObject());
                RyaURI scontext = statement.getContext();
                boolean includesContext = scontext != null;
                String scontext_str = (includesContext) ? scontext.getData() : null;

                ByteArrayDataOutput output = ByteStreams.newDataOutput();
                output.writeUTF(subj);
                output.writeUTF(RdfCloudTripleStoreConstants.SUBJECT_CF);
                output.writeBoolean(includesContext);
                if (includesContext)
                    output.writeUTF(scontext_str);
                keyOut.set(output.toByteArray());
                context.write(keyOut, valOut);

                output = ByteStreams.newDataOutput();
                output.writeUTF(pred);
                output.writeUTF(RdfCloudTripleStoreConstants.PRED_CF);
                output.writeBoolean(includesContext);
                if (includesContext)
                    output.writeUTF(scontext_str);
                keyOut.set(output.toByteArray());
                context.write(keyOut, valOut);
            } catch (TripleRowResolverException e) {
                throw new IOException(e);
            }
        }
    }

    public static class CountPiecesCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {

        private LongWritable valOut = new LongWritable();

        // TODO: can still add up to be large I guess
        // any count lower than this does not need to be saved
        public static final int TOO_LOW = 2;

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0;
            for (LongWritable lw : values) {
                count += lw.get();
            }

            if (count <= TOO_LOW)
                return;

            valOut.set(count);
            context.write(key, valOut);
        }

    }

    public static class CountPiecesReducer extends Reducer<Text, LongWritable, Text, Mutation> {

        Text row = new Text();
        Text cat_txt = new Text();
        Value v_out = new Value();
        ValueFactory vf = new ValueFactoryImpl();

        // any count lower than this does not need to be saved
        public static final int TOO_LOW = 10;
        private String tablePrefix;
        protected Text table;
        private ColumnVisibility cv = AccumuloRdfConstants.EMPTY_CV;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            tablePrefix = context.getConfiguration().get(MRUtils.TABLE_PREFIX_PROPERTY, RdfCloudTripleStoreConstants.TBL_PRFX_DEF);
            table = new Text(tablePrefix + RdfCloudTripleStoreConstants.TBL_EVAL_SUFFIX);
            final String cv_s = context.getConfiguration().get(MRUtils.AC_CV_PROP);
            if (cv_s != null)
                cv = new ColumnVisibility(cv_s);
        }

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0;
            for (LongWritable lw : values) {
                count += lw.get();
            }

            if (count <= TOO_LOW)
                return;

            ByteArrayDataInput badi = ByteStreams.newDataInput(key.getBytes());
            String v = badi.readUTF();
            cat_txt.set(badi.readUTF());

            Text columnQualifier = RdfCloudTripleStoreConstants.EMPTY_TEXT;
            boolean includesContext = badi.readBoolean();
            if (includesContext) {
                columnQualifier = new Text(badi.readUTF());
            }

            row.set(v);
            Mutation m = new Mutation(row);
            v_out.set((count + "").getBytes());
            m.put(cat_txt, columnQualifier, cv, v_out);
            context.write(table, m);
        }

    }
}
