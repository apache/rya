package mvm.rya.accumulo.mr.fileinput;

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
import java.io.StringReader;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.accumulo.AccumuloRdfConstants;
import mvm.rya.accumulo.RyaTableMutationsFactory;
import mvm.rya.accumulo.mr.utils.MRUtils;
import mvm.rya.api.RdfCloudTripleStoreConstants;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.resolver.RdfToRyaConversions;
import mvm.rya.api.resolver.RyaTripleContext;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.Rio;

/**
 * Do bulk import of rdf files
 * Class RdfFileInputTool2
 * Date: May 16, 2011
 * Time: 3:12:16 PM
 */
public class RdfFileInputByLineTool implements Tool {

    private Configuration conf = new Configuration();

    private String userName = "root";
    private String pwd = "password";
    private String instance = "instance";
    private String zk = "zoo";
    private String tablePrefix = null;
    private RDFFormat format = RDFFormat.NTRIPLES;

    public Configuration getConf() {
        return conf;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(), new RdfFileInputByLineTool(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public long runJob(String[] args) throws IOException, ClassNotFoundException, InterruptedException, AccumuloSecurityException {
        conf.setBoolean("mapred.map.tasks.speculative.execution", false);
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
        conf.set("io.sort.mb", "256");
        conf.setLong("mapred.task.timeout", 600000000);

        zk = conf.get(MRUtils.AC_ZK_PROP, zk);
        instance = conf.get(MRUtils.AC_INSTANCE_PROP, instance);
        userName = conf.get(MRUtils.AC_USERNAME_PROP, userName);
        pwd = conf.get(MRUtils.AC_PWD_PROP, pwd);
        format = RDFFormat.valueOf(conf.get(MRUtils.FORMAT_PROP, RDFFormat.NTRIPLES.toString()));

        String tablePrefix = conf.get(MRUtils.TABLE_PREFIX_PROPERTY, RdfCloudTripleStoreConstants.TBL_PRFX_DEF);

        Job job = new Job(conf);
        job.setJarByClass(RdfFileInputByLineTool.class);

        // set up cloudbase input
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // set input output of the particular job
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Mutation.class);

        job.setOutputFormatClass(AccumuloOutputFormat.class);
        AccumuloOutputFormat.setConnectorInfo(job, userName, new PasswordToken(pwd.getBytes()));
        AccumuloOutputFormat.setCreateTables(job, true);
        AccumuloOutputFormat.setDefaultTableName(job, tablePrefix + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX);
        AccumuloOutputFormat.setZooKeeperInstance(job, instance, zk);

        // set mapper and reducer classes
        job.setMapperClass(TextToMutationMapper.class);
        job.setNumReduceTasks(0);

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
            return job
                    .getCounters()
                    .findCounter("org.apache.hadoop.mapred.Task$Counter",
                            "REDUCE_OUTPUT_RECORDS").getValue();
        } else {
            System.out.println("Job Failed!!!");
        }

        return -1;
    }

    @Override
    public int run(String[] args) throws Exception {
        return (int) runJob(args);
    }

    public static class TextToMutationMapper extends Mapper<LongWritable, Text, Text, Mutation> {
        protected RDFParser parser;
        private String prefix;
        private RDFFormat rdfFormat;
        protected Text spo_table;
        private Text po_table;
        private Text osp_table;
        private byte[] cv = AccumuloRdfConstants.EMPTY_CV.getExpression();

        public TextToMutationMapper() {
        }

        @Override
        protected void setup(final Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            prefix = conf.get(MRUtils.TABLE_PREFIX_PROPERTY, null);
            if (prefix != null) {
                RdfCloudTripleStoreConstants.prefixTables(prefix);
            }

            spo_table = new Text(prefix + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX);
            po_table = new Text(prefix + RdfCloudTripleStoreConstants.TBL_PO_SUFFIX);
            osp_table = new Text(prefix + RdfCloudTripleStoreConstants.TBL_OSP_SUFFIX);

            final String cv_s = conf.get(MRUtils.AC_CV_PROP);
            if (cv_s != null)
                cv = cv_s.getBytes();

            rdfFormat = RDFFormat.valueOf(conf.get(MRUtils.FORMAT_PROP, RDFFormat.NTRIPLES.toString()));
            parser = Rio.createParser(rdfFormat);
            RyaTripleContext tripleContext = RyaTripleContext.getInstance(new AccumuloRdfConfiguration(conf));
            final RyaTableMutationsFactory mut = new RyaTableMutationsFactory(tripleContext);

            parser.setRDFHandler(new RDFHandler() {

                @Override
                public void startRDF() throws RDFHandlerException {

                }

                @Override
                public void endRDF() throws RDFHandlerException {

                }

                @Override
                public void handleNamespace(String s, String s1) throws RDFHandlerException {

                }

                @Override
                public void handleStatement(Statement statement) throws RDFHandlerException {
                    try {
                        RyaStatement ryaStatement = RdfToRyaConversions.convertStatement(statement);
                        if(ryaStatement.getColumnVisibility() == null) {
                            ryaStatement.setColumnVisibility(cv);
                        }
                        Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> mutationMap =
                                mut.serialize(ryaStatement);
                        Collection<Mutation> spo = mutationMap.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.SPO);
                        Collection<Mutation> po = mutationMap.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.PO);
                        Collection<Mutation> osp = mutationMap.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.OSP);

                        for (Mutation m : spo) {
                            context.write(spo_table, m);
                        }
                        for (Mutation m : po) {
                            context.write(po_table, m);
                        }
                        for (Mutation m : osp) {
                            context.write(osp_table, m);
                        }
                    } catch (Exception e) {
                        throw new RDFHandlerException(e);
                    }
                }

                @Override
                public void handleComment(String s) throws RDFHandlerException {

                }
            });
        }

        @Override
        protected void map(LongWritable key, Text value, final Context context) throws IOException, InterruptedException {
            try {
                parser.parse(new StringReader(value.toString()), "");
            } catch (Exception e) {
                throw new IOException(e);
            }
        }

    }
}

