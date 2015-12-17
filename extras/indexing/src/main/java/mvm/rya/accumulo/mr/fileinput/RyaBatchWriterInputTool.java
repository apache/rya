package mvm.rya.accumulo.mr.fileinput;

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



import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.StringReader;

import mvm.rya.accumulo.mr.RyaOutputFormat;
import mvm.rya.accumulo.mr.StatementWritable;
import mvm.rya.accumulo.mr.utils.MRUtils;
import mvm.rya.indexing.accumulo.ConfigUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.ParserConfig;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.Rio;
import org.openrdf.rio.helpers.RDFHandlerBase;

import com.google.common.base.Preconditions;

/**
 * Take large ntrips files and use MapReduce to ingest into other indexing
 */
public class RyaBatchWriterInputTool extends Configured implements Tool {
    private static final Logger logger = Logger.getLogger(RyaBatchWriterInputTool.class);

    @Override
    public int run(final String[] args) throws Exception {
        String userName = null;
        String pwd = null;
        String instance = null;
        String zk = null;
        String format = null;

        final Configuration conf = getConf();
        // conf
        zk = conf.get(MRUtils.AC_ZK_PROP, zk);
        instance = conf.get(MRUtils.AC_INSTANCE_PROP, instance);
        userName = conf.get(MRUtils.AC_USERNAME_PROP, userName);
        pwd = conf.get(MRUtils.AC_PWD_PROP, pwd);
        format = conf.get(MRUtils.FORMAT_PROP,  RDFFormat.NTRIPLES.getName());

        String auths = conf.get(MRUtils.AC_CV_PROP, "");

        conf.set(MRUtils.FORMAT_PROP, format);
        Preconditions.checkNotNull(zk, MRUtils.AC_ZK_PROP + " not set");
        Preconditions.checkNotNull(instance, MRUtils.AC_INSTANCE_PROP + " not set");
        Preconditions.checkNotNull(userName, MRUtils.AC_USERNAME_PROP + " not set");
        Preconditions.checkNotNull(pwd, MRUtils.AC_PWD_PROP + " not set");

        // map the config values to free text configure values
        conf.set(ConfigUtils.CLOUDBASE_ZOOKEEPERS, zk);
        conf.set(ConfigUtils.CLOUDBASE_INSTANCE, instance);
        conf.set(ConfigUtils.CLOUDBASE_USER, userName);
        conf.set(ConfigUtils.CLOUDBASE_PASSWORD, pwd);
        conf.set(ConfigUtils.CLOUDBASE_AUTHS, auths);

        final String inputDir = args[0];

        String tablePrefix = conf.get(MRUtils.TABLE_PREFIX_PROPERTY, null);
        Preconditions.checkNotNull(tablePrefix, MRUtils.TABLE_PREFIX_PROPERTY + " not set");

        String docTextTable = tablePrefix + "text";
        conf.set(ConfigUtils.FREE_TEXT_DOC_TABLENAME, docTextTable);

        String docTermTable = tablePrefix + "terms";
        conf.set(ConfigUtils.FREE_TEXT_TERM_TABLENAME, docTermTable);

        String geoTable = tablePrefix + "geo";
        conf.set(ConfigUtils.GEO_TABLENAME, geoTable);

        logger.info("Loading data into tables[rya, freetext, geo]");
        logger.info("Loading data into tables[" + docTermTable + " " + docTextTable + " " + geoTable + "]");

        Job job = new Job(new Configuration(conf), "Batch Writer load data into Rya Core and Indexing Tables");
        job.setJarByClass(this.getClass());

        // setting long job
        Configuration jobConf = job.getConfiguration();
        jobConf.setBoolean("mapred.map.tasks.speculative.execution", false);

        jobConf.setInt("mapred.task.timeout", 1000 * 60 * 60 * 24); // timeout after 1 day

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(ParseNtripsMapper.class);

        job.setNumReduceTasks(0);
        
        // Use Rya Output Format
        job.setOutputFormatClass(RyaOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(StatementWritable.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(StatementWritable.class);

        TextInputFormat.setInputPaths(job, new Path(inputDir));

        job.waitForCompletion(true);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new RyaBatchWriterInputTool(), args);
    }

    public static class ParseNtripsMapper extends Mapper<LongWritable, Text, Writable, Statement> {
        private static final Logger logger = Logger.getLogger(ParseNtripsMapper.class);

        private RDFParser parser;
        private RDFFormat rdfFormat;

        @Override
        protected void setup(final Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();

            final ValueFactory vf = new ValueFactoryImpl();

            String rdfFormatName = conf.get(MRUtils.FORMAT_PROP);
            checkNotNull(rdfFormatName, "Rdf format cannot be null");
            rdfFormat = RDFFormat.valueOf(rdfFormatName);

            String namedGraphString = conf.get(MRUtils.NAMED_GRAPH_PROP);
            checkNotNull(namedGraphString, MRUtils.NAMED_GRAPH_PROP + " cannot be null");

            final Resource namedGraph = vf.createURI(namedGraphString);

            parser = Rio.createParser(rdfFormat);
            parser.setParserConfig(new ParserConfig(true, true, true, RDFParser.DatatypeHandling.VERIFY));
            parser.setRDFHandler(new RDFHandlerBase() {
                @Override
                public void handleStatement(Statement statement) throws RDFHandlerException {
                    Statement output;
                    if (rdfFormat.equals(RDFFormat.NTRIPLES)) {
                        output = new ConextStatementWrapper(statement, namedGraph);
                    } else {
                        output = statement;
                    }
                    try {
                        context.write(NullWritable.get(), new StatementWritable(output));
                    } catch (IOException e) {
                        logger.error("Error writing statement", e);
                        throw new RDFHandlerException(e);
                    } catch (InterruptedException e) {
                        logger.error("Error writing statement", e);
                        throw new RDFHandlerException(e);
                    }
                }

            });
        }

        @Override
        public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {
            String rdf = value.toString();
            try {
                parser.parse(new StringReader(rdf), "");
            } catch (RDFParseException e) {
                logger.error("Line[" + rdf + "] cannot be formatted with format[" + rdfFormat + "]. Exception[" + e.getMessage()
                        + "]", e);
            } catch (Exception e) {
                logger.error("error during map", e);
                throw new IOException("Exception occurred parsing triple[" + rdf + "]", e);
            }
        }
    }

    @SuppressWarnings("serial")
    private static class ConextStatementWrapper implements Statement {
        private Statement statementWithoutConext;
        private Resource context;

        public ConextStatementWrapper(Statement statementWithoutConext, Resource context) {
            this.statementWithoutConext = statementWithoutConext;
            this.context = context;
        }

        @Override
        public Resource getSubject() {
            return statementWithoutConext.getSubject();
        }

        @Override
        public URI getPredicate() {
            return statementWithoutConext.getPredicate();
        }

        @Override
        public Value getObject() {
            return statementWithoutConext.getObject();
        }

        @Override
        public Resource getContext() {
            return context;
        }
    }

}
