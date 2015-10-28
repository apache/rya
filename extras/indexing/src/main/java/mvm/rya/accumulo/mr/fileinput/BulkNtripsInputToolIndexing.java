package mvm.rya.accumulo.mr.fileinput;

/*
 * #%L
 * mvm.rya.indexing.accumulo
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

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.StringReader;

import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.accumulo.mr.utils.MRUtils;
import mvm.rya.api.resolver.RdfToRyaConversions;
import mvm.rya.indexing.FreeTextIndexer;
import mvm.rya.indexing.GeoIndexer;
import mvm.rya.indexing.accumulo.ConfigUtils;
import mvm.rya.indexing.accumulo.freetext.AccumuloFreeTextIndexer;
import mvm.rya.indexing.accumulo.geo.GeoMesaGeoIndexer;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.geotools.feature.SchemaException;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ContextStatementImpl;
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
public class BulkNtripsInputToolIndexing extends Configured implements Tool {

    private String userName = null;
    private String pwd = null;
    private String instance = null;
    private String zk = null;

    private String format = RDFFormat.NTRIPLES.getName();

    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = getConf();
        // conf
        zk = conf.get(MRUtils.AC_ZK_PROP, zk);
        instance = conf.get(MRUtils.AC_INSTANCE_PROP, instance);
        userName = conf.get(MRUtils.AC_USERNAME_PROP, userName);
        pwd = conf.get(MRUtils.AC_PWD_PROP, pwd);
        format = conf.get(MRUtils.FORMAT_PROP, format);

        String auths = conf.get(MRUtils.AC_CV_PROP, "");

        conf.set(MRUtils.FORMAT_PROP, format);
        Preconditions.checkNotNull(zk, MRUtils.AC_ZK_PROP + " not set");
        Preconditions.checkNotNull(instance, MRUtils.AC_INSTANCE_PROP + " not set");
        Preconditions.checkNotNull(userName, MRUtils.AC_USERNAME_PROP + " not set");
        Preconditions.checkNotNull(pwd, MRUtils.AC_PWD_PROP + " not set");

        // map the config values to free text configu values
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

        System.out.println("Loading data into tables[freetext, geo]");
        System.out.println("Loading data into tables[" + docTermTable + " " + docTextTable + " " + geoTable + "]");

        Job job = new Job(new Configuration(conf), "Bulk Ingest load data into Indexing Tables");
        job.setJarByClass(this.getClass());

        // setting long job
        Configuration jobConf = job.getConfiguration();
        jobConf.setBoolean("mapred.map.tasks.speculative.execution", false);
        jobConf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
        jobConf.set("io.sort.mb", jobConf.get("io.sort.mb", "256"));
        jobConf.setBoolean("mapred.compress.map.output", true);

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(ParseNtripsMapper.class);

        // I'm not actually going to write output.
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(inputDir));

        job.setNumReduceTasks(0);

        job.waitForCompletion(true);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new BulkNtripsInputToolIndexing(), args);
    }

    public static class ParseNtripsMapper extends Mapper<LongWritable, Text, Text, Text> {
        private static final Logger logger = Logger.getLogger(ParseNtripsMapper.class);

        public static final String TABLE_PROPERTY = "parsentripsmapper.table";

        private RDFParser parser;
        private FreeTextIndexer freeTextIndexer;
        private GeoIndexer geoIndexer;
        private String rdfFormat;

        @Override
        protected void setup(final Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();

            freeTextIndexer = new AccumuloFreeTextIndexer();
            freeTextIndexer.setConf(conf);
            geoIndexer = new GeoMesaGeoIndexer();
            geoIndexer.setConf(conf);
            final ValueFactory vf = new ValueFactoryImpl();

            rdfFormat = conf.get(MRUtils.FORMAT_PROP);
            checkNotNull(rdfFormat, "Rdf format cannot be null");

            String namedGraphString = conf.get(MRUtils.NAMED_GRAPH_PROP);
            checkNotNull(namedGraphString, MRUtils.NAMED_GRAPH_PROP + " cannot be null");

            final Resource namedGraph = vf.createURI(namedGraphString);

            parser = Rio.createParser(RDFFormat.valueOf(rdfFormat));
            parser.setParserConfig(new ParserConfig(true, true, true, RDFParser.DatatypeHandling.VERIFY));
            parser.setRDFHandler(new RDFHandlerBase() {

                @Override
                public void handleStatement(Statement statement) throws RDFHandlerException {
                    Statement contextStatement = new ContextStatementImpl(statement.getSubject(), statement
                            .getPredicate(), statement.getObject(), namedGraph);
                    try {
                        freeTextIndexer.storeStatement(RdfToRyaConversions.convertStatement(contextStatement));
                        geoIndexer.storeStatement(RdfToRyaConversions.convertStatement(contextStatement));
                    } catch (IOException e) {
                        logger.error("Error creating indexers", e);
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
                System.out.println("Line[" + rdf + "] cannot be formatted with format[" + rdfFormat + "]. Exception[" + e.getMessage()
                        + "]");
            } catch (Exception e) {
                logger.error("error during map", e);
                throw new IOException("Exception occurred parsing triple[" + rdf + "]");
            }
        }

        @Override
        public void cleanup(Context context) {
            IOUtils.closeStream(freeTextIndexer);
            IOUtils.closeStream(geoIndexer);
        }
    }

}
