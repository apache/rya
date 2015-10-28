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

import static com.google.common.base.Preconditions.checkNotNull;
import static mvm.rya.accumulo.AccumuloRdfUtils.extractValue;
import static mvm.rya.accumulo.AccumuloRdfUtils.from;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.accumulo.AccumuloRdfConstants;
import mvm.rya.accumulo.mr.utils.MRUtils;
import mvm.rya.api.RdfCloudTripleStoreConstants;
import mvm.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.domain.RyaURI;
import mvm.rya.api.resolver.RdfToRyaConversions;
import mvm.rya.api.resolver.RyaTripleContext;
import mvm.rya.api.resolver.triple.TripleRow;
import mvm.rya.api.resolver.triple.TripleRowResolver;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.core.client.mapreduce.lib.partition.KeyRangePartitioner;
import org.apache.accumulo.core.client.mapreduce.lib.partition.RangePartitioner;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.openrdf.model.Statement;
import org.openrdf.rio.ParserConfig;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.nquads.NQuadsParser;

import com.google.common.base.Preconditions;

/**
 * Take large ntrips files and use MapReduce and Cloudbase
 * Bulk ingest techniques to load into the table in our partition format.
 * <p/>
 * Input: NTrips file
 * Map:
 * - key : shard row - Text
 * - value : stmt in doc triple format - Text
 * Partitioner: RangePartitioner
 * Reduce:
 * - key : all the entries for each triple - Cloudbase Key
 * Class BulkNtripsInputTool
 * Date: Sep 13, 2011
 * Time: 10:00:17 AM
 */
public class BulkNtripsInputTool extends Configured implements Tool {

    public static final String WORKDIR_PROP = "bulk.n3.workdir";

    private String userName = "root";
    private String pwd = "root";
    private String instance = "isntance";
    private String zk = "zoo";
    private String ttl = null;
    private String workDirBase = "/temp/bulkcb/work";
    private String format = RDFFormat.NQUADS.getName();

    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = getConf();
        try {
            //conf
            zk = conf.get(MRUtils.AC_ZK_PROP, zk);
            ttl = conf.get(MRUtils.AC_TTL_PROP, ttl);
            instance = conf.get(MRUtils.AC_INSTANCE_PROP, instance);
            userName = conf.get(MRUtils.AC_USERNAME_PROP, userName);
            pwd = conf.get(MRUtils.AC_PWD_PROP, pwd);
            workDirBase = conf.get(WORKDIR_PROP, workDirBase);
            format = conf.get(MRUtils.FORMAT_PROP, format);
            conf.set(MRUtils.FORMAT_PROP, format);
            final String inputDir = args[0];

            ZooKeeperInstance zooKeeperInstance = new ZooKeeperInstance(instance, zk);
            Connector connector = zooKeeperInstance.getConnector(userName, new PasswordToken(pwd));
            TableOperations tableOperations = connector.tableOperations();
            
            if (conf.get(AccumuloRdfConfiguration.CONF_ADDITIONAL_INDEXERS) != null ) {
                throw new IllegalArgumentException("Cannot use Bulk N Trips tool with Additional Indexers");
            }

            String tablePrefix = conf.get(MRUtils.TABLE_PREFIX_PROPERTY, null);
            if (tablePrefix != null)
                RdfCloudTripleStoreConstants.prefixTables(tablePrefix);
            String[] tables = {tablePrefix + RdfCloudTripleStoreConstants.TBL_OSP_SUFFIX,
                    tablePrefix + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX,
                    tablePrefix + RdfCloudTripleStoreConstants.TBL_PO_SUFFIX};
            Collection<Job> jobs = new ArrayList<Job>();
            for (final String tableName : tables) {
                PrintStream out = null;
                try {
                    String workDir = workDirBase + "/" + tableName;
                    System.out.println("Loading data into table[" + tableName + "]");

                    Job job = new Job(new Configuration(conf), "Bulk Ingest load data to Generic RDF Table[" + tableName + "]");
                    job.setJarByClass(this.getClass());
                    //setting long job
                    Configuration jobConf = job.getConfiguration();
                    jobConf.setBoolean("mapred.map.tasks.speculative.execution", false);
                    jobConf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
                    jobConf.set("io.sort.mb", jobConf.get("io.sort.mb", "256"));
                    jobConf.setBoolean("mapred.compress.map.output", true);
//                    jobConf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec"); //TODO: I would like LZO compression

                    job.setInputFormatClass(TextInputFormat.class);

                    job.setMapperClass(ParseNtripsMapper.class);
                    job.setMapOutputKeyClass(Key.class);
                    job.setMapOutputValueClass(Value.class);

                    job.setCombinerClass(OutStmtMutationsReducer.class);
                    job.setReducerClass(OutStmtMutationsReducer.class);
                    job.setOutputFormatClass(AccumuloFileOutputFormat.class);
                   // AccumuloFileOutputFormat.setZooKeeperInstance(jobConf, instance, zk);

                    jobConf.set(ParseNtripsMapper.TABLE_PROPERTY, tableName);

                    TextInputFormat.setInputPaths(job, new Path(inputDir));

                    FileSystem fs = FileSystem.get(conf);
                    Path workPath = new Path(workDir);
                    if (fs.exists(workPath))
                        fs.delete(workPath, true);

                    //make failures dir
                    Path failures = new Path(workDir, "failures");
                    fs.delete(failures, true);
                    fs.mkdirs(new Path(workDir, "failures"));

                    AccumuloFileOutputFormat.setOutputPath(job, new Path(workDir + "/files"));

                    out = new PrintStream(new BufferedOutputStream(fs.create(new Path(workDir + "/splits.txt"))));

                    if (!tableOperations.exists(tableName))
                        tableOperations.create(tableName);
                    Collection<Text> splits = tableOperations.getSplits(tableName, Integer.MAX_VALUE);
                    for (Text split : splits)
                        out.println(new String(Base64.encodeBase64(TextUtil.getBytes(split))));

                    job.setNumReduceTasks(splits.size() + 1);
                    out.close();

                    job.setPartitionerClass(KeyRangePartitioner.class);
                    RangePartitioner.setSplitFile(job, workDir + "/splits.txt");

                    jobConf.set(WORKDIR_PROP, workDir);

                    job.submit();
                    jobs.add(job);

                } catch (Exception re) {
                    throw new RuntimeException(re);
                } finally {
                    if (out != null)
                        out.close();
                }
            }

            for (Job job : jobs) {
                while (!job.isComplete()) {
                    Thread.sleep(1000);
                }
            }

            for(String tableName : tables) {
                String workDir = workDirBase + "/" + tableName;
                String filesDir = workDir + "/files";
                String failuresDir = workDir + "/failures";
                
                FileSystem fs = FileSystem.get(conf);
                
                //make sure that the "accumulo" user can read/write/execute into these directories this path
                fs.setPermission(new Path(filesDir), new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
                fs.setPermission(new Path(failuresDir), new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
                
                tableOperations.importDirectory(
                        tableName,
                        filesDir,
                        failuresDir,
                        false);
                
            }

        } catch (Exception e ){
            throw new RuntimeException(e);
        }

        return 0;
    }

    public static void main(String[] args) throws Exception {
    	ToolRunner.run(new Configuration(), new BulkNtripsInputTool(), args);
    }

    /**
     * input: ntrips format triple
     * <p/>
     * output: key: shard row from generator
     * value: stmt in serialized format (document format)
     */
    public static class ParseNtripsMapper extends Mapper<LongWritable, Text, Key, Value> {
        public static final String TABLE_PROPERTY = "parsentripsmapper.table";

        private RDFParser parser;
        private String rdfFormat;
        private String namedGraph;
        private RyaTripleContext ryaContext;
        private TripleRowResolver rowResolver;

        @Override
        protected void setup(final Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            final String table = conf.get(TABLE_PROPERTY);
            Preconditions.checkNotNull(table, "Set the " + TABLE_PROPERTY + " property on the map reduce job");
            this.ryaContext = RyaTripleContext.getInstance(new AccumuloRdfConfiguration(conf));
            rowResolver = ryaContext.getTripleResolver();

            final String cv_s = conf.get(MRUtils.AC_CV_PROP);
            final byte[] cv = cv_s == null ? null : cv_s.getBytes();
            rdfFormat = conf.get(MRUtils.FORMAT_PROP);
            checkNotNull(rdfFormat, "Rdf format cannot be null");

            namedGraph = conf.get(MRUtils.NAMED_GRAPH_PROP);

            parser = new NQuadsParser();
    		parser.setParserConfig(new ParserConfig(true, true, true, RDFParser.DatatypeHandling.VERIFY));
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
                        RyaStatement rs = RdfToRyaConversions.convertStatement(statement);
                        if(rs.getColumnVisibility() == null) {
                            rs.setColumnVisibility(cv);
                        }

                    	// Inject the specified context into the statement.
                        if(namedGraph != null){
                            rs.setContext(new RyaURI(namedGraph));
                        } else if (statement.getContext() != null) {
                            rs.setContext(new RyaURI(statement.getContext().toString()));
                        } 

                        Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT,TripleRow> serialize = rowResolver.serialize(rs);

                        if (table.contains(RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX)) {
                            TripleRow tripleRow = serialize.get(TABLE_LAYOUT.SPO);
                            context.write(
                                    from(tripleRow),
                                    extractValue(tripleRow)
                            );
                        } else if (table.contains(RdfCloudTripleStoreConstants.TBL_PO_SUFFIX)) {
                            TripleRow tripleRow = serialize.get(TABLE_LAYOUT.PO);
                            context.write(
                                    from(tripleRow),
                                    extractValue(tripleRow)
                            );
                        } else if (table.contains(RdfCloudTripleStoreConstants.TBL_OSP_SUFFIX)) {
                            TripleRow tripleRow = serialize.get(TABLE_LAYOUT.OSP);
                            context.write(
                                    from(tripleRow),
                                    extractValue(tripleRow)
                            );
                        } else
                            throw new IllegalArgumentException("Unrecognized table[" + table + "]");

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
        public void map(LongWritable key, Text value, Context output)
                throws IOException, InterruptedException {
            String rdf = value.toString();
            try {
                parser.parse(new StringReader(rdf), "");
            } catch (RDFParseException e) {
                System.out.println("Line[" + rdf + "] cannot be formatted with format[" + rdfFormat + "]. Exception[" + e.getMessage() + "]");
            } catch (Exception e) {
                e.printStackTrace();
                throw new IOException("Exception occurred parsing triple[" + rdf + "]");
            }
        }
    }

    public static class OutStmtMutationsReducer extends Reducer<Key, Value, Key, Value> {

        public void reduce(Key key, Iterable<Value> values, Context output)
                throws IOException, InterruptedException {
            output.write(key, AccumuloRdfConstants.EMPTY_VALUE);
        }
    }
}
