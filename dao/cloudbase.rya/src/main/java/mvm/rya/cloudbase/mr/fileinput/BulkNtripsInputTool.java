package mvm.rya.cloudbase.mr.fileinput;

import cloudbase.core.client.Connector;
import cloudbase.core.client.ZooKeeperInstance;
import cloudbase.core.client.admin.TableOperations;
import cloudbase.core.client.mapreduce.CloudbaseFileOutputFormat;
import cloudbase.core.client.mapreduce.lib.partition.RangePartitioner;
import cloudbase.core.data.Key;
import cloudbase.core.data.Value;
import cloudbase.core.util.TextUtil;
import com.google.common.base.Preconditions;
import mvm.rya.api.RdfCloudTripleStoreConstants;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.domain.RyaURI;
import mvm.rya.api.resolver.RdfToRyaConversions;
import mvm.rya.api.resolver.RyaContext;
import mvm.rya.api.resolver.triple.TripleRow;
import mvm.rya.api.resolver.triple.TripleRowResolver;
import mvm.rya.cloudbase.CloudbaseRdfConstants;
import mvm.rya.cloudbase.mr.utils.MRUtils;
import mvm.rya.cloudbase.utils.bulk.KeyRangePartitioner;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.openrdf.model.Statement;
import org.openrdf.rio.*;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static mvm.rya.cloudbase.CloudbaseRdfUtils.extractValue;
import static mvm.rya.cloudbase.CloudbaseRdfUtils.from;

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
    private String pwd = "password";
    private String instance = "stratus";
    private String zk = "10.40.190.129:2181";
    private String ttl = null;
    private String workDirBase = "/temp/bulkcb/work";
    private String format = RDFFormat.NTRIPLES.getName();

    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = getConf();
        try {
            //conf
            zk = conf.get(MRUtils.CB_ZK_PROP, zk);
            ttl = conf.get(MRUtils.CB_TTL_PROP, ttl);
            instance = conf.get(MRUtils.CB_INSTANCE_PROP, instance);
            userName = conf.get(MRUtils.CB_USERNAME_PROP, userName);
            pwd = conf.get(MRUtils.CB_PWD_PROP, pwd);
            workDirBase = conf.get(WORKDIR_PROP, workDirBase);
            format = conf.get(MRUtils.FORMAT_PROP, format);
            conf.set(MRUtils.FORMAT_PROP, format);
            final String inputDir = args[0];

            ZooKeeperInstance zooKeeperInstance = new ZooKeeperInstance(instance, zk);
            Connector connector = zooKeeperInstance.getConnector(userName, pwd);
            TableOperations tableOperations = connector.tableOperations();

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
                    job.setOutputFormatClass(CloudbaseFileOutputFormat.class);
                    CloudbaseFileOutputFormat.setZooKeeperInstance(job, instance, zk);

                    jobConf.set(ParseNtripsMapper.TABLE_PROPERTY, tableName);

                    TextInputFormat.setInputPaths(job, new Path(inputDir));

                    FileSystem fs = FileSystem.get(conf);
                    Path workPath = new Path(workDir);
                    if (fs.exists(workPath))
                        fs.delete(workPath, true);

                    CloudbaseFileOutputFormat.setOutputPath(job, new Path(workDir + "/files"));

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

            for (String tableName : tables) {
                String workDir = workDirBase + "/" + tableName;
                tableOperations.importDirectory(
                        tableName,
                        workDir + "/files",
                        workDir + "/failures",
                        20,
                        4,
                        false);
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return 0;
    }

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(), new BulkNtripsInputTool(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
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
        private RyaContext ryaContext = RyaContext.getInstance();
        private TripleRowResolver rowResolver = ryaContext.getTripleResolver();

        @Override
        protected void setup(final Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            final String table = conf.get(TABLE_PROPERTY);
            Preconditions.checkNotNull(table, "Set the " + TABLE_PROPERTY + " property on the map reduce job");

            final String cv_s = conf.get(MRUtils.CB_CV_PROP);
            final byte[] cv = cv_s == null ? null : cv_s.getBytes();
            rdfFormat = conf.get(MRUtils.FORMAT_PROP);
            checkNotNull(rdfFormat, "Rdf format cannot be null");

            namedGraph = conf.get(MRUtils.NAMED_GRAPH_PROP);

            parser = Rio.createParser(RDFFormat.valueOf(rdfFormat));
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
                        } 

                        Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT,TripleRow> serialize = rowResolver.serialize(rs);

                        if (table.contains(RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX)) {
                            TripleRow tripleRow = serialize.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.SPO);
                            context.write(
                                    from(tripleRow),
                                    extractValue(tripleRow)
                            );
                        } else if (table.contains(RdfCloudTripleStoreConstants.TBL_PO_SUFFIX)) {
                            TripleRow tripleRow = serialize.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.PO);
                            context.write(
                                    from(tripleRow),
                                    extractValue(tripleRow)
                            );
                        } else if (table.contains(RdfCloudTripleStoreConstants.TBL_OSP_SUFFIX)) {
                            TripleRow tripleRow = serialize.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.OSP);
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
            output.write(key, CloudbaseRdfConstants.EMPTY_VALUE);
        }
    }
}
