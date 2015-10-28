//package mvm.rya.cloudbase.mr.fileinput;
//
//import cloudbase.core.client.Connector;
//import cloudbase.core.client.ZooKeeperInstance;
//import cloudbase.core.client.admin.TableOperations;
//import cloudbase.core.client.mapreduce.CloudbaseFileOutputFormat;
//import cloudbase.core.client.mapreduce.lib.partition.RangePartitioner;
//import cloudbase.core.data.Key;
//import cloudbase.core.data.Value;
//import cloudbase.core.util.TextUtil;
//import com.google.common.base.Preconditions;
//import mvm.rya.api.RdfCloudTripleStoreConstants;
//import mvm.rya.cloudbase.CloudbaseRdfConstants;
//import mvm.rya.cloudbase.RyaTableKeyValues;
//import mvm.rya.cloudbase.mr.utils.MRUtils;
//import mvm.rya.cloudbase.utils.bulk.KeyRangePartitioner;
//import mvm.rya.cloudbase.utils.shard.HashAlgorithm;
//import mvm.rya.cloudbase.utils.shard.HashCodeHashAlgorithm;
//import org.apache.commons.codec.binary.Base64;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.conf.Configured;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
//import org.apache.hadoop.util.Tool;
//import org.apache.hadoop.util.ToolRunner;
//import org.openrdf.model.Resource;
//import org.openrdf.model.Statement;
//import org.openrdf.rio.*;
//
//import java.io.BufferedOutputStream;
//import java.io.IOException;
//import java.io.PrintStream;
//import java.io.StringReader;
//import java.util.ArrayList;
//import java.util.Collection;
//import java.util.Map;
//
//import static com.google.common.base.Preconditions.checkNotNull;
//
///**
//* Take large ntrips files and use MapReduce and Cloudbase
//* Bulk ingest techniques to load into the table in our partition format.
//* Uses a sharded scheme
//* <p/>
//* Input: NTrips file
//* Map:
//* - key : shard row - Text
//* - value : stmt in doc triple format - Text
//* Partitioner: RangePartitioner
//* Reduce:
//* - key : all the entries for each triple - Cloudbase Key
//* Class BulkNtripsInputTool
//* Date: Sep 13, 2011
//* Time: 10:00:17 AM
//*/
//public class ShardedBulkNtripsInputTool extends Configured implements Tool {
//
//    public static final String WORKDIR_PROP = "bulk.n3.workdir";
//    public static final String BULK_N3_NUMSHARD = "bulk.n3.numshard";
//
//    private String userName = "root";
//    private String pwd = "password";
//    private String instance = "stratus";
//    private String zk = "10.40.190.129:2181";
//    private String ttl = null;
//    private String workDirBase = "/temp/bulkcb/work";
//    private String format = RDFFormat.NTRIPLES.getName();
//    private int numShards;
//
//    @Override
//    public int run(final String[] args) throws Exception {
//        final Configuration conf = getConf();
//        try {
//            //conf
//            zk = conf.get(MRUtils.CB_ZK_PROP, zk);
//            ttl = conf.get(MRUtils.CB_TTL_PROP, ttl);
//            instance = conf.get(MRUtils.CB_INSTANCE_PROP, instance);
//            userName = conf.get(MRUtils.CB_USERNAME_PROP, userName);
//            pwd = conf.get(MRUtils.CB_PWD_PROP, pwd);
//            workDirBase = conf.get(WORKDIR_PROP, workDirBase);
//            format = conf.get(MRUtils.FORMAT_PROP, format);
//            String numShards_s = conf.get(BULK_N3_NUMSHARD);
//            Preconditions.checkArgument(numShards_s != null);
//            numShards = Integer.parseInt(numShards_s);
//            conf.set(MRUtils.FORMAT_PROP, format);
//            final String inputDir = args[0];
//
//            ZooKeeperInstance zooKeeperInstance = new ZooKeeperInstance(instance, zk);
//            Connector connector = zooKeeperInstance.getConnector(userName, pwd);
//            TableOperations tableOperations = connector.tableOperations();
//
//            String tablePrefix = conf.get(MRUtils.TABLE_PREFIX_PROPERTY, null);
//            if (tablePrefix != null)
//                RdfCloudTripleStoreConstants.prefixTables(tablePrefix);
//            String[] tables = {tablePrefix + RdfCloudTripleStoreConstants.TBL_OSP_SUFFIX,
//                    tablePrefix + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX,
//                    tablePrefix + RdfCloudTripleStoreConstants.TBL_PO_SUFFIX};
//            Collection<Job> jobs = new ArrayList<Job>();
//            for (final String table : tables) {
//                for (int i = 0; i < numShards; i++) {
//                    final String tableName = table + i;
//                    PrintStream out = null;
//                    try {
//                        String workDir = workDirBase + "/" + tableName;
//                        System.out.println("Loading data into table[" + tableName + "]");
//
//                        Job job = new Job(new Configuration(conf), "Bulk Ingest load data to Generic RDF Table[" + tableName + "]");
//                        job.setJarByClass(this.getClass());
//                        //setting long job
//                        job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);
//                        job.getConfiguration().setBoolean("mapred.reduce.tasks.speculative.execution", false);
//                        job.getConfiguration().set("io.sort.mb", "256");
//                        job.getConfiguration().setBoolean("mapred.compress.map.output", true);
//                        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec"); //TODO: I would like LZO compression
//
//                        job.setInputFormatClass(TextInputFormat.class);
//
//                        job.setMapperClass(ShardedParseNtripsMapper.class);
//                        job.setMapOutputKeyClass(Key.class);
//                        job.setMapOutputValueClass(Value.class);
//
//                        job.setCombinerClass(OutStmtMutationsReducer.class);
//                        job.setReducerClass(OutStmtMutationsReducer.class);
//                        job.setOutputFormatClass(CloudbaseFileOutputFormat.class);
//                        CloudbaseFileOutputFormat.setZooKeeperInstance(job, instance, zk);
//
//                        job.getConfiguration().set(ShardedParseNtripsMapper.TABLE_PROPERTY, tableName);
//                        job.getConfiguration().set(ShardedParseNtripsMapper.SHARD_PROPERTY, i + "");
//
//                        TextInputFormat.setInputPaths(job, new Path(inputDir));
//
//                        FileSystem fs = FileSystem.get(conf);
//                        Path workPath = new Path(workDir);
//                        if (fs.exists(workPath))
//                            fs.deleteMutation(workPath, true);
//
//                        CloudbaseFileOutputFormat.setOutputPath(job, new Path(workDir + "/files"));
//
//                        out = new PrintStream(new BufferedOutputStream(fs.create(new Path(workDir + "/splits.txt"))));
//
//                        if (!tableOperations.exists(tableName))
//                            tableOperations.create(tableName);
//                        Collection<Text> splits = tableOperations.getSplits(tableName, Integer.MAX_VALUE);
//                        for (Text split : splits)
//                            out.println(new String(Base64.encodeBase64(TextUtil.getBytes(split))));
//
//                        job.setNumReduceTasks(splits.size() + 1);
//                        out.close();
//
//                        job.setPartitionerClass(KeyRangePartitioner.class);
//                        RangePartitioner.setSplitFile(job, workDir + "/splits.txt");
//
//                        job.getConfiguration().set(WORKDIR_PROP, workDir);
//
//                        job.submit();
//                        jobs.add(job);
//
//                    } catch (Exception re) {
//                        throw new RuntimeException(re);
//                    } finally {
//                        if (out != null)
//                            out.close();
//                    }
//                }
//            }
//
//            for (Job job : jobs) {
//                while (!job.isComplete()) {
//                    Thread.sleep(1000);
//                }
//            }
//
//            for (String table : tables) {
//                for (int i = 0; i < numShards; i++) {
//                    final String tableName = table + i;
//                    String workDir = workDirBase + "/" + tableName;
//                    tableOperations.importDirectory(
//                            tableName,
//                            workDir + "/files",
//                            workDir + "/failures",
//                            20,
//                            4,
//                            false);
//                }
//            }
//
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//
//        return 0;
//    }
//
//    public static void main(String[] args) {
//        try {
//            ToolRunner.run(new Configuration(), new ShardedBulkNtripsInputTool(), args);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//    /**
//     * input: ntrips format triple
//     * <p/>
//     * output: key: shard row from generator
//     * value: stmt in serialized format (document format)
//     */
//    public static class ShardedParseNtripsMapper extends Mapper<LongWritable, Text, Key, Value> {
//        public static final String TABLE_PROPERTY = "shardedparsentripsmapper.table";
//        public static final String SHARD_PROPERTY = "shardedparsentripsmapper.shard";
//
//        private RDFParser parser;
//        private String rdfFormat;
//        private HashAlgorithm hashAlgorithm = new HashCodeHashAlgorithm();
//        private int shard;
//        private int numShards;
//
//        @Override
//        protected void setup(final Context context) throws IOException, InterruptedException {
//            super.setup(context);
//            Configuration conf = context.getConfiguration();
//            final String table = conf.get(TABLE_PROPERTY);
//            Preconditions.checkNotNull(table, "Set the " + TABLE_PROPERTY + " property on the map reduce job");
//
//            String shard_s = conf.get(SHARD_PROPERTY);
//            Preconditions.checkNotNull(shard_s, "Set the " + SHARD_PROPERTY + " property");
//            shard = Integer.parseInt(shard_s);
//
//            numShards = Integer.parseInt(conf.get(BULK_N3_NUMSHARD));
//
//            final String cv_s = conf.get(MRUtils.CB_CV_PROP);
//            rdfFormat = conf.get(MRUtils.FORMAT_PROP);
//            checkNotNull(rdfFormat, "Rdf format cannot be null");
//
//            parser = Rio.createParser(RDFFormat.valueOf(rdfFormat));
//            parser.setRDFHandler(new RDFHandler() {
//
//                @Override
//                public void startRDF() throws RDFHandlerException {
//
//                }
//
//                @Override
//                public void endRDF() throws RDFHandlerException {
//
//                }
//
//                @Override
//                public void handleNamespace(String s, String s1) throws RDFHandlerException {
//
//                }
//
//                @Override
//                public void handleStatement(Statement statement) throws RDFHandlerException {
//                    try {
//                        Resource subject = statement.getSubject();
//                        if ((hashAlgorithm.hash(subject.stringValue()) % numShards) != shard) {
//                            return;
//                        }
//                        RyaTableKeyValues rdfTableKeyValues = new RyaTableKeyValues(subject, statement.getPredicate(), statement.getObject(), cv_s, statement.getContext()).invoke();
//                        Collection<Map.Entry<Key, Value>> entries = null;
//                        if (table.contains(RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX)) {
//                            entries = rdfTableKeyValues.getSpo();
//                        } else if (table.contains(RdfCloudTripleStoreConstants.TBL_PO_SUFFIX)) {
//                            entries = rdfTableKeyValues.getPo();
//                        } else if (table.contains(RdfCloudTripleStoreConstants.TBL_OSP_SUFFIX)) {
//                            entries = rdfTableKeyValues.getOsp();
//                        } else
//                            throw new IllegalArgumentException("Unrecognized table[" + table + "]");
//
//                        for (Map.Entry<Key, Value> entry : entries) {
//                            context.write(entry.getKey(), entry.getValue());
//                        }
//                    } catch (Exception e) {
//                        throw new RDFHandlerException(e);
//                    }
//                }
//
//                @Override
//                public void handleComment(String s) throws RDFHandlerException {
//
//                }
//            });
//        }
//
//        @Override
//        public void map(LongWritable key, Text value, Context output)
//                throws IOException, InterruptedException {
//            String rdf = value.toString();
//            try {
//                parser.parse(new StringReader(rdf), "");
//            } catch (RDFParseException e) {
//                System.out.println("Line[" + rdf + "] cannot be formatted with format[" + rdfFormat + "]. Exception[" + e.getMessage() + "]");
//            } catch (Exception e) {
//                e.printStackTrace();
//                throw new IOException("Exception occurred parsing triple[" + rdf + "]");
//            }
//        }
//    }
//
//    public static class OutStmtMutationsReducer extends Reducer<Key, Value, Key, Value> {
//
//        public void reduce(Key key, Iterable<Value> values, Context output)
//                throws IOException, InterruptedException {
//            output.write(key, CloudbaseRdfConstants.EMPTY_VALUE);
//        }
//    }
//}
