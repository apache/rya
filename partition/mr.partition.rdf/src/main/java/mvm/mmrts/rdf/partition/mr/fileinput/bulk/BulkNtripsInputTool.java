package mvm.mmrts.rdf.partition.mr.fileinput.bulk;

import cloudbase.core.client.Connector;
import cloudbase.core.client.Instance;
import cloudbase.core.client.ZooKeeperInstance;
import cloudbase.core.client.mapreduce.CloudbaseFileOutputFormat;
import cloudbase.core.client.mapreduce.lib.partition.RangePartitioner;
import cloudbase.core.data.Key;
import cloudbase.core.data.Value;
import cloudbase.core.util.TextUtil;
import com.google.common.base.Preconditions;
import mvm.rya.cloudbase.utils.bulk.KeyRangePartitioner;
import mvm.mmrts.rdf.partition.shard.DateHashModShardValueGenerator;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.ntriples.NTriplesParserFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringReader;
import java.util.Collection;

import static mvm.mmrts.rdf.partition.PartitionConstants.*;
import static mvm.mmrts.rdf.partition.utils.RdfIO.writeStatement;
import static mvm.mmrts.rdf.partition.utils.RdfIO.writeValue;

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

    private static DateHashModShardValueGenerator generator = new DateHashModShardValueGenerator();
    public static final String BASE_MOD = "baseMod";

    @Override
    public int run(String[] args) throws Exception {
        Preconditions.checkArgument(args.length >= 7, "Usage: hadoop jar jarfile BulkNtripsInputTool <cb instance>" +
                " <zookeepers> <username> <password> <output table> <hdfs ntrips dir> <work dir> (<shard size>)");

        Configuration conf = getConf();
        PrintStream out = null;
        try {
            Job job = new Job(conf, "Bulk Ingest NTrips to Partition RDF");
            job.setJarByClass(this.getClass());

            //setting long job
            job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);
            job.getConfiguration().setBoolean("mapred.reduce.tasks.speculative.execution", false);
            job.getConfiguration().set("io.sort.mb", "256");

            job.setInputFormatClass(TextInputFormat.class);

            job.setMapperClass(ParseNtripsMapper.class);
            job.setMapOutputKeyClass(Key.class);
            job.setMapOutputValueClass(Value.class);

            job.setCombinerClass(OutStmtMutationsReducer.class);
            job.setReducerClass(OutStmtMutationsReducer.class);
            job.setOutputFormatClass(CloudbaseFileOutputFormat.class);
            CloudbaseFileOutputFormat.setZooKeeperInstance(job, args[0], args[1]);

            Instance instance = new ZooKeeperInstance(args[0], args[1]);
            String user = args[2];
            byte[] pass = args[3].getBytes();
            String tableName = args[4];
            String inputDir = args[5];
            String workDir = args[6];
            if(args.length > 7) {
                int baseMod = Integer.parseInt(args[7]);
                generator.setBaseMod(baseMod);
                job.getConfiguration().setInt(BASE_MOD, baseMod);
            }

            Connector connector = instance.getConnector(user, pass);

            TextInputFormat.setInputPaths(job, new Path(inputDir));

            FileSystem fs = FileSystem.get(conf);
            Path workPath = new Path(workDir);
            if (fs.exists(workPath))
                fs.delete(workPath, true);

            CloudbaseFileOutputFormat.setOutputPath(job, new Path(workDir + "/files"));

            out = new PrintStream(new BufferedOutputStream(fs.create(new Path(workDir + "/splits.txt"))));

            Collection<Text> splits = connector.tableOperations().getSplits(tableName, Integer.MAX_VALUE);
            for (Text split : splits)
                out.println(new String(Base64.encodeBase64(TextUtil.getBytes(split))));

            job.setNumReduceTasks(splits.size() + 1);
            out.close();

            job.setPartitionerClass(KeyRangePartitioner.class);
            RangePartitioner.setSplitFile(job, workDir + "/splits.txt");

            job.waitForCompletion(true);

            connector.tableOperations().importDirectory(
                    tableName,
                    workDir + "/files",
                    workDir + "/failures",
                    20,
                    4,
                    false);

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (out != null)
                out.close();
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
        private static final NTriplesParserFactory N_TRIPLES_PARSER_FACTORY = new NTriplesParserFactory();

        private Text outputKey = new Text();
        private Text outputValue = new Text();
        private RDFParser parser;
        private static byte[] EMPTY_BYTE_ARRAY = new byte[0];

        @Override
        protected void setup(final Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            generator.setBaseMod(conf.getInt(BASE_MOD, generator.getBaseMod()));
            parser = N_TRIPLES_PARSER_FACTORY.getParser();
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
//                        byte[] doc_serialized = writeStatement(statement, true);
                        Text shard = new Text(generator.generateShardValue(statement.getSubject()));

                        context.write(new Key(shard, DOC, new Text(writeStatement(statement, true))), EMPTY_VALUE);
                        context.write(new Key(shard, INDEX, new Text(writeStatement(statement, false))), EMPTY_VALUE);
                        //TODO: Wish we didn't have to do this constantly, probably better to just aggregate all subjects and do it once
                        context.write(new Key(new Text(writeValue(statement.getSubject())), shard, EMPTY_TXT), EMPTY_VALUE);

//                        outputKey.set(key);
//                        outputValue.set(doc_serialized);
//                        context.write(outputKey, outputValue);
//                        outputKey.set(writeValue(statement.getSubject()));
//                        outputValue.set(EMPTY_BYTE_ARRAY);
//                        context.write(outputKey, outputValue);
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
            try {
                parser.parse(new StringReader(value.toString()), "");
            } catch (Exception e) {
                throw new IOException("Exception occurred parsing ntrips triple[" + value + "]");
            }
        }
    }

    public static class OutStmtMutationsReducer extends Reducer<Key, Value, Key, Value> {

        public void reduce(Key key, Iterable<Value> values, Context output)
                throws IOException, InterruptedException {
            output.write(key, EMPTY_VALUE);
//            System.out.println(key);
//            for (Value value : values) {
//                System.out.println(value);
            /**
             * Each of these is a triple.
             * 1. format back to statement
             * 2. Output the doc,index key,value pairs for each triple
             */
//                Statement stmt = readStatement(ByteStreams.newDataInput(value.getBytes()), VALUE_FACTORY);
//                output.write(new Key(shardKey, DOC, new Text(writeStatement(stmt, true))), EMPTY_VALUE);
//                output.write(new Key(shardKey, INDEX, new Text(writeStatement(stmt, false))), EMPTY_VALUE);
//                //TODO: Wish we didn't have to do this constantly, probably better to just aggregate all subjects and do it once
//                output.write(new Key(new Text(writeValue(stmt.getSubject())), shardKey, EMPTY_TXT), EMPTY_VALUE);
//            }
        }
    }

    public static class EmbedKeyGroupingComparator implements RawComparator<Text> {

        public EmbedKeyGroupingComparator() {

        }

        @Override
        public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3, int arg4,
                           int arg5) {
            DataInputBuffer n = new DataInputBuffer();

            Text temp1 = new Text();
            Text temp2 = new Text();

            try {
                n.reset(arg0, arg1, arg2);
                temp1.readFields(n);
                n.reset(arg3, arg4, arg5);
                temp2.readFields(n);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                //e.printStackTrace();
                throw new RuntimeException(e);
            }

            return compare(temp1, temp2);
        }

        @Override
        public int compare(Text a1, Text a2) {
            return EmbedKeyRangePartitioner.retrieveEmbedKey(a1).compareTo(EmbedKeyRangePartitioner.retrieveEmbedKey(a2));
        }

    }

    /**
     * Really it does a normal Text compare
     */
    public static class EmbedKeySortComparator implements RawComparator<Text> {

        public EmbedKeySortComparator() {

        }

        @Override
        public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3, int arg4,
                           int arg5) {
            DataInputBuffer n = new DataInputBuffer();

            Text temp1 = new Text();
            Text temp2 = new Text();

            try {
                n.reset(arg0, arg1, arg2);
                temp1.readFields(n);
                n.reset(arg3, arg4, arg5);
                temp2.readFields(n);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                //e.printStackTrace();
                throw new RuntimeException(e);
            }

            return compare(temp1, temp2);
        }

        @Override
        public int compare(Text a1, Text a2) {
            return a1.compareTo(a2);
        }

    }
}
