package mvm.mmrts.rdf.partition.mr;

import cloudbase.core.client.ZooKeeperInstance;
import cloudbase.core.util.ArgumentChecker;
import mvm.mmrts.rdf.partition.PartitionSail;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.*;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static mvm.mmrts.rdf.partition.PartitionConstants.*;

/**
 * Class SparqlPartitionStoreInputFormat
 * Date: Oct 28, 2010
 * Time: 11:48:17 AM
 */
public class SparqlPartitionStoreInputFormat extends InputFormat<LongWritable, MapWritable> {

    public static final String PREFIX = "mvm.mmrts.rdf.partition.mr.sparqlinputformat";
    public static final String INPUT_INFO_HAS_BEEN_SET = PREFIX + ".configured";
    public static final String INSTANCE_HAS_BEEN_SET = PREFIX + ".instanceConfigured";
    public static final String USERNAME = PREFIX + ".username";
    public static final String PASSWORD = PREFIX + ".password";

    public static final String INSTANCE_NAME = PREFIX + ".instanceName";
    public static final String ZK = PREFIX + ".zk";

    public static final String STARTTIME = PREFIX + ".starttime";
    public static final String ENDTIME = PREFIX + ".endtime";
    public static final String TABLE = PREFIX + ".table";
    public static final String SHARD_TABLE = PREFIX + ".shardtable";
    public static final String SPARQL_QUERIES_PROP = PREFIX + ".sparql";
    public static final String MR_NUMTHREADS_PROP = PREFIX + ".numthreads";
//    public static final String RANGE_PROP = PREFIX + ".range";
//    public static final String NUM_RANGES_PROP = PREFIX + ".numranges";
//    public static final String TABLE_PREFIX_PROP = PREFIX + ".tablePrefix";
//    public static final String OFFSET_RANGE_PROP = PREFIX + ".offsetrange";

//    public static final String INFER_PROP = PREFIX + ".infer";

    private static final String UTF_8 = "UTF-8";

    private static final ValueFactory vf = ValueFactoryImpl.getInstance();

    static class SparqlInputSplit extends InputSplit implements Writable {

        protected String sparql;
        protected String startTime;
        protected String endTime;
        protected String table;
//        private Long offset;
//        private Long limit;

        private SparqlInputSplit() {
        }

        private SparqlInputSplit(String sparql, String startTime, String endTime, String table) {
            this.sparql = sparql;
            this.startTime = startTime;
            this.endTime = endTime;
            this.table = table;
//            this.offset = offset;
//            this.limit = limit;
        }

        @Override
        public long getLength() throws IOException, InterruptedException {
            return 0;
        }

        @Override
        public String[] getLocations() throws IOException, InterruptedException {
            return new String[]{sparql};
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            boolean startTimeExists = startTime != null;
            dataOutput.writeBoolean(startTimeExists);
            if (startTimeExists)
                dataOutput.writeUTF(startTime);

            boolean endTimeExists = endTime != null;
            dataOutput.writeBoolean(endTimeExists);
            if (endTimeExists)
                dataOutput.writeUTF(endTime);

            dataOutput.writeUTF(table);
            dataOutput.writeUTF(sparql);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            if (dataInput.readBoolean())
                this.startTime = dataInput.readUTF();
            if (dataInput.readBoolean())
                this.endTime = dataInput.readUTF();
            this.table = dataInput.readUTF();
            this.sparql = dataInput.readUTF();
        }
    }

    /**
     * Create a SparqlInputSplit for every sparql query.<br>
     * Separate a single sparql query into numRanges of time ranges. For example,
     * a numRange of 3, with range of 1 day (ms), and 1 query, will have 3 input splits
     * with the same query, however the first range will go from now to a day before, the second
     * will go from the day before to the day before that, the third will go from the two days
     * ago to forever back.
     * <br><br>
     * If the numRanges is not set, or set to 1, the inputsplit can only focus on a certain startTime,
     * ttl. If these are not set, then look at all time.
     *
     * @param job
     * @return
     * @throws java.io.IOException
     * @throws InterruptedException
     */
    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException, InterruptedException {
        validateOptions(job.getConfiguration());
        final Collection<String> queries = getSparqlQueries(job.getConfiguration());
        if (queries == null || queries.size() == 0)
            throw new IOException("Queries cannot be null or empty");

        String startTime_s = getStartTime(job.getConfiguration());
        String endTime_s = getEndTime(job.getConfiguration());

        List<InputSplit> splits = new ArrayList<InputSplit>();
        for (String query : queries) {
            splits.add(new SparqlInputSplit(query, startTime_s, endTime_s, getTable(job.getConfiguration())));
        }
        return splits;
    }

    @Override
    public RecordReader<LongWritable, MapWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {
        return new SparqlResultsRecordReader(taskAttemptContext.getConfiguration());
    }

    protected static String getUsername(Configuration conf) {
        return conf.get(USERNAME);
    }

    /**
     * WARNING: The password is stored in the Configuration and shared with all
     * MapReduce tasks; It is BASE64 encoded to provide a charset safe
     * conversion to a string, and is not intended to be secure.
     */
    protected static String getPassword(Configuration conf) {
        return new String(Base64.decodeBase64(conf.get(PASSWORD, "").getBytes()));
    }

    protected static String getInstance(Configuration conf) {
        return conf.get(INSTANCE_NAME);
    }

    public static void setSparqlQueries(JobContext job, String... queries) {
        if (queries == null || queries.length == 0)
            throw new IllegalArgumentException("Queries cannot be null or empty");

        final Configuration conf = job.getConfiguration();
        setSparqlQueries(conf, queries);
    }

    public static void setSparqlQueries(Configuration conf, String... queries) {
        try {
            Collection<String> qencs = new ArrayList<String>();
            for (String query : queries) {
                final String qenc = URLEncoder.encode(query, UTF_8);
                qencs.add(qenc);
            }
            conf.setStrings(SPARQL_QUERIES_PROP, qencs.toArray(new String[qencs.size()]));
        } catch (UnsupportedEncodingException e) {
            //what to do...
            e.printStackTrace();
        }
    }

    public static Collection<String> getSparqlQueries(Configuration conf) {
        Collection<String> queries = new ArrayList<String>();
        final Collection<String> qencs = conf.getStringCollection(SPARQL_QUERIES_PROP);
        for (String qenc : qencs) {
            queries.add(qenc);
        }
        return queries;
    }

    public static void setLongJob(JobContext job, Long time) {
        Configuration conf = job.getConfiguration();
        //need to make the runtime longer, default 30 min
        time = (time == null) ? 1800000 : time;
        conf.setLong("mapreduce.tasktracker.healthchecker.script.timeout", time);
        conf.set("mapred.child.java.opts", "-Xmx1G");
        conf.setBoolean("mapred.map.tasks.speculative.execution", false);
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
    }

    public static void setInputInfo(JobContext job, String user, byte[] passwd) {
        Configuration conf = job.getConfiguration();
        if (conf.getBoolean(INPUT_INFO_HAS_BEEN_SET, false))
            throw new IllegalStateException("Input info can only be set once per job");
        conf.setBoolean(INPUT_INFO_HAS_BEEN_SET, true);

        ArgumentChecker.notNull(user, passwd);
        conf.set(USERNAME, user);
        conf.set(PASSWORD, new String(Base64.encodeBase64(passwd)));
    }

    public static void setEndTime(JobContext job, String endTime) {
        Configuration conf = job.getConfiguration();
        conf.set(ENDTIME, endTime);
    }

    public static String getEndTime(Configuration conf) {
        return conf.get(ENDTIME);
    }

    public static void setNumThreads(JobContext job, int numThreads) {
        Configuration conf = job.getConfiguration();
        conf.setInt(MR_NUMTHREADS_PROP, numThreads);
    }

    public static int getNumThreads(Configuration conf) {
        return conf.getInt(MR_NUMTHREADS_PROP, -1);
    }

    public static void setTable(JobContext job, String table) {
        Configuration conf = job.getConfiguration();
        conf.set(TABLE, table);
    }

    public static String getTable(Configuration conf) {
        return conf.get(TABLE);
    }

    public static void setShardTable(JobContext job, String table) {
        Configuration conf = job.getConfiguration();
        conf.set(SHARD_TABLE, table);
    }

    public static String getShardTable(Configuration conf) {
        String t = conf.get(SHARD_TABLE);
        return (t != null) ? t : getTable(conf);
    }

    public static void setStartTime(JobContext job, String startTime) {
        Configuration conf = job.getConfiguration();
        conf.set(STARTTIME, startTime);
    }

    public static String getStartTime(Configuration conf) {
        return conf.get(STARTTIME);
    }

    public static void setZooKeeperInstance(JobContext job, String instanceName, String zk) {
        Configuration conf = job.getConfiguration();
        if (conf.getBoolean(INSTANCE_HAS_BEEN_SET, false))
            throw new IllegalStateException("Instance info can only be set once per job");
        conf.setBoolean(INSTANCE_HAS_BEEN_SET, true);

        ArgumentChecker.notNull(instanceName, zk);
        conf.set(INSTANCE_NAME, instanceName);
        conf.set(ZK, zk);
    }

    protected static void validateOptions(Configuration conf) throws IOException {
        if (!conf.getBoolean(INPUT_INFO_HAS_BEEN_SET, false))
            throw new IOException("Input info has not been set.");
        if (!conf.getBoolean(INSTANCE_HAS_BEEN_SET, false))
            throw new IOException("Instance info has not been set.");
        if (conf.getStrings(SPARQL_QUERIES_PROP) == null)
            throw new IOException("Sparql queries have not been set.");
    }

    private class SparqlResultsRecordReader extends RecordReader<LongWritable, MapWritable>
//            implements TupleQueryResultWriter, Runnable
    {

        boolean closed = false;
        long count = 0;
        BlockingQueue<MapWritable> queue = new LinkedBlockingQueue<MapWritable>();
        private Repository repo;
        String query;

        Configuration conf;
        private TupleQueryResult result;
        private RepositoryConnection conn;

        public SparqlResultsRecordReader(Configuration conf) {
            this.conf = conf;
        }

        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

            try {
                validateOptions(conf);

                SparqlInputSplit sis = (SparqlInputSplit) inputSplit;
                this.query = sis.sparql;

                // init RdfCloudTripleStore
                final PartitionSail store = new PartitionSail(new ZooKeeperInstance(getInstance(conf),
                        conf.get(ZK)).getConnector(getUsername(conf), getPassword(conf).getBytes()), getTable(conf), getShardTable(conf));

                repo = new SailRepository(store);
                repo.initialize();

                conn = repo.getConnection();
                query = URLDecoder.decode(query, UTF_8);
                TupleQuery tupleQuery = conn.prepareTupleQuery(
                        QueryLanguage.SPARQL, query);

                if (sis.startTime != null && sis.endTime != null) {
                    tupleQuery.setBinding(START_BINDING, vf.createLiteral(sis.startTime));
                    tupleQuery.setBinding(END_BINDING, vf.createLiteral(sis.endTime));
                }

                int threads = getNumThreads(conf);
                if (threads > 0) {
                    tupleQuery.setBinding(NUMTHREADS_PROP, vf.createLiteral(threads));
                }

                result = tupleQuery.evaluate();
            } catch (Exception e) {
                throw new IOException("Exception occurred opening Repository", e);
            }
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            try {
                return result.hasNext();
            } catch (QueryEvaluationException e) {
                throw new IOException(e);
            }
//            return false;
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return new LongWritable(count++);
        }

        @Override
        public MapWritable getCurrentValue() throws IOException, InterruptedException {
            try {
                if (result.hasNext()) {
                    BindingSet bindingSet = result.next();
                    return transformRow(bindingSet);
                }
                return null;
            } catch (QueryEvaluationException e) {
                throw new IOException(e);
            }
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return (closed) ? (1) : (0);
        }

        @Override
        public void close() throws IOException {
            closed = true;
            try {
                conn.close();
                repo.shutDown();
            } catch (RepositoryException e) {
                throw new IOException("Exception occurred closing Repository", e);
            }
        }

        MapWritable mw = new MapWritable();

        protected MapWritable transformRow(BindingSet bindingSet) {
            mw.clear(); //handle the case of optional bindings. -mbraun
            for (String name : bindingSet.getBindingNames()) {
                final Text key = new Text(name);
                final Text value = new Text(bindingSet.getValue(name).stringValue());
                mw.put(key, value);
            }
            return mw;
        }
    }
}
