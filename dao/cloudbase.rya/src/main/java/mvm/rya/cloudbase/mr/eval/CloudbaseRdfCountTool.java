package mvm.rya.cloudbase.mr.eval;

import cloudbase.core.CBConstants;
import cloudbase.core.client.mapreduce.CloudbaseInputFormat;
import cloudbase.core.client.mapreduce.CloudbaseOutputFormat;
import cloudbase.core.data.Key;
import cloudbase.core.data.Mutation;
import cloudbase.core.data.Range;
import cloudbase.core.data.Value;
import cloudbase.core.iterators.FilteringIterator;
import cloudbase.core.iterators.filter.AgeOffFilter;
import cloudbase.core.security.Authorizations;
import cloudbase.core.security.ColumnVisibility;
import cloudbase.core.util.Pair;
import com.google.common.collect.Lists;
import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import mvm.rya.api.RdfCloudTripleStoreConstants;
import mvm.rya.api.RdfCloudTripleStoreUtils;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.domain.RyaURI;
import mvm.rya.api.resolver.RyaContext;
import mvm.rya.api.resolver.triple.TripleRow;
import mvm.rya.api.resolver.triple.TripleRowResolverException;
import mvm.rya.cloudbase.CloudbaseRdfConstants;
import mvm.rya.cloudbase.mr.utils.MRUtils;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;

/**
 * Count subject, predicate, object. Save in table
 * Class RdfCloudTripleStoreCountTool
 * Date: Apr 12, 2011
 * Time: 10:39:40 AM
 */
public class CloudbaseRdfCountTool implements Tool {

    public static final String TTL_PROP = "mvm.rya.cloudbase.sail.mr.eval.ttl";

    private Configuration conf;

    public static void main(String[] args) {
        try {

            ToolRunner.run(new Configuration(), new CloudbaseRdfCountTool(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * cloudbase props
     */
    private RdfCloudTripleStoreConstants.TABLE_LAYOUT rdfTableLayout = RdfCloudTripleStoreConstants.TABLE_LAYOUT.OSP;
    private String userName = "root";
    private String pwd = "password";
    private String instance = "stratus";
    private String zk = "10.40.190.113:2181";
    private Authorizations authorizations = CBConstants.NO_AUTHS;
    private String ttl = null;

    @Override
    public int run(String[] strings) throws Exception {
        conf.set(MRUtils.JOB_NAME_PROP, "Gather Evaluation Statistics");

        //conf
        zk = conf.get(MRUtils.CB_ZK_PROP, zk);
        ttl = conf.get(MRUtils.CB_TTL_PROP, ttl);
        instance = conf.get(MRUtils.CB_INSTANCE_PROP, instance);
        userName = conf.get(MRUtils.CB_USERNAME_PROP, userName);
        pwd = conf.get(MRUtils.CB_PWD_PROP, pwd);
        boolean mock = conf.getBoolean(MRUtils.CB_MOCK_PROP, false);
        String tablePrefix = conf.get(MRUtils.TABLE_PREFIX_PROPERTY, null);
        if (tablePrefix != null)
            RdfCloudTripleStoreConstants.prefixTables(tablePrefix);
        rdfTableLayout = RdfCloudTripleStoreConstants.TABLE_LAYOUT.valueOf(
                conf.get(MRUtils.TABLE_LAYOUT_PROP, RdfCloudTripleStoreConstants.TABLE_LAYOUT.OSP.toString()));

        String auth = conf.get(MRUtils.CB_AUTH_PROP);
        if (auth != null)
            authorizations = new Authorizations(auth.split(","));

        conf.setBoolean("mapred.map.tasks.speculative.execution", false);
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
        conf.set("io.sort.mb", "256");
        Job job = new Job(conf);
        job.setJarByClass(CloudbaseRdfCountTool.class);

        //set ttl
        ttl = conf.get(TTL_PROP);

        // set up cloudbase input
        job.setInputFormatClass(CloudbaseInputFormat.class);
        CloudbaseInputFormat.setInputInfo(job, userName, pwd.getBytes(),
                RdfCloudTripleStoreUtils.layoutPrefixToTable(rdfTableLayout, tablePrefix), authorizations);
        CloudbaseInputFormat.setZooKeeperInstance(job, instance, zk);
        Collection<Pair<Text, Text>> columns = new ArrayList<Pair<Text, Text>>();
        //TODO: What about named graphs/contexts here?
//        final Pair pair = new Pair(RdfCloudTripleStoreConstants.INFO_TXT, RdfCloudTripleStoreConstants.INFO_TXT);
//        columns.add(pair);
//        CloudbaseInputFormat.fetchColumns(job, columns);
        if (ttl != null) {
            CloudbaseInputFormat.setIterator(job, 1, FilteringIterator.class.getName(), "filteringIterator");
            CloudbaseInputFormat.setIteratorOption(job, "filteringIterator", "0", AgeOffFilter.class.getName());
            CloudbaseInputFormat.setIteratorOption(job, "filteringIterator", "0.ttl", ttl);
        }

        CloudbaseInputFormat.setRanges(job, Lists.newArrayList(new Range(new Text(new byte[]{}), new Text(new byte[]{Byte.MAX_VALUE}))));

        // set input output of the particular job
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Mutation.class);

        // set mapper and reducer classes
        job.setMapperClass(CountPiecesMapper.class);
        job.setCombinerClass(CountPiecesCombiner.class);
        job.setReducerClass(CountPiecesReducer.class);

        CloudbaseOutputFormat.setOutputInfo(job, userName, pwd.getBytes(), true, tablePrefix + RdfCloudTripleStoreConstants.TBL_EVAL_SUFFIX);
        CloudbaseOutputFormat.setZooKeeperInstance(job, instance, zk);
        job.setOutputFormatClass(CloudbaseOutputFormat.class);

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

    @Override
    public void setConf(Configuration configuration) {
        this.conf = configuration;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    public String getInstance() {
        return instance;
    }

    public void setInstance(String instance) {
        this.instance = instance;
    }

    public String getPwd() {
        return pwd;
    }

    public void setPwd(String pwd) {
        this.pwd = pwd;
    }

    public String getZk() {
        return zk;
    }

    public void setZk(String zk) {
        this.zk = zk;
    }

    public String getTtl() {
        return ttl;
    }

    public void setTtl(String ttl) {
        this.ttl = ttl;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public static class CountPiecesMapper extends Mapper<Key, Value, Text, LongWritable> {

        public static final byte[] EMPTY_BYTES = new byte[0];
        private RdfCloudTripleStoreConstants.TABLE_LAYOUT tableLayout = RdfCloudTripleStoreConstants.TABLE_LAYOUT.OSP;

        ValueFactoryImpl vf = new ValueFactoryImpl();

        private Text keyOut = new Text();
        private LongWritable valOut = new LongWritable(1);
        private RyaContext ryaContext = RyaContext.getInstance();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            tableLayout = RdfCloudTripleStoreConstants.TABLE_LAYOUT.valueOf(
                    conf.get(MRUtils.TABLE_LAYOUT_PROP, RdfCloudTripleStoreConstants.TABLE_LAYOUT.OSP.toString()));
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


                //TODO: Obj in eval stats table?
//                output = ByteStreams.newDataOutput();
//                output.write(objBytes);
//                output.writeByte(RdfCloudTripleStoreConstants.DELIM_BYTE);
//                output.writeUTF(RdfCloudTripleStoreConstants.OBJ_CF);
//                output.writeBoolean(includesContext);
//                if (includesContext)
//                    output.write(scontext_bytes);
//                keyOut.set(output.toByteArray());
//                context.write(keyOut, valOut);
            } catch (TripleRowResolverException e) {
                throw new IOException(e);
            }
        }
    }

    public static class CountPiecesCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {

        private LongWritable valOut = new LongWritable();

        // TODO: can still add up to be larger I guess
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
        private ColumnVisibility cv = CloudbaseRdfConstants.EMPTY_CV;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            tablePrefix = context.getConfiguration().get(MRUtils.TABLE_PREFIX_PROPERTY, RdfCloudTripleStoreConstants.TBL_PRFX_DEF);
            table = new Text(tablePrefix + RdfCloudTripleStoreConstants.TBL_EVAL_SUFFIX);
            final String cv_s = context.getConfiguration().get(MRUtils.CB_CV_PROP);
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