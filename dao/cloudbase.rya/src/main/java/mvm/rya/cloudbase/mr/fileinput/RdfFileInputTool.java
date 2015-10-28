package mvm.rya.cloudbase.mr.fileinput;

import cloudbase.core.client.mapreduce.CloudbaseOutputFormat;
import cloudbase.core.data.Mutation;
import cloudbase.core.security.ColumnVisibility;
import mvm.rya.api.RdfCloudTripleStoreConstants;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.domain.utils.RyaStatementWritable;
import mvm.rya.cloudbase.CloudbaseRdfConstants;
import mvm.rya.cloudbase.RyaTableMutationsFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.openrdf.rio.RDFFormat;

import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

import static mvm.rya.cloudbase.mr.utils.MRUtils.*;

/**
 * Do bulk import of rdf files
 * Class RdfFileInputTool
 * Date: May 16, 2011
 * Time: 3:12:16 PM
 */
public class RdfFileInputTool implements Tool {

    private Configuration conf;

    private String userName = "root";
    private String pwd = "password";
    private String instance = "stratus";
    private String zk = "10.40.190.113:2181";
    private String tablePrefix = RdfCloudTripleStoreConstants.TBL_PRFX_DEF;
    private String format = RDFFormat.RDFXML.getName();


    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(), new RdfFileInputTool(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public long runJob(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //faster
        conf.setBoolean("mapred.map.tasks.speculative.execution", false);
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

        zk = conf.get(CB_ZK_PROP, zk);
        instance = conf.get(CB_INSTANCE_PROP, instance);
        userName = conf.get(CB_USERNAME_PROP, userName);
        pwd = conf.get(CB_PWD_PROP, pwd);

        tablePrefix = conf.get(TABLE_PREFIX_PROPERTY, tablePrefix);
        format = conf.get(FORMAT_PROP, format);
        conf.set(FORMAT_PROP, format);

        Job job = new Job(conf);
        job.setJarByClass(RdfFileInputTool.class);

        // set up cloudbase input
        job.setInputFormatClass(RdfFileInputFormat.class);
        RdfFileInputFormat.addInputPath(job, new Path(args[0]));

        // set input output of the particular job
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(RyaStatementWritable.class);
//        job.setOutputKeyClass(LongWritable.class);
//        job.setOutputValueClass(StatementWritable.class);

        job.setOutputFormatClass(CloudbaseOutputFormat.class);
        CloudbaseOutputFormat.setOutputInfo(job, userName, pwd.getBytes(), true, tablePrefix + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX);
        CloudbaseOutputFormat.setZooKeeperInstance(job, instance, zk);

        // set mapper and reducer classes
        job.setMapperClass(StatementToMutationMapper.class);
        job.setNumReduceTasks(0);
//        job.setReducerClass(Reducer.class);

        // set output
//        Path outputDir = new Path("/temp/sparql-out/testout");
//        FileSystem dfs = FileSystem.get(outputDir.toUri(), conf);
//        if (dfs.exists(outputDir))
//            dfs.deleteMutation(outputDir, true);
//
//        FileOutputFormat.setOutputPath(job, outputDir);

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
        runJob(args);
        return 0;
    }

    public static class StatementToMutationMapper extends Mapper<LongWritable, RyaStatementWritable, Text, Mutation> {
        protected String tablePrefix;
        protected Text spo_table;
        protected Text po_table;
        protected Text osp_table;
        private byte[] cv = CloudbaseRdfConstants.EMPTY_CV.getExpression();
        RyaTableMutationsFactory mut = new RyaTableMutationsFactory();

        public StatementToMutationMapper() {
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            tablePrefix = conf.get(TABLE_PREFIX_PROPERTY, RdfCloudTripleStoreConstants.TBL_PRFX_DEF);
            spo_table = new Text(tablePrefix + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX);
            po_table = new Text(tablePrefix + RdfCloudTripleStoreConstants.TBL_PO_SUFFIX);
            osp_table = new Text(tablePrefix + RdfCloudTripleStoreConstants.TBL_OSP_SUFFIX);

            final String cv_s = conf.get(CB_CV_PROP);
            if (cv_s != null)
                cv = cv_s.getBytes();
        }

        @Override
        protected void map(LongWritable key, RyaStatementWritable value, Context context) throws IOException, InterruptedException {
            RyaStatement statement = value.getRyaStatement();
            if (statement.getColumnVisibility() == null) {
                statement.setColumnVisibility(cv);
            }
            Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> mutationMap =
                    mut.serialize(statement);
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
        }

    }
}

