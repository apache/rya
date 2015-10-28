package mvm.rya.cloudbase.mr.fileinput;

import cloudbase.core.client.mapreduce.CloudbaseOutputFormat;
import cloudbase.core.data.Mutation;
import cloudbase.core.security.ColumnVisibility;
import mvm.rya.api.RdfCloudTripleStoreConstants;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.resolver.RdfToRyaConversions;
import mvm.rya.cloudbase.CloudbaseRdfConstants;
import mvm.rya.cloudbase.RyaTableMutationsFactory;
import mvm.rya.cloudbase.mr.utils.MRUtils;
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
import org.openrdf.rio.*;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

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
    private String instance = "stratus";
    private String zk = "10.40.190.113:2181";
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

    public long runJob(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        conf.setBoolean("mapred.map.tasks.speculative.execution", false);
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
        conf.set("io.sort.mb", "256");
        conf.setLong("mapred.task.timeout", 600000000);

        zk = conf.get(MRUtils.CB_ZK_PROP, zk);
        instance = conf.get(MRUtils.CB_INSTANCE_PROP, instance);
        userName = conf.get(MRUtils.CB_USERNAME_PROP, userName);
        pwd = conf.get(MRUtils.CB_PWD_PROP, pwd);
        format = RDFFormat.valueOf(conf.get(MRUtils.FORMAT_PROP, RDFFormat.NTRIPLES.getName()));

        String tablePrefix = conf.get(MRUtils.TABLE_PREFIX_PROPERTY, RdfCloudTripleStoreConstants.TBL_PRFX_DEF);

        Job job = new Job(conf);
        job.setJarByClass(RdfFileInputByLineTool.class);

        // set up cloudbase input
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // set input output of the particular job
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Mutation.class);
//        job.setOutputKeyClass(LongWritable.class);
//        job.setOutputValueClass(StatementWritable.class);

        job.setOutputFormatClass(CloudbaseOutputFormat.class);
        CloudbaseOutputFormat.setOutputInfo(job, userName, pwd.getBytes(), true, tablePrefix + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX);
        CloudbaseOutputFormat.setZooKeeperInstance(job, instance, zk);

        // set mapper and reducer classes
        job.setMapperClass(TextToMutationMapper.class);
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
        return (int) runJob(args);
    }

    public static class TextToMutationMapper extends Mapper<LongWritable, Text, Text, Mutation> {
        protected RDFParser parser;
        private String prefix;
        private RDFFormat rdfFormat;
        protected Text spo_table;
        private Text po_table;
        private Text osp_table;
        private byte[] cv = CloudbaseRdfConstants.EMPTY_CV.getExpression();

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

            final String cv_s = conf.get(MRUtils.CB_CV_PROP);
            if (cv_s != null)
                cv = cv_s.getBytes();

            rdfFormat = RDFFormat.valueOf(conf.get(MRUtils.FORMAT_PROP, RDFFormat.NTRIPLES.toString()));
            parser = Rio.createParser(rdfFormat);
            final RyaTableMutationsFactory mut = new RyaTableMutationsFactory();

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

