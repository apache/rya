package mvm.mmrts.rdf.partition.mr.transform;

import cloudbase.core.util.ArgumentChecker;
import mvm.mmrts.rdf.partition.query.evaluation.FilterTimeIndexVisitor;
import mvm.mmrts.rdf.partition.query.evaluation.SubjectGroupingOptimizer;
import mvm.mmrts.rdf.partition.query.operators.ShardSubjectLookup;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.parser.QueryParser;
import org.openrdf.query.parser.sparql.SPARQLParserFactory;

/**
 * Class SparqlCloudbaseIFJob
 * Date: Sep 1, 2011
 * Time: 6:04:35 PM
 */
public class SparqlCloudbaseIFJob {

    private String[] queries;
    private String table;

    //Cloudbase properties
    private String userName;
    private String pwd;
    private String instance;
    private String zk;
    //

    private Class classOriginal; //Calling class for this job.
    private String outputPath;

    public SparqlCloudbaseIFJob(String table, String userName, String pwd, String instance, String zk,
                                String outputPath, Class classOriginal, String... queries) {
        ArgumentChecker.notNull(queries);
        this.queries = queries;
        this.table = table;
        this.userName = userName;
        this.pwd = pwd;
        this.instance = instance;
        this.zk = zk;
        this.outputPath = outputPath;
        this.classOriginal = classOriginal;
    }

    public String[] run() throws Exception {
        int count = 0;
        outputPath = outputPath + "/results/";
        String[] resultsOut = new String[queries.length];

        for (String query : queries) {
            QueryParser parser = (new SPARQLParserFactory()).getParser();
            TupleExpr expr = parser.parseQuery(query, "http://www.w3.org/1999/02/22-rdf-syntax-ns#").getTupleExpr();

            final Configuration queryConf = new Configuration();
            expr.visit(new FilterTimeIndexVisitor(queryConf));

            (new SubjectGroupingOptimizer(queryConf)).optimize(expr, null, null);

            //make sure of only one shardlookup
            expr.visit(new QueryModelVisitorBase<RuntimeException>() {
                int count = 0;

                @Override
                public void meetOther(QueryModelNode node) throws RuntimeException {
                    super.meetOther(node);
                    count++;
                    if (count > 1)
                        throw new IllegalArgumentException("Query can only have one subject-star lookup");
                }
            });

            final Job job = new Job(queryConf);
            job.setJarByClass(classOriginal);
            job.setJobName("SparqlCloudbaseIFTransformer. Query: " + ((query.length() > 32) ? (query.substring(0, 32)) : (query)));

            expr.visit(new QueryModelVisitorBase<RuntimeException>() {
                @Override
                public void meetOther(QueryModelNode node) throws RuntimeException {
                    super.meetOther(node);

                    //set up CloudbaseBatchScannerInputFormat here
                    if (node instanceof ShardSubjectLookup) {
                        System.out.println("Lookup: " + node);
                        try {
                            new SparqlCloudbaseIFTransformer((ShardSubjectLookup) node, queryConf, job, table,
                                    userName, pwd, instance, zk);
                        } catch (QueryEvaluationException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });


            String resultOutPath = outputPath + "/result-" + count;
            resultsOut[count] = resultOutPath;
            Path outputDir = new Path(resultOutPath);
            FileSystem dfs = FileSystem.get(outputDir.toUri(), queryConf);
            if (dfs.exists(outputDir))
                dfs.delete(outputDir, true);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            SequenceFileOutputFormat.setOutputPath(job, outputDir);


            // Submit the job
            job.waitForCompletion(true);
            count++;
        }
        return resultsOut;
    }
}
