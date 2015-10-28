package mvm.mmrts.rdf.partition.mr;

import com.google.common.io.ByteStreams;
import mvm.mmrts.rdf.partition.mr.transform.SparqlCloudbaseIFJob;

import java.io.FileInputStream;

/**
 * Class MrTstBed
 * Date: Sep 1, 2011
 * Time: 9:18:53 AM
 */
public class MrTstBed {
    public static void main(String[] args) {
        try {
//            String query = "PREFIX tdp: <http://here/2010/tracked-data-provenance/ns#>\n" +
//                    "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
//                    "PREFIX mvmpart: <urn:mvm.mmrts.partition.rdf/08/2011#>\n" +
//                    "SELECT * WHERE\n" +
//                    "{\n" +
//                    "?id tdp:reportedAt ?timestamp. \n" +
//                    "FILTER(mvmpart:timeRange(?id, tdp:reportedAt, 1314898074000 , 1314898374000 , 'XMLDATETIME')).\n" +
//                    "?id tdp:performedBy ?system.\n" +
//                    "?id <http://here/2010/cmv/ns#hasMarkingText> \"U\".\n" +
//                    "?id rdf:type tdp:Sent.\n" +
//                    "} \n";

            FileInputStream fis = new FileInputStream(args[0]);
            String query = new String(ByteStreams.toByteArray(fis));
            fis.close();

//            String query = "PREFIX tdp: <http://here/2010/tracked-data-provenance/ns#>\n" +
//                    "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
//                    "PREFIX mvmpart: <urn:mvm.mmrts.partition.rdf/08/2011#>\n" +
//                    "SELECT * WHERE\n" +
//                    "{\n" +
//                    "?id tdp:reportedAt ?timestamp.\n" +
//                    "FILTER(mvmpart:timeRange(?id, tdp:reportedAt, 1314381770000 , 1314381880000 , 'XMLDATETIME')).\n" +
//                    "?id tdp:performedBy ?system.\n" +
//                    "}";

            new SparqlCloudbaseIFJob("partitionRdf", "root", "password", "stratus", "stratus13:2181", "/temp/queryout", MrTstBed.class, query).run();

//            QueryParser parser = (new SPARQLParserFactory()).getParser();
//            TupleExpr expr = parser.parseQuery(query, "http://www.w3.org/1999/02/22-rdf-syntax-ns#").getTupleExpr();
//            System.out.println(expr);
//
//            final Configuration queryConf = new Configuration();
//            expr.visit(new FilterTimeIndexVisitor(queryConf));
//
//            (new SubjectGroupingOptimizer(queryConf)).optimize(expr, null, null);
//
//            System.out.println(expr);
//
//            //make sure of only one shardlookup
//            expr.visit(new QueryModelVisitorBase<RuntimeException>() {
//                int count = 0;
//
//                @Override
//                public void meetOther(QueryModelNode node) throws RuntimeException {
//                    super.meetOther(node);
//                    count++;
//                    if (count > 1)
//                        throw new IllegalArgumentException("Query can only have one subject-star lookup");
//                }
//            });
//
//            final Job job = new Job(queryConf);
//            job.setJarByClass(MrTstBed.class);
//
//            expr.visit(new QueryModelVisitorBase<RuntimeException>() {
//                @Override
//                public void meetOther(QueryModelNode node) throws RuntimeException {
//                    super.meetOther(node);
//
//                    //set up CloudbaseBatchScannerInputFormat here
//                    if (node instanceof ShardSubjectLookup) {
//                        System.out.println("Lookup: " + node);
//                        try {
//                            new SparqlCloudbaseIFTransformer((ShardSubjectLookup) node, queryConf, job, "partitionRdf",
//                                    "root", "password", "stratus", "stratus13:2181");
//                        } catch (QueryEvaluationException e) {
//                            e.printStackTrace();
//                        }
//                    }
//                }
//            });
//
//            Path outputDir = new Path("/temp/sparql-out/testout");
//            FileSystem dfs = FileSystem.get(outputDir.toUri(), queryConf);
//            if (dfs.exists(outputDir))
//                dfs.delete(outputDir, true);
//
//            FileOutputFormat.setOutputPath(job, outputDir);
//
//            // Submit the job
//            Date startTime = new Date();
//            System.out.println("Job started: " + startTime);
//            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
