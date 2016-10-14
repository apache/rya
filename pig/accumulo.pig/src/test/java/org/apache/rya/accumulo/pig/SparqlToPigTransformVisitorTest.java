package org.apache.rya.accumulo.pig;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */



import junit.framework.TestCase;
import org.apache.rya.accumulo.pig.optimizer.SimilarVarJoinOptimizer;
import org.openrdf.query.algebra.QueryRoot;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.QueryParser;
import org.openrdf.query.parser.sparql.SPARQLParser;

/**
 * Created by IntelliJ IDEA.
 * Date: 4/12/12
 * Time: 10:18 AM
 * To change this template use File | Settings | File Templates.
 */
public class SparqlToPigTransformVisitorTest extends TestCase {

    private String zk;
    private String instance;
    private String tablePrefix;
    private String user;
    private String password;

    protected void setUp() throws Exception {
        super.setUp();
        zk = "zoo";
        instance = "instance";
        tablePrefix = "l_";
        user = "root";
        password = "root";
    }

    public void testStatementPattern() throws Exception {
        String query = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                " PREFIX ub: <urn:lubm:rdfts#>\n" +
                "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                " SELECT * WHERE\n" +
                " {\n" +
                "\t?x rdf:type ub:UndergraduateStudent\n" +
                " }\n" +
                "";
        QueryParser parser = new SPARQLParser();
        ParsedQuery parsedQuery = parser.parseQuery(query, null);

//        System.out.println(parsedQuery);

        SparqlToPigTransformVisitor visitor = new SparqlToPigTransformVisitor();
        visitor.setTablePrefix(tablePrefix);
        visitor.setInstance(instance);
        visitor.setZk(zk);
        visitor.setUser(user);
        visitor.setPassword(password);
        visitor.meet(new QueryRoot(parsedQuery.getTupleExpr()));
//        System.out.println(visitor.getPigScript());
    }

    public void testStatementPatternContext() throws Exception {
        String query = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                " PREFIX ub: <urn:lubm:rdfts#>\n" +
                "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                " SELECT * WHERE\n" +
                " {\n" +
                " GRAPH ub:g1 {\n" +
                "\t?x rdf:type ub:UndergraduateStudent\n" +
                " }\n" +
                " }\n" +
                "";
        QueryParser parser = new SPARQLParser();
        ParsedQuery parsedQuery = parser.parseQuery(query, null);

//        System.out.println(parsedQuery);

        SparqlToPigTransformVisitor visitor = new SparqlToPigTransformVisitor();
        visitor.setTablePrefix(tablePrefix);
        visitor.setInstance(instance);
        visitor.setZk(zk);
        visitor.setUser(user);
        visitor.setPassword(password);
        visitor.meet(new QueryRoot(parsedQuery.getTupleExpr()));
//        System.out.println(visitor.getPigScript());
    }

    public void testStatementPatternContextVar() throws Exception {
        String query = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                " PREFIX ub: <urn:lubm:rdfts#>\n" +
                "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                " SELECT * WHERE\n" +
                " {\n" +
                " GRAPH ?g {\n" +
                "\t?x rdf:type ub:UndergraduateStudent\n" +
                " }\n" +
                " ?x ub:pred ?g." +
                " }\n" +
                "";
        QueryParser parser = new SPARQLParser();
        ParsedQuery parsedQuery = parser.parseQuery(query, null);

//        System.out.println(parsedQuery);

        SparqlToPigTransformVisitor visitor = new SparqlToPigTransformVisitor();
        visitor.setTablePrefix(tablePrefix);
        visitor.setInstance(instance);
        visitor.setZk(zk);
        visitor.setUser(user);
        visitor.setPassword(password);
        visitor.meet(new QueryRoot(parsedQuery.getTupleExpr()));
//        System.out.println(visitor.getPigScript());
    }

    public void testJoin() throws Exception {
        String query = "select * where {\n" +
                "?subj <urn:lubm:rdfts#name> 'Department0'.\n" +
                "?subj <urn:lubm:rdfts#subOrganizationOf> <http://www.University0.edu>.\n" +
                "}";
//        System.out.println(query);
        QueryParser parser = new SPARQLParser();
        ParsedQuery parsedQuery = parser.parseQuery(query, null);

//        System.out.println(parsedQuery);

        SparqlToPigTransformVisitor visitor = new SparqlToPigTransformVisitor();
        visitor.setTablePrefix(tablePrefix);
        visitor.setInstance(instance);
        visitor.setZk(zk);
        visitor.setUser(user);
        visitor.setPassword(password);
        visitor.meet(new QueryRoot(parsedQuery.getTupleExpr()));
//        System.out.println(visitor.getPigScript());
    }

    public void testMutliReturnJoin() throws Exception {
        String query = "select * where {\n" +
                "?subj <urn:lubm:rdfts#name> 'Department0'.\n" +
                "?subj <urn:lubm:rdfts#subOrganizationOf> ?suborg.\n" +
                "}";
        QueryParser parser = new SPARQLParser();
        ParsedQuery parsedQuery = parser.parseQuery(query, null);

        System.out.println(query);

        SparqlToPigTransformVisitor visitor = new SparqlToPigTransformVisitor();
        visitor.setTablePrefix(tablePrefix);
        visitor.setInstance(instance);
        visitor.setZk(zk);
        visitor.setUser(user);
        visitor.setPassword(password);
        visitor.meet(new QueryRoot(parsedQuery.getTupleExpr()));
        System.out.println(visitor.getPigScript());
    }

    public void testMutlipleJoins() throws Exception {
        String query = "select * where {\n" +
                "?subj <urn:lubm:rdfts#name> 'Department0'.\n" +
                "?subj <urn:lubm:rdfts#subOrganizationOf> <http://www.University0.edu>.\n" +
                "?subj <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <urn:lubm:rdfts#Department>.\n" +
                "}";
//        System.out.println(query);
        QueryParser parser = new SPARQLParser();
        ParsedQuery parsedQuery = parser.parseQuery(query, null);

//        System.out.println(parsedQuery);

        SparqlToPigTransformVisitor visitor = new SparqlToPigTransformVisitor();
        visitor.setTablePrefix(tablePrefix);
        visitor.setInstance(instance);
        visitor.setZk(zk);
        visitor.setUser(user);
        visitor.setPassword(password);
        visitor.meet(new QueryRoot(parsedQuery.getTupleExpr()));
//        System.out.println(visitor.getPigScript());
    }

    public void testCross() throws Exception {
        String query = "select * where {\n" +
                "?subj0 <urn:lubm:rdfts#name> 'Department0'.\n" +
                "?subj1 <urn:lubm:rdfts#name> 'Department1'.\n" +
                "?subj0 <urn:lubm:rdfts#subOrganizationOf> <http://www.University0.edu>.\n" +
                "?subj1 <urn:lubm:rdfts#subOrganizationOf> <http://www.University0.edu>.\n" +
                "}";
//        System.out.println(query);
        QueryParser parser = new SPARQLParser();
        ParsedQuery parsedQuery = parser.parseQuery(query, null);
        QueryRoot tupleExpr = new QueryRoot(parsedQuery.getTupleExpr());

        SimilarVarJoinOptimizer similarVarJoinOptimizer = new SimilarVarJoinOptimizer();
        similarVarJoinOptimizer.optimize(tupleExpr, null, null);

//        System.out.println(tupleExpr);

        SparqlToPigTransformVisitor visitor = new SparqlToPigTransformVisitor();
        visitor.setTablePrefix(tablePrefix);
        visitor.setInstance(instance);
        visitor.setZk(zk);
        visitor.setUser(user);
        visitor.setPassword(password);
        visitor.meet(tupleExpr);
//        System.out.println(visitor.getPigScript());
    }

    public void testLimit() throws Exception {
        String query = "select * where {\n" +
                "?subj <urn:lubm:rdfts#name> 'Department0'.\n" +
                "?subj <urn:lubm:rdfts#subOrganizationOf> ?suborg.\n" +
                "} limit 100";
//        System.out.println(query);
        QueryParser parser = new SPARQLParser();
        ParsedQuery parsedQuery = parser.parseQuery(query, null);

//        System.out.println(parsedQuery);

        SparqlToPigTransformVisitor visitor = new SparqlToPigTransformVisitor();
        visitor.setTablePrefix(tablePrefix);
        visitor.setInstance(instance);
        visitor.setZk(zk);
        visitor.setUser(user);
        visitor.setPassword(password);
        visitor.meet(new QueryRoot(parsedQuery.getTupleExpr()));
//        System.out.println(visitor.getPigScript());
    }

    public void testHardQuery() throws Exception {
        String query = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                " PREFIX ub: <urn:lubm:rdfts#>\n" +
                " SELECT * WHERE\n" +
                " {\n" +
                "        ?y rdf:type ub:University .\n" +
                "        ?z ub:subOrganizationOf ?y .\n" +
                "        ?z rdf:type ub:Department .\n" +
                "        ?x ub:memberOf ?z .\n" +
                "        ?x ub:undergraduateDegreeFrom ?y .\n" +
                "       ?x rdf:type ub:GraduateStudent .\n" +
                " }\n" +
                "limit 100";
//        String query = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
//                " PREFIX ub: <urn:lubm:rdfts#>\n" +
//                " SELECT * WHERE\n" +
//                " {\n" +
//                "\t?x ub:advisor ?y.\n" +
//                "\t?y ub:teacherOf ?z.\n" +
//                "\t?x ub:takesCourse ?z.\n" +
//                "\t?x rdf:type ub:Student.\n" +
//                "\t?y rdf:type ub:Faculty.\n" +
//                "\t?z rdf:type ub:Course.\n" +
//                " }\n" +
//                "limit 100";
//        System.out.println(query);
        QueryParser parser = new SPARQLParser();
        ParsedQuery parsedQuery = parser.parseQuery(query, null);

        TupleExpr tupleExpr = parsedQuery.getTupleExpr();

//        CloudbaseRdfEvalStatsDAO rdfEvalStatsDAO = new CloudbaseRdfEvalStatsDAO();
//        rdfEvalStatsDAO.setConnector(new ZooKeeperInstance("stratus", "stratus13:2181").getConnector("root", "password".getBytes()));
//        rdfEvalStatsDAO.setEvalTable("l_eval");
//        RdfCloudTripleStoreEvaluationStatistics stats = new RdfCloudTripleStoreEvaluationStatistics(new Configuration(), rdfEvalStatsDAO);
//        (new SimilarVarJoinOptimizer(stats)).optimize(tupleExpr, null, null);

//        System.out.println(tupleExpr);

        SparqlToPigTransformVisitor visitor = new SparqlToPigTransformVisitor();
        visitor.setTablePrefix(tablePrefix);
        visitor.setInstance(instance);
        visitor.setZk(zk);
        visitor.setUser(user);
        visitor.setPassword(password);
        visitor.meet(new QueryRoot(tupleExpr));
        //System.out.println(visitor.getPigScript());
    }

    public void testFixedStatementPatternInferenceQuery() throws Exception {
        String query = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                " PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                " PREFIX ub: <urn:lubm:rdfts#>\n" +
                " SELECT * WHERE\n" +
                " {\n" +
                "      ?y ub:memberOf <http://www.Department3.University10.edu>.\n" +
                "      {?y rdf:type ub:Professor.}\n" +
                "       UNION \n" +
                "      {?y rdf:type ub:GraduateStudent.}\n" +
                " }";
//        System.out.println(query);
        QueryParser parser = new SPARQLParser();
        ParsedQuery parsedQuery = parser.parseQuery(query, null);

        TupleExpr tupleExpr = parsedQuery.getTupleExpr();

//        Configuration conf = new Configuration();
//        Connector connector = new ZooKeeperInstance("stratus", "stratus13:2181").getConnector("root", "password".getBytes());
//
//        InferenceEngine inferenceEngine = new InferenceEngine();
//        CloudbaseRdfDAO rdfDAO = new CloudbaseRdfDAO();
//        rdfDAO.setConf(conf);
//        rdfDAO.setConnector(connector);
//        rdfDAO.setNamespaceTable("l_ns");
//        rdfDAO.setSpoTable("l_spo");
//        rdfDAO.setPoTable("l_po");
//        rdfDAO.setOspTable("l_osp");
//        rdfDAO.init();
//
//        inferenceEngine.setRdfDao(rdfDAO);
//        inferenceEngine.setConf(conf);
//        inferenceEngine.init();
//
//        tupleExpr.visit(new TransitivePropertyVisitor(conf, inferenceEngine));
//        tupleExpr.visit(new SymmetricPropertyVisitor(conf, inferenceEngine));
//        tupleExpr.visit(new InverseOfVisitor(conf, inferenceEngine));
//        tupleExpr.visit(new SubPropertyOfVisitor(conf, inferenceEngine));
//        tupleExpr.visit(new SubClassOfVisitor(conf, inferenceEngine));
//
//        CloudbaseRdfEvalStatsDAO rdfEvalStatsDAO = new CloudbaseRdfEvalStatsDAO();
//        rdfEvalStatsDAO.setConnector(connector);
//        rdfEvalStatsDAO.setEvalTable("l_eval");
//        RdfCloudTripleStoreEvaluationStatistics stats = new RdfCloudTripleStoreEvaluationStatistics(conf, rdfEvalStatsDAO);
//        (new QueryJoinOptimizer(stats)).optimize(tupleExpr, null, null);

//        System.out.println(tupleExpr);

        SparqlToPigTransformVisitor visitor = new SparqlToPigTransformVisitor();
        visitor.setTablePrefix(tablePrefix);
        visitor.setInstance(instance);
        visitor.setZk(zk);
        visitor.setUser(user);
        visitor.setPassword(password);
        visitor.meet(new QueryRoot(tupleExpr));
//        System.out.println(visitor.getPigScript());
    }

//    public void testInverseOf() throws Exception {
//        String query = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
//                " PREFIX ub: <urn:lubm:rdfts#>\n" +
//                " SELECT * WHERE\n" +
//                " {\n" +
//                "     ?x rdf:type ub:Person .\n" +
//                "     <http://www.University0.edu> ub:hasAlumnus ?x .\n" +
//                " } ";
//        System.out.println(query);
//        QueryParser parser = new SPARQLParser();
//        ParsedQuery parsedQuery = parser.parseQuery(query, null);
//        TupleExpr tupleExpr = parsedQuery.getTupleExpr();
//
//        Configuration conf = new Configuration();
//        Connector connector = new ZooKeeperInstance("stratus", "stratus13:2181").getConnector("root", "password".getBytes());
//
//        InferenceEngine inferenceEngine = new InferenceEngine();
//        CloudbaseRdfDAO rdfDAO = new CloudbaseRdfDAO();
//        rdfDAO.setConf(conf);
//        rdfDAO.setConnector(connector);
//        rdfDAO.setNamespaceTable("l_ns");
//        rdfDAO.setSpoTable("l_spo");
//        rdfDAO.setPoTable("l_po");
//        rdfDAO.setOspTable("l_osp");
//        rdfDAO.init();
//
//        inferenceEngine.setRdfDao(rdfDAO);
//        inferenceEngine.setConf(conf);
//        inferenceEngine.init();
//
//        tupleExpr.visit(new TransitivePropertyVisitor(conf, inferenceEngine));
//        tupleExpr.visit(new SymmetricPropertyVisitor(conf, inferenceEngine));
//        tupleExpr.visit(new InverseOfVisitor(conf, inferenceEngine));
//
//        CloudbaseRdfEvalStatsDAO rdfEvalStatsDAO = new CloudbaseRdfEvalStatsDAO();
//        rdfEvalStatsDAO.setConnector(connector);
//        rdfEvalStatsDAO.setEvalTable("l_eval");
//        RdfCloudTripleStoreEvaluationStatistics stats = new RdfCloudTripleStoreEvaluationStatistics(conf, rdfEvalStatsDAO);
//        (new QueryJoinOptimizer(stats)).optimize(tupleExpr, null, null);
//
//
//        System.out.println(tupleExpr);
//
//        SparqlToPigTransformVisitor visitor = new SparqlToPigTransformVisitor();
//        visitor.setTablePrefix("l_");
//        visitor.setInstance("stratus");
//        visitor.setZk("stratus13:2181");
//        visitor.setUser("root");
//        visitor.setPassword("password");
//        visitor.meet(new QueryRoot(parsedQuery.getTupleExpr()));
//        System.out.println(visitor.getPigScript());
//    }
}
