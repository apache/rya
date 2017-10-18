package org.apache.rya.accumulo.pig;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import com.google.common.base.Preconditions;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRdfEvalStatsDAO;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.rdftriplestore.evaluation.QueryJoinOptimizer;
import org.apache.rya.rdftriplestore.evaluation.RdfCloudTripleStoreEvaluationStatistics;
import org.apache.rya.rdftriplestore.inference.InferenceEngine;
import org.apache.rya.rdftriplestore.inference.InverseOfVisitor;
import org.apache.rya.rdftriplestore.inference.SymmetricPropertyVisitor;
import org.apache.rya.rdftriplestore.inference.TransitivePropertyVisitor;
import org.eclipse.rdf4j.query.algebra.QueryRoot;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.QueryParser;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;

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

/**
 * Created by IntelliJ IDEA.
 * Date: 4/23/12
 * Time: 9:31 AM
 * To change this template use File | Settings | File Templates.
 */
public class SparqlQueryPigEngine {
    private static final Log logger = LogFactory.getLog(SparqlQueryPigEngine.class);

    private String hadoopDir;
    private ExecType execType = ExecType.MAPREDUCE; //default to mapreduce
    private boolean inference = true;
    private boolean stats = true;
    private SparqlToPigTransformVisitor sparqlToPigTransformVisitor;
    private PigServer pigServer;
    private InferenceEngine inferenceEngine = null;
    private RdfCloudTripleStoreEvaluationStatistics rdfCloudTripleStoreEvaluationStatistics;
    private AccumuloRyaDAO ryaDAO;
    AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();

    private AccumuloRdfEvalStatsDAO rdfEvalStatsDAO;

    public AccumuloRdfConfiguration getConf() {
        return conf;
    }

    public void setConf(final AccumuloRdfConfiguration conf) {
        this.conf = conf;
    }

    public void init() throws Exception {
        Preconditions.checkNotNull(sparqlToPigTransformVisitor, "Sparql To Pig Transform Visitor must not be null");
        logger.info("Initializing Sparql Query Pig Engine");
        if (hadoopDir != null) {
            //set hadoop dir property
            System.setProperty("HADOOPDIR", hadoopDir);
        }
        //TODO: Maybe have validation of the HadoopDir system property

        if (pigServer == null) {
            pigServer = new PigServer(execType);
        }

        if (inference || stats) {
            final String instance = sparqlToPigTransformVisitor.getInstance();
            final String zoo = sparqlToPigTransformVisitor.getZk();
            final String user = sparqlToPigTransformVisitor.getUser();
            final String pass = sparqlToPigTransformVisitor.getPassword();

            final Connector connector = new ZooKeeperInstance(instance, zoo).getConnector(user, pass.getBytes(StandardCharsets.UTF_8));

            final String tablePrefix = sparqlToPigTransformVisitor.getTablePrefix();
            conf.setTablePrefix(tablePrefix);
            if (inference) {
                logger.info("Using inference");
                inferenceEngine = new InferenceEngine();
                ryaDAO = new AccumuloRyaDAO();
                ryaDAO.setConf(conf);
                ryaDAO.setConnector(connector);
                ryaDAO.init();

                inferenceEngine.setRyaDAO(ryaDAO);
                inferenceEngine.setConf(conf);
                inferenceEngine.setSchedule(false);
                inferenceEngine.init();
            }
            if (stats) {
                logger.info("Using stats");
                rdfEvalStatsDAO = new AccumuloRdfEvalStatsDAO();
                rdfEvalStatsDAO.setConf(conf);
                rdfEvalStatsDAO.setConnector(connector);
//                rdfEvalStatsDAO.setEvalTable(tablePrefix + RdfCloudTripleStoreConstants.TBL_EVAL_SUFFIX);
                rdfEvalStatsDAO.init();
                rdfCloudTripleStoreEvaluationStatistics = new RdfCloudTripleStoreEvaluationStatistics(conf, rdfEvalStatsDAO);
            }
        }
    }

    public void destroy() throws Exception {
        logger.info("Shutting down Sparql Query Pig Engine");
        pigServer.shutdown();
        if (ryaDAO != null) {
            ryaDAO.destroy();
        }
        if (inferenceEngine != null) {
            inferenceEngine.destroy();
        }
        if (rdfEvalStatsDAO != null) {
            rdfEvalStatsDAO.destroy();
        }
    }

    /**
     * Transform a sparql query into a pig script and execute it. Save results in hdfsSaveLocation
     *
     * @param sparql           to execute
     * @param hdfsSaveLocation to save the execution
     * @throws java.io.IOException
     */
    public void runQuery(final String sparql, final String hdfsSaveLocation) throws IOException {
        Preconditions.checkNotNull(sparql, "Sparql query cannot be null");
        Preconditions.checkNotNull(hdfsSaveLocation, "Hdfs save location cannot be null");
        logger.info("Running query[" + sparql + "]\n to Location[" + hdfsSaveLocation + "]");
        pigServer.deleteFile(hdfsSaveLocation);
        try {
            final String pigScript = generatePigScript(sparql);
            if (logger.isDebugEnabled()) {
                logger.debug("Pig script [" + pigScript + "]");
            }
            pigServer.registerScript(new ByteArrayInputStream(pigScript.getBytes(StandardCharsets.UTF_8)));
            pigServer.store("PROJ", hdfsSaveLocation); //TODO: Make this a constant
        } catch (final Exception e) {
            throw new IOException(e);
        }
    }

    public String generatePigScript(final String sparql) throws Exception {
        Preconditions.checkNotNull(sparql, "Sparql query cannot be null");
        final QueryParser parser = new SPARQLParser();
        final ParsedQuery parsedQuery = parser.parseQuery(sparql, null);
        final QueryRoot tupleExpr = new QueryRoot(parsedQuery.getTupleExpr());

//        SimilarVarJoinOptimizer similarVarJoinOptimizer = new SimilarVarJoinOptimizer();
//        similarVarJoinOptimizer.optimize(tupleExpr, null, null);

        if (inference || stats) {
            if (inference) {
                tupleExpr.visit(new TransitivePropertyVisitor(conf, inferenceEngine));
                tupleExpr.visit(new SymmetricPropertyVisitor(conf, inferenceEngine));
                tupleExpr.visit(new InverseOfVisitor(conf, inferenceEngine));
            }
            if (stats) {
                (new QueryJoinOptimizer(rdfCloudTripleStoreEvaluationStatistics)).optimize(tupleExpr, null, null);
            }
        }

        sparqlToPigTransformVisitor.meet(tupleExpr);
        return sparqlToPigTransformVisitor.getPigScript();
    }


    public static void main(final String[] args) {
        try {
            Preconditions.checkArgument(args.length == 7, "Usage: java -cp <jar>:$PIG_LIB <class> sparqlFile hdfsSaveLocation cbinstance cbzk cbuser cbpassword rdfTablePrefix.\n " +
                    "Sample command: java -cp java -cp cloudbase.pig-2.0.0-SNAPSHOT-shaded.jar:/usr/local/hadoop-etc/hadoop-0.20.2/hadoop-0.20.2-core.jar:/srv_old/hdfs-tmp/pig/pig-0.9.2/pig-0.9.2.jar:$HADOOP_HOME/conf org.apache.rya.accumulo.pig.SparqlQueryPigEngine " +
                    "tstSpqrl.query temp/engineTest stratus stratus13:2181 root password l_");
            final String sparql = FileUtils.readFileToString(new File(args[0]), StandardCharsets.UTF_8);
            final String hdfsSaveLocation = args[1];
            final SparqlToPigTransformVisitor visitor = new SparqlToPigTransformVisitor();
            visitor.setTablePrefix(args[6]);
            visitor.setInstance(args[2]);
            visitor.setZk(args[3]);
            visitor.setUser(args[4]);
            visitor.setPassword(args[5]);

            final SparqlQueryPigEngine engine = new SparqlQueryPigEngine();
            engine.setSparqlToPigTransformVisitor(visitor);
            engine.setInference(false);
            engine.setStats(false);

            engine.init();

            engine.runQuery(sparql, hdfsSaveLocation);

            engine.destroy();
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }

    public String getHadoopDir() {
        return hadoopDir;
    }

    public void setHadoopDir(final String hadoopDir) {
        this.hadoopDir = hadoopDir;
    }

    public PigServer getPigServer() {
        return pigServer;
    }

    public void setPigServer(final PigServer pigServer) {
        this.pigServer = pigServer;
    }

    public ExecType getExecType() {
        return execType;
    }

    public void setExecType(final ExecType execType) {
        this.execType = execType;
    }

    public boolean isInference() {
        return inference;
    }

    public void setInference(final boolean inference) {
        this.inference = inference;
    }

    public boolean isStats() {
        return stats;
    }

    public void setStats(final boolean stats) {
        this.stats = stats;
    }

    public SparqlToPigTransformVisitor getSparqlToPigTransformVisitor() {
        return sparqlToPigTransformVisitor;
    }

    public void setSparqlToPigTransformVisitor(final SparqlToPigTransformVisitor sparqlToPigTransformVisitor) {
        this.sparqlToPigTransformVisitor = sparqlToPigTransformVisitor;
    }
}
