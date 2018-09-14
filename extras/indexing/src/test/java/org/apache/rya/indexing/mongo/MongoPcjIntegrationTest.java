/**
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
package org.apache.rya.indexing.mongo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.rya.indexing.IndexPlanValidator.IndexPlanValidator;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.external.PcjIntegrationTestingUtil;
import org.apache.rya.indexing.external.PrecomputedJoinIndexerConfig;
import org.apache.rya.indexing.external.PrecomputedJoinIndexerConfig.PrecomputedJoinStorageType;
import org.apache.rya.indexing.external.PrecomputedJoinIndexerConfig.PrecomputedJoinUpdaterType;
import org.apache.rya.indexing.external.tupleSet.ExternalTupleSet;
import org.apache.rya.indexing.mongodb.pcj.MongoPcjIndexSetProvider;
import org.apache.rya.indexing.mongodb.pcj.MongoPcjQueryNode;
import org.apache.rya.indexing.pcj.matching.PCJOptimizer;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;
import org.apache.rya.mongodb.MongoRyaITBase;
import org.apache.rya.mongodb.StatefulMongoDBRdfConfiguration;
import org.apache.rya.sail.config.RyaSailFactory;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.QueryResultHandlerException;
import org.eclipse.rdf4j.query.TupleQueryResultHandler;
import org.eclipse.rdf4j.query.TupleQueryResultHandlerException;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.sail.Sail;
import org.junit.Test;

import com.google.common.collect.Lists;

public class MongoPcjIntegrationTest extends MongoRyaITBase {
    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    private static final IRI talksTo = VF.createIRI("uri:talksTo");
    private static final IRI sub = VF.createIRI("uri:entity");
    private static final IRI sub2 = VF.createIRI("uri:entity2");
    private static final IRI subclass = VF.createIRI("uri:class");
    private static final IRI subclass2 = VF.createIRI("uri:class2");
    private static final IRI obj = VF.createIRI("uri:obj");
    private static final IRI obj2 = VF.createIRI("uri:obj2");

    private void addPCJS(final SailRepositoryConnection conn) throws Exception {
        conn.add(sub, RDF.TYPE, subclass);
        conn.add(sub, RDFS.LABEL, VF.createLiteral("label"));
        conn.add(sub, talksTo, obj);

        conn.add(sub2, RDF.TYPE, subclass2);
        conn.add(sub2, RDFS.LABEL, VF.createLiteral("label2"));
        conn.add(sub2, talksTo, obj2);
    }

    @Override
    protected void updateConfiguration(final MongoDBRdfConfiguration conf) {
        conf.set(PrecomputedJoinIndexerConfig.PCJ_STORAGE_TYPE, PrecomputedJoinStorageType.MONGO.name());
        conf.set(PrecomputedJoinIndexerConfig.PCJ_UPDATER_TYPE, PrecomputedJoinUpdaterType.NO_UPDATE.name());
    }

    @Test
    public void testEvaluateSingleIndex() throws Exception {
        final Sail nonPcjSail = RyaSailFactory.getInstance(conf);
        final MongoDBRdfConfiguration pcjConf = conf.clone();
        pcjConf.setBoolean(ConfigUtils.USE_PCJ, true);
        final Sail pcjSail = RyaSailFactory.getInstance(pcjConf);
        final SailRepositoryConnection conn = new SailRepository(nonPcjSail).getConnection();
        final SailRepositoryConnection pcjConn = new SailRepository(pcjSail).getConnection();
        addPCJS(pcjConn);
        try {
            final String indexSparqlString = ""//
                    + "SELECT ?e ?l ?c " //
                    + "{" //
                    + "  ?e a ?c . "//
                    + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                    + "}";//

            PcjIntegrationTestingUtil.createAndPopulatePcj(conn, getMongoClient(), conf.getRyaInstanceName() + 1, conf.getRyaInstanceName(), indexSparqlString);

            final String queryString = ""//
                    + "SELECT ?e ?c ?l ?o " //
                    + "{" //
                    + "  ?e a ?c . "//
                    + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
                    + "  ?e <uri:talksTo> ?o . "//
                    + "}";//

            final CountingResultHandler crh1 = new CountingResultHandler();
            final CountingResultHandler crh2 = new CountingResultHandler();

            conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh1);
            pcjConn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh2);

            assertEquals(crh1.getCount(), crh2.getCount());
        } finally {
            conn.close();
            pcjConn.close();
            nonPcjSail.shutDown();
            pcjSail.shutDown();
        }
    }

    @Test
    public void testEvaluateOneIndex() throws Exception {
        final Sail nonPcjSail = RyaSailFactory.getInstance(conf);
        final MongoDBRdfConfiguration pcjConf = conf.clone();
        pcjConf.setBoolean(ConfigUtils.USE_PCJ, true);
        final Sail pcjSail = RyaSailFactory.getInstance(pcjConf);
        final SailRepositoryConnection conn = new SailRepository(nonPcjSail).getConnection();
        final SailRepositoryConnection pcjConn = new SailRepository(pcjSail).getConnection();
        addPCJS(pcjConn);
        try {
            final IRI superclass = VF.createIRI("uri:superclass");
            final IRI superclass2 = VF.createIRI("uri:superclass2");

            conn.add(subclass, RDF.TYPE, superclass);
            conn.add(subclass2, RDF.TYPE, superclass2);
            conn.add(obj, RDFS.LABEL, VF.createLiteral("label"));
            conn.add(obj2, RDFS.LABEL, VF.createLiteral("label2"));

            final String indexSparqlString = ""//
                    + "SELECT ?dog ?pig ?duck  " //
                    + "{" //
                    + "  ?pig a ?dog . "//
                    + "  ?pig <http://www.w3.org/2000/01/rdf-schema#label> ?duck "//
                    + "}";//

            final CountingResultHandler crh1 = new CountingResultHandler();
            final CountingResultHandler crh2 = new CountingResultHandler();

            PcjIntegrationTestingUtil.createAndPopulatePcj(conn, getMongoClient(), conf.getRyaInstanceName() + 1, conf.getRyaInstanceName(), indexSparqlString);

            conn.prepareTupleQuery(QueryLanguage.SPARQL, indexSparqlString).evaluate(crh1);
            PcjIntegrationTestingUtil.deleteCoreRyaTables(getMongoClient(), conf.getRyaInstanceName(), conf.getTriplesCollectionName());
            pcjConn.prepareTupleQuery(QueryLanguage.SPARQL, indexSparqlString).evaluate(crh2);

            assertEquals(crh1.count, crh2.count);
        } finally {
            conn.close();
            pcjConn.close();
            nonPcjSail.shutDown();
            pcjSail.shutDown();
        }
    }

    @Test
    public void testEvaluateTwoIndexValidate() throws Exception {
        final Sail nonPcjSail = RyaSailFactory.getInstance(conf);
        final MongoDBRdfConfiguration pcjConf = conf.clone();
        pcjConf.setBoolean(ConfigUtils.USE_PCJ, true);
        final Sail pcjSail = RyaSailFactory.getInstance(pcjConf);
        final SailRepositoryConnection conn = new SailRepository(nonPcjSail).getConnection();
        final SailRepositoryConnection pcjConn = new SailRepository(pcjSail).getConnection();
        addPCJS(pcjConn);
        try {
            final IRI superclass = VF.createIRI("uri:superclass");
            final IRI superclass2 = VF.createIRI("uri:superclass2");

            conn.add(subclass, RDF.TYPE, superclass);
            conn.add(subclass2, RDF.TYPE, superclass2);
            conn.add(obj, RDFS.LABEL, VF.createLiteral("label"));
            conn.add(obj2, RDFS.LABEL, VF.createLiteral("label2"));

            final String indexSparqlString = ""//
                    + "SELECT ?dog ?pig ?duck  " //
                    + "{" //
                    + "  ?pig a ?dog . "//
                    + "  ?pig <http://www.w3.org/2000/01/rdf-schema#label> ?duck "//
                    + "}";//

            final String indexSparqlString2 = ""//
                    + "SELECT ?o ?f ?e ?c ?l  " //
                    + "{" //
                    + "  ?e <uri:talksTo> ?o . "//
                    + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                    + "  ?c a ?f . " //
                    + "}";//

            final String queryString = ""//
                    + "SELECT ?e ?c ?l ?f ?o " //
                    + "{" //
                    + "  ?e a ?c . "//
                    + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                    + "  ?e <uri:talksTo> ?o . "//
                    + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                    + "  ?c a ?f . " //
                    + "}";//

            PcjIntegrationTestingUtil.createAndPopulatePcj(conn, getMongoClient(), conf.getRyaInstanceName() + 1, conf.getRyaInstanceName(), indexSparqlString);
            final MongoPcjQueryNode ais1 = new MongoPcjQueryNode(conf, conf.getRyaInstanceName() + 1);

            PcjIntegrationTestingUtil.createAndPopulatePcj(conn, getMongoClient(), conf.getRyaInstanceName() + 2, conf.getRyaInstanceName(), indexSparqlString2);
            final MongoPcjQueryNode ais2 = new MongoPcjQueryNode(conf, conf.getRyaInstanceName() + 2);

            final List<ExternalTupleSet> index = new ArrayList<>();
            index.add(ais1);
            index.add(ais2);

            ParsedQuery pq = null;
            final SPARQLParser sp = new SPARQLParser();
            pq = sp.parseQuery(queryString, null);
            final List<TupleExpr> teList = Lists.newArrayList();
            final TupleExpr te = pq.getTupleExpr();

            final PCJOptimizer pcj = new PCJOptimizer(index, false, new MongoPcjIndexSetProvider(new StatefulMongoDBRdfConfiguration(conf, getMongoClient())));
            pcj.optimize(te, null, null);
            teList.add(te);

            final IndexPlanValidator ipv = new IndexPlanValidator(false);

            assertTrue(ipv.isValid(te));
        } finally {
            conn.close();
            pcjConn.close();
            nonPcjSail.shutDown();
            pcjSail.shutDown();
        }
    }

    @Test
    public void testEvaluateThreeIndexValidate() throws Exception {
        final Sail nonPcjSail = RyaSailFactory.getInstance(conf);
        final MongoDBRdfConfiguration pcjConf = conf.clone();
        pcjConf.setBoolean(ConfigUtils.USE_PCJ, true);
        final Sail pcjSail = RyaSailFactory.getInstance(pcjConf);
        final SailRepositoryConnection conn = new SailRepository(nonPcjSail).getConnection();
        final SailRepositoryConnection pcjConn = new SailRepository(pcjSail).getConnection();
        addPCJS(pcjConn);
        try {
            final IRI superclass = VF.createIRI("uri:superclass");
            final IRI superclass2 = VF.createIRI("uri:superclass2");

            final IRI howlsAt = VF.createIRI("uri:howlsAt");
            final IRI subType = VF.createIRI("uri:subType");
            final IRI superSuperclass = VF.createIRI("uri:super_superclass");

            conn.add(subclass, RDF.TYPE, superclass);
            conn.add(subclass2, RDF.TYPE, superclass2);
            conn.add(obj, RDFS.LABEL, VF.createLiteral("label"));
            conn.add(obj2, RDFS.LABEL, VF.createLiteral("label2"));
            conn.add(sub, howlsAt, superclass);
            conn.add(superclass, subType, superSuperclass);

            final String indexSparqlString = ""//
                    + "SELECT ?dog ?pig ?duck  " //
                    + "{" //
                    + "  ?pig a ?dog . "//
                    + "  ?pig <http://www.w3.org/2000/01/rdf-schema#label> ?duck "//
                    + "}";//

            final String indexSparqlString2 = ""//
                    + "SELECT ?o ?f ?e ?c ?l  " //
                    + "{" //
                    + "  ?e <uri:talksTo> ?o . "//
                    + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                    + "  ?c a ?f . " //
                    + "}";//

            final String indexSparqlString3 = ""//
                    + "SELECT ?wolf ?sheep ?chicken  " //
                    + "{" //
                    + "  ?wolf <uri:howlsAt> ?sheep . "//
                    + "  ?sheep <uri:subType> ?chicken. "//
                    + "}";//

            final String queryString = ""//
                    + "SELECT ?e ?c ?l ?f ?o " //
                    + "{" //
                    + "  ?e a ?c . "//
                    + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                    + "  ?e <uri:talksTo> ?o . "//
                    + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                    + "  ?c a ?f . " //
                    + "  ?e <uri:howlsAt> ?f. "//
                    + "  ?f <uri:subType> ?o. "//
                    + "}";//

            PcjIntegrationTestingUtil.createAndPopulatePcj(conn, getMongoClient(), conf.getRyaInstanceName() + 1, conf.getRyaInstanceName(), indexSparqlString);
            final MongoPcjQueryNode ais1 = new MongoPcjQueryNode(conf, conf.getRyaInstanceName() + 1);

            PcjIntegrationTestingUtil.createAndPopulatePcj(conn, getMongoClient(), conf.getRyaInstanceName() + 2, conf.getRyaInstanceName(), indexSparqlString2);
            final MongoPcjQueryNode ais2 = new MongoPcjQueryNode(conf, conf.getRyaInstanceName() + 2);

            PcjIntegrationTestingUtil.createAndPopulatePcj(conn, getMongoClient(), conf.getRyaInstanceName() + 3, conf.getRyaInstanceName(), indexSparqlString3);
            final MongoPcjQueryNode ais3 = new MongoPcjQueryNode(conf, conf.getRyaInstanceName() + 3);

            final List<ExternalTupleSet> index = new ArrayList<>();
            index.add(ais1);
            index.add(ais3);
            index.add(ais2);

            ParsedQuery pq = null;
            final SPARQLParser sp = new SPARQLParser();
            pq = sp.parseQuery(queryString, null);
            final List<TupleExpr> teList = Lists.newArrayList();
            final TupleExpr te = pq.getTupleExpr();

            final PCJOptimizer pcj = new PCJOptimizer(index, false, new MongoPcjIndexSetProvider(new StatefulMongoDBRdfConfiguration(conf, getMongoClient())));
            pcj.optimize(te, null, null);

            teList.add(te);

            final IndexPlanValidator ipv = new IndexPlanValidator(false);

            assertTrue(ipv.isValid(te));
        } finally {
            conn.close();
            pcjConn.close();
            nonPcjSail.shutDown();
            pcjSail.shutDown();
        }
    }

    public static class CountingResultHandler implements TupleQueryResultHandler {
        private int count = 0;

        public int getCount() {
            return count;
        }

        public void resetCount() {
            count = 0;
        }

        @Override
        public void startQueryResult(final List<String> arg0) throws TupleQueryResultHandlerException {
        }

        @Override
        public void handleSolution(final BindingSet arg0) throws TupleQueryResultHandlerException {
            count++;
            System.out.println(arg0);
        }

        @Override
        public void endQueryResult() throws TupleQueryResultHandlerException {
        }

        @Override
        public void handleBoolean(final boolean arg0) throws QueryResultHandlerException {

        }

        @Override
        public void handleLinks(final List<String> arg0) throws QueryResultHandlerException {

        }
    }
}