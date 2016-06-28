package mvm.rya;

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



import static mvm.rya.api.RdfCloudTripleStoreConstants.NAMESPACE;

import java.io.InputStream;
import java.util.List;

import junit.framework.TestCase;
import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.accumulo.AccumuloRyaDAO;
import mvm.rya.api.RdfCloudTripleStoreConfiguration;
import mvm.rya.api.RdfCloudTripleStoreConstants;
import mvm.rya.rdftriplestore.RdfCloudTripleStore;
import mvm.rya.rdftriplestore.RyaSailRepository;
import mvm.rya.rdftriplestore.inference.InferenceEngine;
import mvm.rya.rdftriplestore.namespace.NamespaceManager;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.openrdf.model.Literal;
import org.openrdf.model.Model;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.QueryResultHandlerException;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResultHandler;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.query.Update;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryResult;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.Rio;

/**
 * Class RdfCloudTripleStoreConnectionTest
 * Date: Mar 3, 2011
 * Time: 12:03:29 PM
 */
public class RdfCloudTripleStoreConnectionTest extends TestCase {
    private Repository repository;
    ValueFactoryImpl vf = new ValueFactoryImpl();
    private InferenceEngine internalInferenceEngine;

    static String litdupsNS = "urn:test:litdups#";
    URI cpu = vf.createURI(litdupsNS, "cpu");
    protected RdfCloudTripleStore store;

    @Override
	public void setUp() throws Exception {
        super.setUp();
        store = new MockRdfCloudStore();
//        store.setDisplayQueryPlan(true);
//        store.setInferencing(false);
        NamespaceManager nm = new NamespaceManager(store.getRyaDAO(), store.getConf());
        store.setNamespaceManager(nm);
        repository = new RyaSailRepository(store);
        repository.initialize();
    }

    @Override
	public void tearDown() throws Exception {
        super.tearDown();
        repository.shutDown();
    }

    public void testAddStatement() throws Exception {
        RepositoryConnection conn = repository.getConnection();

        URI loadPerc = vf.createURI(litdupsNS, "loadPerc");
        URI uri1 = vf.createURI(litdupsNS, "uri1");
        conn.add(cpu, loadPerc, uri1);
        conn.commit();

        RepositoryResult<Statement> result = conn.getStatements(cpu, loadPerc, null, true);
        int count = 0;
        while (result.hasNext()) {
            count++;
            result.next();
        }
        result.close();
        assertEquals(1, count);

        //clean up
        conn.remove(cpu, loadPerc, uri1);

//        //test removal
        result = conn.getStatements(cpu, loadPerc, null, true, new Resource[0]);
        count = 0;
        while (result.hasNext()) {
            count++;
            result.next();
        }
        result.close();
        assertEquals(0, count);

        conn.close();
    }

//    public void testAddAuth() throws Exception {
//        RepositoryConnection conn = repository.getConnection();
//        URI cpu = vf.createURI(litdupsNS, "cpu");
//        URI loadPerc = vf.createURI(litdupsNS, "loadPerc");
//        URI uri1 = vf.createURI(litdupsNS, "uri1");
//        URI uri2 = vf.createURI(litdupsNS, "uri2");
//        URI uri3 = vf.createURI(litdupsNS, "uri3");
//        URI auth1 = vf.createURI(RdfCloudTripleStoreConstants.AUTH_NAMESPACE, "1");
//        URI auth2 = vf.createURI(RdfCloudTripleStoreConstants.AUTH_NAMESPACE, "2");
//        URI auth3 = vf.createURI(RdfCloudTripleStoreConstants.AUTH_NAMESPACE, "3");
//        conn.add(cpu, loadPerc, uri1, auth1, auth2, auth3);
//        conn.add(cpu, loadPerc, uri2, auth2, auth3);
//        conn.add(cpu, loadPerc, uri3, auth3);
//        conn.commit();
//
//        //query with no auth
//        RepositoryResult<Statement> result = conn.getStatements(cpu, loadPerc, null, true);
//        int count = 0;
//        while (result.hasNext()) {
//            count++;
//            result.next();
//        }
//        assertEquals(0, count);
//        result.close();
//
//        String query = "select * where {" +
//                "<" + cpu.toString() + "> ?p ?o1." +
//                "}";
//        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
//        tupleQuery.setBinding(RdfCloudTripleStoreConfiguration.CONF_QUERY_AUTH, vf.createLiteral("2"));
//        CountTupleHandler cth = new CountTupleHandler();
//        tupleQuery.evaluate(cth);
//        assertEquals(2, cth.getCount());
//
//        conn.close();
//    }

    public void testEvaluate() throws Exception {
        RepositoryConnection conn = repository.getConnection();
        URI loadPerc = vf.createURI(litdupsNS, "loadPerc");
        URI uri1 = vf.createURI(litdupsNS, "uri1");
        conn.add(cpu, loadPerc, uri1);
        conn.commit();

        String query = "select * where {" +
                "?x <" + loadPerc.stringValue() + "> ?o1." +
                "}";
        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        CountTupleHandler cth = new CountTupleHandler();
        tupleQuery.evaluate(cth);
        assertEquals(cth.getCount(), 1);
        conn.close();
    }

    public void testEvaluateMultiLine() throws Exception {
        RepositoryConnection conn = repository.getConnection();
        URI loadPerc = vf.createURI(litdupsNS, "loadPerc");
        URI uri1 = vf.createURI(litdupsNS, "uri1");
        URI pred2 = vf.createURI(litdupsNS, "pred2");
        URI uri2 = vf.createURI(litdupsNS, "uri2");
        conn.add(cpu, loadPerc, uri1);
        conn.add(cpu, pred2, uri2);
        conn.commit();

        String query = "select * where {" +
                "?x <" + loadPerc.stringValue() + "> ?o1." +
                "?x <" + pred2.stringValue() + "> ?o2." +
                "}";
        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        tupleQuery.setBinding(RdfCloudTripleStoreConfiguration.CONF_QUERYPLAN_FLAG, RdfCloudTripleStoreConstants.VALUE_FACTORY.createLiteral(true));
        CountTupleHandler cth = new CountTupleHandler();
        tupleQuery.evaluate(cth);
        conn.close();
        assertEquals(cth.getCount(), 1);
    }

    public void testPOObjRange() throws Exception {
        RepositoryConnection conn = repository.getConnection();
        URI loadPerc = vf.createURI(litdupsNS, "loadPerc");
        Literal six = vf.createLiteral("6");
        Literal sev = vf.createLiteral("7");
        Literal ten = vf.createLiteral("10");
        conn.add(cpu, loadPerc, six);
        conn.add(cpu, loadPerc, sev);
        conn.add(cpu, loadPerc, ten);
        conn.commit();

        String query = "PREFIX mvm: <" + NAMESPACE + ">\n" +
                "select * where {" +
                "?x <" + loadPerc.stringValue() + "> ?o.\n" +
                "FILTER(mvm:range(?o, '6', '8'))." +
                "}";
        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        CountTupleHandler cth = new CountTupleHandler();
        tupleQuery.evaluate(cth);
        conn.close();
        assertEquals(2, cth.getCount());
    }

    public void testPOPredRange() throws Exception {
        RepositoryConnection conn = repository.getConnection();
        URI loadPerc = vf.createURI(litdupsNS, "loadPerc1");
        URI loadPerc2 = vf.createURI(litdupsNS, "loadPerc2");
        URI loadPerc3 = vf.createURI(litdupsNS, "loadPerc3");
        URI loadPerc4 = vf.createURI(litdupsNS, "loadPerc4");
        Literal six = vf.createLiteral("6");
        Literal sev = vf.createLiteral("7");
        Literal ten = vf.createLiteral("10");
        conn.add(cpu, loadPerc, six);
        conn.add(cpu, loadPerc2, sev);
        conn.add(cpu, loadPerc4, ten);
        conn.commit();

        String query = "PREFIX mvm: <" + NAMESPACE + ">\n" +
                "select * where {" +
                "?x ?p ?o.\n" +
                "FILTER(mvm:range(?p, <" + loadPerc.stringValue() + ">, <" + loadPerc3.stringValue() + ">))." +
                "}";
        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        CountTupleHandler cth = new CountTupleHandler();
        tupleQuery.evaluate(cth);
        conn.close();
        assertEquals(cth.getCount(), 2);
    }

    public void testSPOPredRange() throws Exception {
        RepositoryConnection conn = repository.getConnection();
        URI loadPerc = vf.createURI(litdupsNS, "loadPerc1");
        URI loadPerc2 = vf.createURI(litdupsNS, "loadPerc2");
        URI loadPerc3 = vf.createURI(litdupsNS, "loadPerc3");
        URI loadPerc4 = vf.createURI(litdupsNS, "loadPerc4");
        Literal six = vf.createLiteral("6");
        Literal sev = vf.createLiteral("7");
        Literal ten = vf.createLiteral("10");
        conn.add(cpu, loadPerc, six);
        conn.add(cpu, loadPerc2, sev);
        conn.add(cpu, loadPerc4, ten);
        conn.commit();

        String query = "PREFIX mvm: <" + NAMESPACE + ">\n" +
                "select * where {" +
                "<" + cpu.stringValue() + "> ?p ?o.\n" +
                "FILTER(mvm:range(?p, <" + loadPerc.stringValue() + ">, <" + loadPerc3.stringValue() + ">))." +
                "}";
        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        CountTupleHandler cth = new CountTupleHandler();
        tupleQuery.evaluate(cth);
        conn.close();
        assertEquals(2, cth.getCount());
    }

    public void testSPOSubjRange() throws Exception {
        RepositoryConnection conn = repository.getConnection();
        URI cpu2 = vf.createURI(litdupsNS, "cpu2");
        URI cpu3 = vf.createURI(litdupsNS, "cpu3");
        URI loadPerc = vf.createURI(litdupsNS, "loadPerc");
        Literal six = vf.createLiteral("6");
        Literal sev = vf.createLiteral("7");
        Literal ten = vf.createLiteral("10");
        conn.add(cpu, loadPerc, six);
        conn.add(cpu2, loadPerc, sev);
        conn.add(cpu3, loadPerc, ten);
        conn.commit();

        String query = "PREFIX mvm: <" + NAMESPACE + ">\n" +
                "select * where {" +
                "?s ?p ?o.\n" +
                "FILTER(mvm:range(?s, <" + cpu.stringValue() + ">, <" + cpu2.stringValue() + ">))." +
                "}";
        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        CountTupleHandler cth = new CountTupleHandler();
        tupleQuery.evaluate(cth);
        conn.close();
        assertEquals(cth.getCount(), 2);
    }

    public void testSPOObjRange() throws Exception {
        RepositoryConnection conn = repository.getConnection();
        URI loadPerc = vf.createURI(litdupsNS, "loadPerc");
        Literal six = vf.createLiteral("6");
        Literal sev = vf.createLiteral("7");
        Literal ten = vf.createLiteral("10");
        conn.add(cpu, loadPerc, six);
        conn.add(cpu, loadPerc, sev);
        conn.add(cpu, loadPerc, ten);
        conn.commit();

        String query = "PREFIX mvm: <" + NAMESPACE + ">\n" +
                "select * where {" +
                "<" + cpu.stringValue() + "> <" + loadPerc.stringValue() + "> ?o.\n" +
                "FILTER(mvm:range(?o, '6', '8'))." +
                "}";
        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        CountTupleHandler cth = new CountTupleHandler();
        tupleQuery.evaluate(cth);
        conn.close();
        assertEquals(cth.getCount(), 2);
    }

    public void testOSPObjRange() throws Exception {
        RepositoryConnection conn = repository.getConnection();
        URI loadPerc = vf.createURI(litdupsNS, "loadPerc");
        Literal six = vf.createLiteral("6");
        Literal sev = vf.createLiteral("7");
        Literal ten = vf.createLiteral("10");
        conn.add(cpu, loadPerc, six);
        conn.add(cpu, loadPerc, sev);
        conn.add(cpu, loadPerc, ten);
        conn.commit();

        String query = "PREFIX mvm: <" + NAMESPACE + ">\n" +
                "select * where {" +
                "?s ?p ?o.\n" +
                "FILTER(mvm:range(?o, '6', '8'))." +
                "}";
        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        CountTupleHandler cth = new CountTupleHandler();
        tupleQuery.evaluate(cth);
        conn.close();
        assertEquals(cth.getCount(), 2);
    }

    public void testRegexFilter() throws Exception {
        RepositoryConnection conn = repository.getConnection();
        URI loadPerc = vf.createURI(litdupsNS, "loadPerc");
        URI testClass = vf.createURI(litdupsNS, "test");
        Literal six = vf.createLiteral("6");
        Literal sev = vf.createLiteral("7");
        Literal ten = vf.createLiteral("10");
        conn.add(cpu, loadPerc, six);
        conn.add(cpu, loadPerc, sev);
        conn.add(cpu, loadPerc, ten);
        conn.add(cpu, RDF.TYPE, testClass);
        conn.commit();

        String query = "PREFIX mvm: <" + NAMESPACE + ">\n" +
                "select * where {" +
                String.format("<%s> ?p ?o.\n", cpu.stringValue()) +
                "FILTER(regex(?o, '^1'))." +
                "}";
        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        CountTupleHandler cth = new CountTupleHandler();
        tupleQuery.evaluate(cth);
        conn.close();
        assertEquals(cth.getCount(), 1);
    }

    public void testMMRTS152() throws Exception {
        RepositoryConnection conn = repository.getConnection();
        URI loadPerc = vf.createURI(litdupsNS, "testPred");
        URI uri1 = vf.createURI(litdupsNS, "uri1");
        conn.add(cpu, loadPerc, uri1);
        conn.commit();

        RepositoryResult<Statement> result = conn.getStatements(cpu, loadPerc, null, false, new Resource[0]);
//        RdfCloudTripleStoreCollectionStatementsIterator iterator = new RdfCloudTripleStoreCollectionStatementsIterator(
//                cpu, loadPerc, null, store.connector,
//                vf, new Configuration(), null);

        while (result.hasNext()) {
            assertTrue(result.hasNext());
            assertNotNull(result.next());
        }

        conn.close();
    }

    public void testDuplicateLiterals() throws Exception {
        RepositoryConnection conn = repository.getConnection();

        URI loadPerc = vf.createURI(litdupsNS, "loadPerc");
        Literal lit1 = vf.createLiteral(0.0);
        Literal lit2 = vf.createLiteral(0.0);
        Literal lit3 = vf.createLiteral(0.0);

        conn.add(cpu, loadPerc, lit1);
        conn.add(cpu, loadPerc, lit2);
        conn.add(cpu, loadPerc, lit3);
        conn.commit();

        RepositoryResult<Statement> result = conn.getStatements(cpu, loadPerc, null, true, new Resource[0]);
        int count = 0;
        while (result.hasNext()) {
            count++;
            result.next();
        }
        result.close();
        assertEquals(1, count);

        //clean up
        conn.remove(cpu, loadPerc, lit1);
        conn.close();
    }

    public void testNotDuplicateUris() throws Exception {
        RepositoryConnection conn = repository.getConnection();

        URI loadPerc = vf.createURI(litdupsNS, "loadPerc");
        URI uri1 = vf.createURI(litdupsNS, "uri1");
        URI uri2 = vf.createURI(litdupsNS, "uri1");
        URI uri3 = vf.createURI(litdupsNS, "uri1");

        conn.add(cpu, loadPerc, uri1);
        conn.add(cpu, loadPerc, uri2);
        conn.add(cpu, loadPerc, uri3);
        conn.commit();

        RepositoryResult<Statement> result = conn.getStatements(cpu, loadPerc, null, true, new Resource[0]);
        int count = 0;
        while (result.hasNext()) {
            count++;
            result.next();
        }
        result.close();
        assertEquals(1, count);

        //clean up
        conn.remove(cpu, loadPerc, uri1);
        conn.close();
    }

    public void testNamespaceUsage() throws Exception {
        RepositoryConnection conn = repository.getConnection();
        conn.setNamespace("lit", litdupsNS);
        URI loadPerc = vf.createURI(litdupsNS, "loadPerc");
        final URI uri1 = vf.createURI(litdupsNS, "uri1");
        conn.add(cpu, loadPerc, uri1);
        conn.commit();

        String query = "PREFIX lit: <" + litdupsNS + ">\n" +
                "select * where {lit:cpu lit:loadPerc ?o.}";
        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        tupleQuery.evaluate(new TupleQueryResultHandler() {

            @Override
            public void startQueryResult(List<String> strings) throws TupleQueryResultHandlerException {
            }

            @Override
            public void endQueryResult() throws TupleQueryResultHandlerException {

            }

            @Override
            public void handleSolution(BindingSet bindingSet) throws TupleQueryResultHandlerException {
                assertTrue(uri1.toString().equals(bindingSet.getBinding("o").getValue().stringValue()));
            }

            @Override
            public void handleBoolean(boolean paramBoolean) throws QueryResultHandlerException {
            }

            @Override
            public void handleLinks(List<String> paramList) throws QueryResultHandlerException {
            }
        });
        conn.close();
    }

    public void testSubPropertyOf() throws Exception {
        if(internalInferenceEngine == null)
		 {
			return; //infer not supported;
		}

        RepositoryConnection conn = repository.getConnection();
        conn.add(new StatementImpl(vf.createURI(litdupsNS, "undergradDegreeFrom"), RDFS.SUBPROPERTYOF, vf.createURI(litdupsNS, "degreeFrom")));
        conn.add(new StatementImpl(vf.createURI(litdupsNS, "gradDegreeFrom"), RDFS.SUBPROPERTYOF, vf.createURI(litdupsNS, "degreeFrom")));
        conn.add(new StatementImpl(vf.createURI(litdupsNS, "degreeFrom"), RDFS.SUBPROPERTYOF, vf.createURI(litdupsNS, "memberOf")));
        conn.add(new StatementImpl(vf.createURI(litdupsNS, "memberOf"), RDFS.SUBPROPERTYOF, vf.createURI(litdupsNS, "associatedWith")));
        conn.add(new StatementImpl(vf.createURI(litdupsNS, "UgradA"), vf.createURI(litdupsNS, "undergradDegreeFrom"), vf.createURI(litdupsNS, "Harvard")));
        conn.add(new StatementImpl(vf.createURI(litdupsNS, "GradB"), vf.createURI(litdupsNS, "gradDegreeFrom"), vf.createURI(litdupsNS, "Yale")));
        conn.add(new StatementImpl(vf.createURI(litdupsNS, "ProfessorC"), vf.createURI(litdupsNS, "memberOf"), vf.createURI(litdupsNS, "Harvard")));
        conn.commit();
        conn.close();

        internalInferenceEngine.refreshGraph();

        conn = repository.getConnection();

        String query = "PREFIX rdfs: <" + RDFS.NAMESPACE + ">\n" +
                "PREFIX rdf: <" + RDF.NAMESPACE + ">\n" +
                "PREFIX lit: <" + litdupsNS + ">\n" +
                "select * where {?s lit:degreeFrom lit:Harvard.}";

        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        CountTupleHandler tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(1, tupleHandler.getCount());

        query = "PREFIX rdfs: <" + RDFS.NAMESPACE + ">\n" +
                "PREFIX rdf: <" + RDF.NAMESPACE + ">\n" +
                "PREFIX lit: <" + litdupsNS + ">\n" +
                "select * where {?s lit:memberOf lit:Harvard.}";

        tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(2, tupleHandler.getCount());

        query = "PREFIX rdfs: <" + RDFS.NAMESPACE + ">\n" +
                "PREFIX rdf: <" + RDF.NAMESPACE + ">\n" +
                "PREFIX lit: <" + litdupsNS + ">\n" +
                "select * where {?s lit:associatedWith ?o.}";

        tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(3, tupleHandler.getCount());

        query = "PREFIX rdfs: <" + RDFS.NAMESPACE + ">\n" +
                "PREFIX rdf: <" + RDF.NAMESPACE + ">\n" +
                "PREFIX lit: <" + litdupsNS + ">\n" +
                "select * where {?s lit:gradDegreeFrom lit:Yale.}";

        tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(1, tupleHandler.getCount());

        conn.close();
    }

    public void testEquivPropOf() throws Exception {
        if(internalInferenceEngine == null)
		 {
			return; //infer not supported;
		}

        RepositoryConnection conn = repository.getConnection();
        conn.add(new StatementImpl(vf.createURI(litdupsNS, "undergradDegreeFrom"), OWL.EQUIVALENTPROPERTY, vf.createURI(litdupsNS, "ugradDegreeFrom")));
        conn.add(new StatementImpl(vf.createURI(litdupsNS, "UgradA"), vf.createURI(litdupsNS, "undergradDegreeFrom"), vf.createURI(litdupsNS, "Harvard")));
        conn.add(new StatementImpl(vf.createURI(litdupsNS, "GradB"), vf.createURI(litdupsNS, "ugradDegreeFrom"), vf.createURI(litdupsNS, "Harvard")));
        conn.add(new StatementImpl(vf.createURI(litdupsNS, "GradC"), vf.createURI(litdupsNS, "ugraduateDegreeFrom"), vf.createURI(litdupsNS, "Harvard")));
        conn.commit();
        conn.close();

        internalInferenceEngine.refreshGraph();

        conn = repository.getConnection();

        String query = "PREFIX rdfs: <" + RDFS.NAMESPACE + ">\n" +
                "PREFIX rdf: <" + RDF.NAMESPACE + ">\n" +
                "PREFIX lit: <" + litdupsNS + ">\n" +
                "select * where {?s lit:ugradDegreeFrom lit:Harvard.}";

        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        CountTupleHandler tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(2, tupleHandler.getCount());

        conn.close();
    }

    public void testSymmPropOf() throws Exception {
        if(internalInferenceEngine == null)
		 {
			return; //infer not supported;
		}

        RepositoryConnection conn = repository.getConnection();
        conn.add(new StatementImpl(vf.createURI(litdupsNS, "friendOf"), RDF.TYPE, OWL.SYMMETRICPROPERTY));
        conn.add(new StatementImpl(vf.createURI(litdupsNS, "Bob"), vf.createURI(litdupsNS, "friendOf"), vf.createURI(litdupsNS, "Jeff")));
        conn.add(new StatementImpl(vf.createURI(litdupsNS, "James"), vf.createURI(litdupsNS, "friendOf"), vf.createURI(litdupsNS, "Jeff")));
        conn.commit();
        conn.close();

        internalInferenceEngine.refreshGraph();

        conn = repository.getConnection();

        String query = "PREFIX rdfs: <" + RDFS.NAMESPACE + ">\n" +
                "PREFIX rdf: <" + RDF.NAMESPACE + ">\n" +
                "PREFIX lit: <" + litdupsNS + ">\n" +
                "select * where {?s lit:friendOf lit:Bob.}";

        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        CountTupleHandler tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(1, tupleHandler.getCount());

        query = "PREFIX rdfs: <" + RDFS.NAMESPACE + ">\n" +
                "PREFIX rdf: <" + RDF.NAMESPACE + ">\n" +
                "PREFIX lit: <" + litdupsNS + ">\n" +
                "select * where {?s lit:friendOf lit:James.}";

        tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(1, tupleHandler.getCount());

        query = "PREFIX rdfs: <" + RDFS.NAMESPACE + ">\n" +
                "PREFIX rdf: <" + RDF.NAMESPACE + ">\n" +
                "PREFIX lit: <" + litdupsNS + ">\n" +
                "select * where {?s lit:friendOf lit:Jeff.}";

        tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(2, tupleHandler.getCount());

        conn.close();
    }

    public void testTransitiveProp() throws Exception {
        if(internalInferenceEngine == null)
		 {
			return; //infer not supported;
		}

        RepositoryConnection conn = repository.getConnection();
        conn.add(new StatementImpl(vf.createURI(litdupsNS, "subRegionOf"), RDF.TYPE, OWL.TRANSITIVEPROPERTY));
        conn.add(new StatementImpl(vf.createURI(litdupsNS, "Queens"), vf.createURI(litdupsNS, "subRegionOf"), vf.createURI(litdupsNS, "NYC")));
        conn.add(new StatementImpl(vf.createURI(litdupsNS, "NYC"), vf.createURI(litdupsNS, "subRegionOf"), vf.createURI(litdupsNS, "NY")));
        conn.add(new StatementImpl(vf.createURI(litdupsNS, "NY"), vf.createURI(litdupsNS, "subRegionOf"), vf.createURI(litdupsNS, "US")));
        conn.add(new StatementImpl(vf.createURI(litdupsNS, "US"), vf.createURI(litdupsNS, "subRegionOf"), vf.createURI(litdupsNS, "NorthAmerica")));
        conn.add(new StatementImpl(vf.createURI(litdupsNS, "NorthAmerica"), vf.createURI(litdupsNS, "subRegionOf"), vf.createURI(litdupsNS, "World")));
        conn.commit();
        conn.close();

        internalInferenceEngine.refreshGraph();

        conn = repository.getConnection();

        String query = "PREFIX rdfs: <" + RDFS.NAMESPACE + ">\n" +
                "PREFIX rdf: <" + RDF.NAMESPACE + ">\n" +
                "PREFIX lit: <" + litdupsNS + ">\n" +
                "select * where {?s lit:subRegionOf lit:NorthAmerica.}";

        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        CountTupleHandler tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(4, tupleHandler.getCount());

        query = "PREFIX rdfs: <" + RDFS.NAMESPACE + ">\n" +
                "PREFIX rdf: <" + RDF.NAMESPACE + ">\n" +
                "PREFIX lit: <" + litdupsNS + ">\n" +
                "select * where {?s lit:subRegionOf lit:NY.}";

        tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(2, tupleHandler.getCount());

        query = "PREFIX rdfs: <" + RDFS.NAMESPACE + ">\n" +
                "PREFIX rdf: <" + RDF.NAMESPACE + ">\n" +
                "PREFIX lit: <" + litdupsNS + ">\n" +
                "select * where {lit:Queens lit:subRegionOf ?s.}";

        tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(5, tupleHandler.getCount());

        query = "PREFIX rdfs: <" + RDFS.NAMESPACE + ">\n" +
                "PREFIX rdf: <" + RDF.NAMESPACE + ">\n" +
                "PREFIX lit: <" + litdupsNS + ">\n" +
                "select * where {lit:NY lit:subRegionOf ?s.}";

        tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(3, tupleHandler.getCount());

        conn.close();
    }

    public void testInverseOf() throws Exception {
        if(internalInferenceEngine == null)
		 {
			return; //infer not supported;
		}

        RepositoryConnection conn = repository.getConnection();
        conn.add(new StatementImpl(vf.createURI(litdupsNS, "degreeFrom"), OWL.INVERSEOF, vf.createURI(litdupsNS, "hasAlumnus")));
        conn.add(new StatementImpl(vf.createURI(litdupsNS, "UgradA"), vf.createURI(litdupsNS, "degreeFrom"), vf.createURI(litdupsNS, "Harvard")));
        conn.add(new StatementImpl(vf.createURI(litdupsNS, "GradB"), vf.createURI(litdupsNS, "degreeFrom"), vf.createURI(litdupsNS, "Harvard")));
        conn.add(new StatementImpl(vf.createURI(litdupsNS, "Harvard"), vf.createURI(litdupsNS, "hasAlumnus"), vf.createURI(litdupsNS, "AlumC")));
        conn.commit();
        conn.close();

        internalInferenceEngine.refreshGraph();

        conn = repository.getConnection();

        String query = "PREFIX rdfs: <" + RDFS.NAMESPACE + ">\n" +
                "PREFIX rdf: <" + RDF.NAMESPACE + ">\n" +
                "PREFIX lit: <" + litdupsNS + ">\n" +
                "select * where {lit:Harvard lit:hasAlumnus ?s.}";

        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        CountTupleHandler tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(3, tupleHandler.getCount());

        query = "PREFIX rdfs: <" + RDFS.NAMESPACE + ">\n" +
                "PREFIX rdf: <" + RDF.NAMESPACE + ">\n" +
                "PREFIX lit: <" + litdupsNS + ">\n" +
                "select * where {?s lit:degreeFrom lit:Harvard.}";

        tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(3, tupleHandler.getCount());

        conn.close();
    }

    public void testSubClassOf() throws Exception {
        if(internalInferenceEngine == null)
		 {
			return; //infer not supported;
		}

        RepositoryConnection conn = repository.getConnection();
        conn.add(new StatementImpl(vf.createURI(litdupsNS, "UndergraduateStudent"), RDFS.SUBCLASSOF, vf.createURI(litdupsNS, "Student")));
        conn.add(new StatementImpl(vf.createURI(litdupsNS, "Student"), RDFS.SUBCLASSOF, vf.createURI(litdupsNS, "Person")));
        conn.add(new StatementImpl(vf.createURI(litdupsNS, "UgradA"), RDF.TYPE, vf.createURI(litdupsNS, "UndergraduateStudent")));
        conn.add(new StatementImpl(vf.createURI(litdupsNS, "StudentB"), RDF.TYPE, vf.createURI(litdupsNS, "Student")));
        conn.add(new StatementImpl(vf.createURI(litdupsNS, "PersonC"), RDF.TYPE, vf.createURI(litdupsNS, "Person")));
        conn.commit();
        conn.close();

        internalInferenceEngine.refreshGraph();

        conn = repository.getConnection();

        //simple api first
        RepositoryResult<Statement> person = conn.getStatements(null, RDF.TYPE, vf.createURI(litdupsNS, "Person"), true);
        int count = 0;
        while (person.hasNext()) {
            count++;
            person.next();
        }
        person.close();
        assertEquals(3, count);

        String query = "PREFIX rdfs: <" + RDFS.NAMESPACE + ">\n" +
                "PREFIX rdf: <" + RDF.NAMESPACE + ">\n" +
                "PREFIX lit: <" + litdupsNS + ">\n" +
                "select * where {?s rdf:type lit:Person.}";

        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        CountTupleHandler tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(3, tupleHandler.getCount());

        query = "PREFIX rdfs: <" + RDFS.NAMESPACE + ">\n" +
                "PREFIX rdf: <" + RDF.NAMESPACE + ">\n" +
                "PREFIX lit: <" + litdupsNS + ">\n" +
                "select * where {?s rdf:type lit:Student.}";

        tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(2, tupleHandler.getCount());

        query = "PREFIX rdfs: <" + RDFS.NAMESPACE + ">\n" +
                "PREFIX rdf: <" + RDF.NAMESPACE + ">\n" +
                "PREFIX lit: <" + litdupsNS + ">\n" +
                "select * where {?s rdf:type lit:UndergraduateStudent.}";

        tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(1, tupleHandler.getCount());

        conn.close();
    }

    public void testSameAs() throws Exception {
        if(internalInferenceEngine == null)
		 {
			return; //infer not supported;
		}

        RepositoryConnection conn = repository.getConnection();
        conn.add(new StatementImpl(vf.createURI(litdupsNS, "StudentA1"), OWL.SAMEAS, vf.createURI(litdupsNS, "StudentA2")));
        conn.add(new StatementImpl(vf.createURI(litdupsNS, "StudentA2"), OWL.SAMEAS, vf.createURI(litdupsNS, "StudentA3")));
        conn.add(new StatementImpl(vf.createURI(litdupsNS, "StudentB1"), OWL.SAMEAS, vf.createURI(litdupsNS, "StudentB2")));
        conn.add(new StatementImpl(vf.createURI(litdupsNS, "StudentB2"), OWL.SAMEAS, vf.createURI(litdupsNS, "StudentB3")));
        conn.add(new StatementImpl(vf.createURI(litdupsNS, "StudentA1"), vf.createURI(litdupsNS, "pred1"), vf.createURI(litdupsNS, "StudentB3")));
        conn.add(new StatementImpl(vf.createURI(litdupsNS, "StudentB1"), vf.createURI(litdupsNS, "pred2"), vf.createURI(litdupsNS, "StudentA3")));
        conn.commit();
        conn.close();

        internalInferenceEngine.refreshGraph();

        conn = repository.getConnection();

        // query where finds sameAs for obj, pred specified
        String query = "PREFIX rdfs: <" + RDFS.NAMESPACE + ">\n" +
                "PREFIX rdf: <" + RDF.NAMESPACE + ">\n" +
                "PREFIX lit: <" + litdupsNS + ">\n" +
                "select ?s where {?s lit:pred1 lit:StudentB2.}";

        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        CountTupleHandler tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(1, tupleHandler.getCount());

        // query where finds sameAs for obj only specified
        query = "PREFIX rdfs: <" + RDFS.NAMESPACE + ">\n" +
                "PREFIX rdf: <" + RDF.NAMESPACE + ">\n" +
                "PREFIX lit: <" + litdupsNS + ">\n" +
                "select ?s where {?s ?p lit:StudentB2.}";

        tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(3, tupleHandler.getCount()); // including sameAs assertions

        // query where finds sameAs for subj, pred specified
        query = "PREFIX rdfs: <" + RDFS.NAMESPACE + ">\n" +
                "PREFIX rdf: <" + RDF.NAMESPACE + ">\n" +
                "PREFIX lit: <" + litdupsNS + ">\n" +
                "select ?s where {lit:StudentB2 lit:pred2 ?s.}";

        tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(1, tupleHandler.getCount()); // including sameAs assertions

        // query where finds sameAs for subj only specified
        query = "PREFIX rdfs: <" + RDFS.NAMESPACE + ">\n" +
                "PREFIX rdf: <" + RDF.NAMESPACE + ">\n" +
                "PREFIX lit: <" + litdupsNS + ">\n" +
                "select ?s where {lit:StudentB2 ?p ?s.}";

        tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(3, tupleHandler.getCount()); // including sameAs assertions

        // query where finds sameAs for subj, obj specified
        query = "PREFIX rdfs: <" + RDFS.NAMESPACE + ">\n" +
                "PREFIX rdf: <" + RDF.NAMESPACE + ">\n" +
                "PREFIX lit: <" + litdupsNS + ">\n" +
                "select ?s where {lit:StudentB2 ?s lit:StudentA2.}";

        tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(1, tupleHandler.getCount());

        conn.close();
    }

    public void testNamedGraphLoad() throws Exception {
        InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream("namedgraphs.trig");
        assertNotNull(stream);
        RepositoryConnection conn = repository.getConnection();
        conn.add(stream, "", RDFFormat.TRIG);
        conn.commit();

        String query = "PREFIX  ex:  <http://www.example.org/exampleDocument#>\n" +
                "PREFIX  voc:  <http://www.example.org/vocabulary#>\n" +
                "PREFIX  foaf:  <http://xmlns.com/foaf/0.1/>\n" +
                "PREFIX  rdfs:  <http://www.w3.org/2000/01/rdf-schema#>\n" +
                "\n" +
                "SELECT * \n" +
//                "FROM NAMED <http://www.example.org/exampleDocument#G1>\n" +
                "WHERE\n" +
                "{\n" +
                "  GRAPH ex:G1\n" +
                "  {\n" +
                "    ?m voc:name ?name ;\n" +
                "           voc:homepage ?hp .\n" +
                "  } .\n" +
                " GRAPH ex:G2\n" +
                "  {\n" +
                "    ?m voc:hasSkill ?skill .\n" +
                "  } .\n" +
                "}";
        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        CountTupleHandler tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
//        tupleQuery.evaluate(new PrintTupleHandler());
        assertEquals(1, tupleHandler.getCount());

        query = "PREFIX  ex:  <http://www.example.org/exampleDocument#>\n" +
                "PREFIX  voc:  <http://www.example.org/vocabulary#>\n" +
                "PREFIX  swp:  <http://www.w3.org/2004/03/trix/swp-1/>\n" +
                "PREFIX  foaf:  <http://xmlns.com/foaf/0.1/>\n" +
                "PREFIX  rdfs:  <http://www.w3.org/2000/01/rdf-schema#>\n" +
                "\n" +
                "SELECT * \n" +
                "WHERE\n" +
                "{\n" +
                "  GRAPH ex:G3\n" +
                "  {\n" +
                "    ?g swp:assertedBy ?w .\n" +
                "    ?w swp:authority ex:Tom .\n" +
                "  } .\n" +
                "  GRAPH ?g\n" +
                "  {\n" +
                "    ?m voc:name ?name .\n" +
                "  } .\n" +
                "}";

        tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(1, tupleHandler.getCount());

        query = "PREFIX  ex:  <http://www.example.org/exampleDocument#>\n" +
                "PREFIX  voc:  <http://www.example.org/vocabulary#>\n" +
                "PREFIX  swp:  <http://www.w3.org/2004/03/trix/swp-1/>\n" +
                "PREFIX  foaf:  <http://xmlns.com/foaf/0.1/>\n" +
                "PREFIX  rdfs:  <http://www.w3.org/2000/01/rdf-schema#>\n" +
                "\n" +
                "SELECT * \n" +
                "WHERE\n" +
                "{\n" +
                "  GRAPH ?g\n" +
                "  {\n" +
                "    ?m voc:name ?name .\n" +
                "  } .\n" +
                "}";

        tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
//        tupleQuery.setBinding(BINDING_DISP_QUERYPLAN, VALUE_FACTORY.createLiteral(true));
        tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(2, tupleHandler.getCount());

        conn.close();
    }

    public void testNamedGraphLoad2() throws Exception {
        InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream("namedgraphs.trig");
        assertNotNull(stream);
        RepositoryConnection conn = repository.getConnection();
        conn.add(stream, "", RDFFormat.TRIG);
        conn.commit();

        RepositoryResult<Statement> statements = conn.getStatements(null, vf.createURI("http://www.example.org/vocabulary#name"), null, true, vf.createURI("http://www.example.org/exampleDocument#G1"));
        int count = 0;
        while (statements.hasNext()) {
            statements.next();
            count++;
        }
        statements.close();
        assertEquals(1, count);

        conn.close();
    }

//    public void testNamedGraphLoadWInlineAuth() throws Exception {
//        InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream("namedgraphs.trig");
//        assertNotNull(stream);
//        URI auth1 = vf.createURI(RdfCloudTripleStoreConstants.AUTH_NAMESPACE, "1");
//        RepositoryConnection conn = repository.getConnection();
//        conn.add(stream, "", RDFFormat.TRIG, auth1);
//        conn.commit();
//
//        String query = "PREFIX  ex:  <http://www.example.org/exampleDocument#>\n" +
//                "PREFIX  voc:  <http://www.example.org/vocabulary#>\n" +
//                "PREFIX  foaf:  <http://xmlns.com/foaf/0.1/>\n" +
//                "PREFIX  rdfs:  <http://www.w3.org/2000/01/rdf-schema#>\n" +
//                "\n" +
//                "SELECT * \n" +
//                "WHERE\n" +
//                "{\n" +
//                "  GRAPH ex:G1\n" +
//                "  {\n" +
//                "    ?m voc:name ?name ;\n" +
//                "           voc:homepage ?hp .\n" +
//                "  } .\n" +
//                " GRAPH ex:G2\n" +
//                "  {\n" +
//                "    ?m voc:hasSkill ?skill .\n" +
//                "  } .\n" +
//                "}";
//        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
//        tupleQuery.setBinding(RdfCloudTripleStoreConfiguration.CONF_QUERY_AUTH, vf.createLiteral("1"));
//        CountTupleHandler tupleHandler = new CountTupleHandler();
//        tupleQuery.evaluate(tupleHandler);
//        assertEquals(1, tupleHandler.getCount());
//
//        query = "PREFIX  ex:  <http://www.example.org/exampleDocument#>\n" +
//                "PREFIX  voc:  <http://www.example.org/vocabulary#>\n" +
//                "PREFIX  swp:  <http://www.w3.org/2004/03/trix/swp-1/>\n" +
//                "PREFIX  foaf:  <http://xmlns.com/foaf/0.1/>\n" +
//                "PREFIX  rdfs:  <http://www.w3.org/2000/01/rdf-schema#>\n" +
//                "\n" +
//                "SELECT * \n" +
//                "WHERE\n" +
//                "{\n" +
//                "  GRAPH ex:G3\n" +
//                "  {\n" +
//                "    ?g swp:assertedBy ?w .\n" +
//                "    ?w swp:authority ex:Tom .\n" +
//                "  } .\n" +
//                "  GRAPH ?g\n" +
//                "  {\n" +
//                "    ?m voc:name ?name .\n" +
//                "  } .\n" +
//                "}";
//
//        tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
//        tupleHandler = new CountTupleHandler();
//        tupleQuery.evaluate(tupleHandler);
//        assertEquals(0, tupleHandler.getCount());
//
//        conn.close();
//    }

    private static String escape(Value r) {
        if (r instanceof URI) {
			return "<" + r.toString() +">";
		}
        return r.toString();
    }

    private static String getSparqlUpdate() throws Exception {
        InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream("namedgraphs.trig");
        assertNotNull(stream);

        Model m = Rio.parse(stream, "", RDFFormat.TRIG);

        StringBuffer updateStr = new StringBuffer();
        updateStr.append("INSERT DATA {\n");
        for (Statement s : m){
            if (s.getContext() != null) {
                updateStr.append("graph ");
                updateStr.append(escape(s.getContext()));
                updateStr.append("{ ");
            }

            updateStr.append(escape(s.getSubject()));
            updateStr.append(" ");
            updateStr.append(escape(s.getPredicate()));
            updateStr.append(" ");
            updateStr.append(escape(s.getObject()));
            if (s.getContext() != null){
                updateStr.append("}");
            }
            updateStr.append(" . \n");
        }
        updateStr.append("}");
        return updateStr.toString();
    }

    // Set the persistence visibilites on the config
    public void testUpdateWAuthOnConfig() throws Exception {
        String sparqlUpdate = getSparqlUpdate();

        RdfCloudTripleStore tstore = new MockRdfCloudStore();
        NamespaceManager nm = new NamespaceManager(tstore.getRyaDAO(), tstore.getConf());
        tstore.setNamespaceManager(nm);
        SailRepository repo = new SailRepository(tstore);
        tstore.getRyaDAO().getConf().setCv("1|2");
        repo.initialize();

        RepositoryConnection conn = repo.getConnection();
        Update u = conn.prepareUpdate(QueryLanguage.SPARQL, sparqlUpdate);
        u.execute();

        String query = "PREFIX  ex:  <http://www.example.org/exampleDocument#>\n" +
                "PREFIX  voc:  <http://www.example.org/vocabulary#>\n" +
                "PREFIX  foaf:  <http://xmlns.com/foaf/0.1/>\n" +
                "PREFIX  rdfs:  <http://www.w3.org/2000/01/rdf-schema#>\n" +
                "\n" +
                "SELECT * \n" +
//                "FROM NAMED <http://www.example.org/exampleDocument#G1>\n" +
                "WHERE\n" +
                "{\n" +
                "  GRAPH ex:G1\n" +
                "  {\n" +
                "    ?m voc:name ?name ;\n" +
                "           voc:homepage ?hp .\n" +
                "  } .\n" +
                " GRAPH ex:G2\n" +
                "  {\n" +
                "    ?m voc:hasSkill ?skill .\n" +
                "  } .\n" +
                "}";
        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        tupleQuery.setBinding(RdfCloudTripleStoreConfiguration.CONF_QUERY_AUTH, vf.createLiteral("2"));
        CountTupleHandler tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(1, tupleHandler.getCount());

        tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query); //no auth
        tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(0, tupleHandler.getCount());

        conn.close();

        repo.shutDown();
    }

    public void testNamedGraphLoadWAuth() throws Exception {
        InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream("namedgraphs.trig");
        assertNotNull(stream);

        RdfCloudTripleStore tstore = new MockRdfCloudStore();
        NamespaceManager nm = new NamespaceManager(tstore.getRyaDAO(), tstore.getConf());
        tstore.setNamespaceManager(nm);
        SailRepository repo = new SailRepository(tstore);
        tstore.getRyaDAO().getConf().setCv("1|2");
        repo.initialize();

        RepositoryConnection conn = repo.getConnection();
        conn.add(stream, "", RDFFormat.TRIG);
        conn.commit();

        String query = "PREFIX  ex:  <http://www.example.org/exampleDocument#>\n" +
                "PREFIX  voc:  <http://www.example.org/vocabulary#>\n" +
                "PREFIX  foaf:  <http://xmlns.com/foaf/0.1/>\n" +
                "PREFIX  rdfs:  <http://www.w3.org/2000/01/rdf-schema#>\n" +
                "\n" +
                "SELECT * \n" +
//                "FROM NAMED <http://www.example.org/exampleDocument#G1>\n" +
                "WHERE\n" +
                "{\n" +
                "  GRAPH ex:G1\n" +
                "  {\n" +
                "    ?m voc:name ?name ;\n" +
                "           voc:homepage ?hp .\n" +
                "  } .\n" +
                " GRAPH ex:G2\n" +
                "  {\n" +
                "    ?m voc:hasSkill ?skill .\n" +
                "  } .\n" +
                "}";
        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        tupleQuery.setBinding(RdfCloudTripleStoreConfiguration.CONF_QUERY_AUTH, vf.createLiteral("2"));
        CountTupleHandler tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(1, tupleHandler.getCount());

        tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query); //no auth
        tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(0, tupleHandler.getCount());

        conn.close();

        repo.shutDown();
    }

    public void testInsertDeleteData() throws Exception {
        RepositoryConnection conn = repository.getConnection();

        String insert = "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n" +
                "INSERT DATA\n" +
                "{ <http://example/book3> dc:title    \"A new book\" ;\n" +
                "                         dc:creator  \"A.N.Other\" .\n" +
                "}";
        Update update = conn.prepareUpdate(QueryLanguage.SPARQL, insert);
        update.execute();

        String query = "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n" +
                "select * where { <http://example/book3> ?p ?o. }";
        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        CountTupleHandler tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(2, tupleHandler.getCount());

        String delete = "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n" +
                "\n" +
                "DELETE DATA\n" +
                "{ <http://example/book3> dc:title    \"A new book\" ;\n" +
                "                         dc:creator  \"A.N.Other\" .\n" +
                "}";
        update = conn.prepareUpdate(QueryLanguage.SPARQL, delete);
        update.execute();

        query = "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n" +
                "select * where { <http://example/book3> ?p ?o. }";
        tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(0, tupleHandler.getCount());

        conn.close();
    }

    public void testUpdateData() throws Exception {
        RepositoryConnection conn = repository.getConnection();

        String insert = "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n" +
                "PREFIX ex: <http://example/addresses#>\n" +
                "INSERT DATA\n" +
                "{ GRAPH ex:G1 {\n" +
                "<http://example/book3> dc:title    \"A new book\" ;\n" +
                "                         dc:creator  \"A.N.Other\" .\n" +
                "}\n" +
                "}";
        Update update = conn.prepareUpdate(QueryLanguage.SPARQL, insert);
        update.execute();

        String query = "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n" +
                "select * where { <http://example/book3> ?p ?o. }";
        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        CountTupleHandler tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(2, tupleHandler.getCount());

        String insdel = "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n" +
                "\n" +
                "WITH <http://example/addresses#G1>\n" +
                "DELETE { ?book dc:title ?title }\n" +
                "INSERT { ?book dc:title \"A newer book\"." +
                "         ?book dc:add \"Additional Info\" }\n" +
                "WHERE\n" +
                "  { ?book dc:creator \"A.N.Other\" ;\n" +
                "        dc:title ?title .\n" +
                "  }";
        update = conn.prepareUpdate(QueryLanguage.SPARQL, insdel);
        update.execute();

        query = "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n" +
                "PREFIX ex: <http://example/addresses#>\n" +
                "select * where { GRAPH ex:G1 {<http://example/book3> ?p ?o. } }";
        tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(3, tupleHandler.getCount());

        conn.close();
    }

    public void testClearGraph() throws Exception {
        RepositoryConnection conn = repository.getConnection();

        String insert = "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n" +
                "PREFIX ex: <http://example/addresses#>\n" +
                "INSERT DATA\n" +
                "{ GRAPH ex:G1 {\n" +
                "<http://example/book3> dc:title    \"A new book\" ;\n" +
                "                         dc:creator  \"A.N.Other\" .\n" +
                "}\n" +
                "}";
        Update update = conn.prepareUpdate(QueryLanguage.SPARQL, insert);
        update.execute();

        insert = "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n" +
                "PREFIX ex: <http://example/addresses#>\n" +
                "INSERT DATA\n" +
                "{ GRAPH ex:G2 {\n" +
                "<http://example/book3> dc:title    \"A new book\" ;\n" +
                "                         dc:creator  \"A.N.Other\" .\n" +
                "}\n" +
                "}";
        update = conn.prepareUpdate(QueryLanguage.SPARQL, insert);
        update.execute();

        String query = "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n" +
                "select * where { <http://example/book3> ?p ?o. }";
        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        CountTupleHandler tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(4, tupleHandler.getCount());

        tupleHandler = new CountTupleHandler();
        conn.clear(new URIImpl("http://example/addresses#G2"));
        tupleQuery.evaluate(tupleHandler);
        assertEquals(2, tupleHandler.getCount());

        tupleHandler = new CountTupleHandler();
        conn.clear(new URIImpl("http://example/addresses#G1"));
        tupleQuery.evaluate(tupleHandler);
        assertEquals(0, tupleHandler.getCount());

        conn.close();
    }

    public void testClearAllGraph() throws Exception {
        RepositoryConnection conn = repository.getConnection();

        String insert = "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n" +
                "PREFIX ex: <http://example/addresses#>\n" +
                "INSERT DATA\n" +
                "{ GRAPH ex:G1 {\n" +
                "<http://example/book3> dc:title    \"A new book\" ;\n" +
                "                         dc:creator  \"A.N.Other\" .\n" +
                "}\n" +
                "}";
        Update update = conn.prepareUpdate(QueryLanguage.SPARQL, insert);
        update.execute();

        insert = "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n" +
                "PREFIX ex: <http://example/addresses#>\n" +
                "INSERT DATA\n" +
                "{ GRAPH ex:G2 {\n" +
                "<http://example/book3> dc:title    \"A new book\" ;\n" +
                "                         dc:creator  \"A.N.Other\" .\n" +
                "}\n" +
                "}";
        update = conn.prepareUpdate(QueryLanguage.SPARQL, insert);
        update.execute();

        String query = "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n" +
                "select * where { <http://example/book3> ?p ?o. }";
        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        CountTupleHandler tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(4, tupleHandler.getCount());

        tupleHandler = new CountTupleHandler();
        conn.clear();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(0, tupleHandler.getCount());

        conn.close();
    }

    public void testDropGraph() throws Exception {
        RepositoryConnection conn = repository.getConnection();

        String insert = "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n" +
                "PREFIX ex: <http://example/addresses#>\n" +
                "INSERT DATA\n" +
                "{ GRAPH ex:G1 {\n" +
                "<http://example/book3> dc:title    \"A new book\" ;\n" +
                "                         dc:creator  \"A.N.Other\" .\n" +
                "}\n" +
                "}";
        Update update = conn.prepareUpdate(QueryLanguage.SPARQL, insert);
        update.execute();

        insert = "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n" +
                "PREFIX ex: <http://example/addresses#>\n" +
                "INSERT DATA\n" +
                "{ GRAPH ex:G2 {\n" +
                "<http://example/book3> dc:title    \"A new book\" ;\n" +
                "                         dc:creator  \"A.N.Other\" .\n" +
                "}\n" +
                "}";
        update = conn.prepareUpdate(QueryLanguage.SPARQL, insert);
        update.execute();

        String query = "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n" +
                "select * where { <http://example/book3> ?p ?o. }";
        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        CountTupleHandler tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(4, tupleHandler.getCount());

        tupleHandler = new CountTupleHandler();
        String drop = "PREFIX ex: <http://example/addresses#>\n" +
                "DROP GRAPH ex:G2 ";
        update = conn.prepareUpdate(QueryLanguage.SPARQL, drop);
        update.execute();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(2, tupleHandler.getCount());

        tupleHandler = new CountTupleHandler();
        drop = "PREFIX ex: <http://example/addresses#>\n" +
                "DROP GRAPH ex:G1 ";
        update = conn.prepareUpdate(QueryLanguage.SPARQL, drop);
        update.execute();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(0, tupleHandler.getCount());

        conn.close();
    }

    public static class CountTupleHandler implements TupleQueryResultHandler {

        int count = 0;

        @Override
        public void startQueryResult(List<String> strings) throws TupleQueryResultHandlerException {
        }

        @Override
        public void endQueryResult() throws TupleQueryResultHandlerException {
        }

        @Override
        public void handleSolution(BindingSet bindingSet) throws TupleQueryResultHandlerException {
            count++;
        }

        public int getCount() {
            return count;
        }

        @Override
        public void handleBoolean(boolean arg0) throws QueryResultHandlerException {
        }

        @Override
        public void handleLinks(List<String> arg0) throws QueryResultHandlerException {
        }
    }

    private static class PrintTupleHandler implements TupleQueryResultHandler {


        @Override
        public void startQueryResult(List<String> strings) throws TupleQueryResultHandlerException {
        }

        @Override
        public void endQueryResult() throws TupleQueryResultHandlerException {
        }

        @Override
        public void handleSolution(BindingSet bindingSet) throws TupleQueryResultHandlerException {
            System.out.println(bindingSet);
        }

        @Override
        public void handleBoolean(boolean arg0) throws QueryResultHandlerException {
        }

        @Override
        public void handleLinks(List<String> arg0) throws QueryResultHandlerException {
        }
    }

    public class MockRdfCloudStore extends RdfCloudTripleStore {

        public MockRdfCloudStore() {
            super();
            Instance instance = new MockInstance();
            try {
                AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
                conf.setInfer(true);
                setConf(conf);
                Connector connector = instance.getConnector("", "");
                AccumuloRyaDAO cdao = new AccumuloRyaDAO();
                cdao.setConf(conf);
                cdao.setConnector(connector);
                setRyaDAO(cdao);
                inferenceEngine = new InferenceEngine();
                inferenceEngine.setRyaDAO(cdao);
                inferenceEngine.setRefreshGraphSchedule(5000); //every 5 sec
                inferenceEngine.setConf(conf);
                setInferenceEngine(inferenceEngine);
                internalInferenceEngine = inferenceEngine;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
