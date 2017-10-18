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
package org.apache.rya;

import java.io.InputStream;
import java.util.List;

import junit.framework.TestCase;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.api.RdfCloudTripleStoreConstants;
import org.apache.rya.api.RdfTripleStoreConfiguration;
import org.apache.rya.rdftriplestore.RdfCloudTripleStore;
import org.apache.rya.rdftriplestore.RyaSailRepository;
import org.apache.rya.rdftriplestore.inference.InferenceEngine;
import org.apache.rya.rdftriplestore.namespace.NamespaceManager;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.impl.StatementImpl;
import org.eclipse.rdf4j.model.impl.URIImpl;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.query.*;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;

import static org.apache.rya.api.RdfCloudTripleStoreConstants.NAMESPACE;

/**
 * Class RdfCloudTripleStoreConnectionTest
 * Date: Mar 3, 2011
 * Time: 12:03:29 PM
 */
public class RdfCloudTripleStoreConnectionTest extends TestCase {
    private Repository repository;
    SimpleValueFactory vf = SimpleValueFactory.getInstance();
    private InferenceEngine internalInferenceEngine;

    static String litdupsNS = "urn:test:litdups#";
    IRI cpu = vf.createIRI(litdupsNS, "cpu");
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

        IRI loadPerc = vf.createIRI(litdupsNS, "loadPerc");
        IRI uri1 = vf.createIRI(litdupsNS, "uri1");
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
        result = conn.getStatements(cpu, loadPerc, null, true);
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
//        URI cpu = vf.createIRI(litdupsNS, "cpu");
//        URI loadPerc = vf.createIRI(litdupsNS, "loadPerc");
//        URI uri1 = vf.createIRI(litdupsNS, "uri1");
//        URI uri2 = vf.createIRI(litdupsNS, "uri2");
//        URI uri3 = vf.createIRI(litdupsNS, "uri3");
//        URI auth1 = vf.createIRI(RdfCloudTripleStoreConstants.AUTH_NAMESPACE, "1");
//        URI auth2 = vf.createIRI(RdfCloudTripleStoreConstants.AUTH_NAMESPACE, "2");
//        URI auth3 = vf.createIRI(RdfCloudTripleStoreConstants.AUTH_NAMESPACE, "3");
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
//        tupleQuery.setBinding(RdfTripleStoreConfiguration.CONF_QUERY_AUTH, vf.createLiteral("2"));
//        CountTupleHandler cth = new CountTupleHandler();
//        tupleQuery.evaluate(cth);
//        assertEquals(2, cth.getCount());
//
//        conn.close();
//    }

    public void testEvaluate() throws Exception {
        RepositoryConnection conn = repository.getConnection();
        IRI loadPerc = vf.createIRI(litdupsNS, "loadPerc");
        IRI uri1 = vf.createIRI(litdupsNS, "uri1");
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
        IRI loadPerc = vf.createIRI(litdupsNS, "loadPerc");
        IRI uri1 = vf.createIRI(litdupsNS, "uri1");
        IRI pred2 = vf.createIRI(litdupsNS, "pred2");
        IRI uri2 = vf.createIRI(litdupsNS, "uri2");
        conn.add(cpu, loadPerc, uri1);
        conn.add(cpu, pred2, uri2);
        conn.commit();

        String query = "select * where {" +
                "?x <" + loadPerc.stringValue() + "> ?o1." +
                "?x <" + pred2.stringValue() + "> ?o2." +
                "}";
        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        tupleQuery.setBinding(RdfTripleStoreConfiguration.CONF_QUERYPLAN_FLAG, RdfCloudTripleStoreConstants.VALUE_FACTORY.createLiteral(true));
        CountTupleHandler cth = new CountTupleHandler();
        tupleQuery.evaluate(cth);
        conn.close();
        assertEquals(cth.getCount(), 1);
    }

    public void testPOObjRange() throws Exception {
        RepositoryConnection conn = repository.getConnection();
        IRI loadPerc = vf.createIRI(litdupsNS, "loadPerc");
        Literal six = vf.createLiteral("6");
        Literal sev = vf.createLiteral("7");
        Literal ten = vf.createLiteral("10");
        conn.add(cpu, loadPerc, six);
        conn.add(cpu, loadPerc, sev);
        conn.add(cpu, loadPerc, ten);
        conn.commit();

        String query = "PREFIX org.apache: <" + NAMESPACE + ">\n" +
                "select * where {" +
                "?x <" + loadPerc.stringValue() + "> ?o.\n" +
                "FILTER(org.apache:range(?o, '6', '8'))." +
                "}";
        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        CountTupleHandler cth = new CountTupleHandler();
        tupleQuery.evaluate(cth);
        conn.close();
        assertEquals(2, cth.getCount());
    }

    public void testPOPredRange() throws Exception {
        RepositoryConnection conn = repository.getConnection();
        IRI loadPerc = vf.createIRI(litdupsNS, "loadPerc1");
        IRI loadPerc2 = vf.createIRI(litdupsNS, "loadPerc2");
        IRI loadPerc3 = vf.createIRI(litdupsNS, "loadPerc3");
        IRI loadPerc4 = vf.createIRI(litdupsNS, "loadPerc4");
        Literal six = vf.createLiteral("6");
        Literal sev = vf.createLiteral("7");
        Literal ten = vf.createLiteral("10");
        conn.add(cpu, loadPerc, six);
        conn.add(cpu, loadPerc2, sev);
        conn.add(cpu, loadPerc4, ten);
        conn.commit();

        String query = "PREFIX org.apache: <" + NAMESPACE + ">\n" +
                "select * where {" +
                "?x ?p ?o.\n" +
                "FILTER(org.apache:range(?p, <" + loadPerc.stringValue() + ">, <" + loadPerc3.stringValue() + ">))." +
                "}";
        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        CountTupleHandler cth = new CountTupleHandler();
        tupleQuery.evaluate(cth);
        conn.close();
        assertEquals(cth.getCount(), 2);
    }

    public void testSPOPredRange() throws Exception {
        RepositoryConnection conn = repository.getConnection();
        IRI loadPerc = vf.createIRI(litdupsNS, "loadPerc1");
        IRI loadPerc2 = vf.createIRI(litdupsNS, "loadPerc2");
        IRI loadPerc3 = vf.createIRI(litdupsNS, "loadPerc3");
        IRI loadPerc4 = vf.createIRI(litdupsNS, "loadPerc4");
        Literal six = vf.createLiteral("6");
        Literal sev = vf.createLiteral("7");
        Literal ten = vf.createLiteral("10");
        conn.add(cpu, loadPerc, six);
        conn.add(cpu, loadPerc2, sev);
        conn.add(cpu, loadPerc4, ten);
        conn.commit();

        String query = "PREFIX org.apache: <" + NAMESPACE + ">\n" +
                "select * where {" +
                "<" + cpu.stringValue() + "> ?p ?o.\n" +
                "FILTER(org.apache:range(?p, <" + loadPerc.stringValue() + ">, <" + loadPerc3.stringValue() + ">))." +
                "}";
        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        CountTupleHandler cth = new CountTupleHandler();
        tupleQuery.evaluate(cth);
        conn.close();
        assertEquals(2, cth.getCount());
    }

    public void testSPOSubjRange() throws Exception {
        RepositoryConnection conn = repository.getConnection();
        IRI cpu2 = vf.createIRI(litdupsNS, "cpu2");
        IRI cpu3 = vf.createIRI(litdupsNS, "cpu3");
        IRI loadPerc = vf.createIRI(litdupsNS, "loadPerc");
        Literal six = vf.createLiteral("6");
        Literal sev = vf.createLiteral("7");
        Literal ten = vf.createLiteral("10");
        conn.add(cpu, loadPerc, six);
        conn.add(cpu2, loadPerc, sev);
        conn.add(cpu3, loadPerc, ten);
        conn.commit();

        String query = "PREFIX org.apache: <" + NAMESPACE + ">\n" +
                "select * where {" +
                "?s ?p ?o.\n" +
                "FILTER(org.apache:range(?s, <" + cpu.stringValue() + ">, <" + cpu2.stringValue() + ">))." +
                "}";
        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        CountTupleHandler cth = new CountTupleHandler();
        tupleQuery.evaluate(cth);
        conn.close();
        assertEquals(cth.getCount(), 2);
    }

    public void testSPOObjRange() throws Exception {
        RepositoryConnection conn = repository.getConnection();
        IRI loadPerc = vf.createIRI(litdupsNS, "loadPerc");
        Literal six = vf.createLiteral("6");
        Literal sev = vf.createLiteral("7");
        Literal ten = vf.createLiteral("10");
        conn.add(cpu, loadPerc, six);
        conn.add(cpu, loadPerc, sev);
        conn.add(cpu, loadPerc, ten);
        conn.commit();

        String query = "PREFIX org.apache: <" + NAMESPACE + ">\n" +
                "select * where {" +
                "<" + cpu.stringValue() + "> <" + loadPerc.stringValue() + "> ?o.\n" +
                "FILTER(org.apache:range(?o, '6', '8'))." +
                "}";
        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        CountTupleHandler cth = new CountTupleHandler();
        tupleQuery.evaluate(cth);
        conn.close();
        assertEquals(cth.getCount(), 2);
    }

    public void testOSPObjRange() throws Exception {
        RepositoryConnection conn = repository.getConnection();
        IRI loadPerc = vf.createIRI(litdupsNS, "loadPerc");
        Literal six = vf.createLiteral("6");
        Literal sev = vf.createLiteral("7");
        Literal ten = vf.createLiteral("10");
        conn.add(cpu, loadPerc, six);
        conn.add(cpu, loadPerc, sev);
        conn.add(cpu, loadPerc, ten);
        conn.commit();

        String query = "PREFIX org.apache: <" + NAMESPACE + ">\n" +
                "select * where {" +
                "?s ?p ?o.\n" +
                "FILTER(org.apache:range(?o, '6', '8'))." +
                "}";
        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        CountTupleHandler cth = new CountTupleHandler();
        tupleQuery.evaluate(cth);
        conn.close();
        assertEquals(cth.getCount(), 2);
    }

    public void testRegexFilter() throws Exception {
        RepositoryConnection conn = repository.getConnection();
        IRI loadPerc = vf.createIRI(litdupsNS, "loadPerc");
        IRI testClass = vf.createIRI(litdupsNS, "test");
        Literal six = vf.createLiteral("6");
        Literal sev = vf.createLiteral("7");
        Literal ten = vf.createLiteral("10");
        conn.add(cpu, loadPerc, six);
        conn.add(cpu, loadPerc, sev);
        conn.add(cpu, loadPerc, ten);
        conn.add(cpu, RDF.TYPE, testClass);
        conn.commit();

        String query = "PREFIX org.apache: <" + NAMESPACE + ">\n" +
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
        IRI loadPerc = vf.createIRI(litdupsNS, "testPred");
        IRI uri1 = vf.createIRI(litdupsNS, "uri1");
        conn.add(cpu, loadPerc, uri1);
        conn.commit();

        RepositoryResult<Statement> result = conn.getStatements(cpu, loadPerc, null, false);
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

        IRI loadPerc = vf.createIRI(litdupsNS, "loadPerc");
        Literal lit1 = vf.createLiteral(0.0);
        Literal lit2 = vf.createLiteral(0.0);
        Literal lit3 = vf.createLiteral(0.0);

        conn.add(cpu, loadPerc, lit1);
        conn.add(cpu, loadPerc, lit2);
        conn.add(cpu, loadPerc, lit3);
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
        conn.remove(cpu, loadPerc, lit1);
        conn.close();
    }

    public void testNotDuplicateUris() throws Exception {
        RepositoryConnection conn = repository.getConnection();

        IRI loadPerc = vf.createIRI(litdupsNS, "loadPerc");
        IRI uri1 = vf.createIRI(litdupsNS, "uri1");
        IRI uri2 = vf.createIRI(litdupsNS, "uri1");
        IRI uri3 = vf.createIRI(litdupsNS, "uri1");

        conn.add(cpu, loadPerc, uri1);
        conn.add(cpu, loadPerc, uri2);
        conn.add(cpu, loadPerc, uri3);
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
        conn.close();
    }

    public void testNamespaceUsage() throws Exception {
        RepositoryConnection conn = repository.getConnection();
        conn.setNamespace("lit", litdupsNS);
        IRI loadPerc = vf.createIRI(litdupsNS, "loadPerc");
        final IRI uri1 = vf.createIRI(litdupsNS, "uri1");
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
        conn.add(new StatementImpl(vf.createIRI(litdupsNS, "undergradDegreeFrom"), RDFS.SUBPROPERTYOF, vf.createIRI(litdupsNS, "degreeFrom")));
        conn.add(new StatementImpl(vf.createIRI(litdupsNS, "gradDegreeFrom"), RDFS.SUBPROPERTYOF, vf.createIRI(litdupsNS, "degreeFrom")));
        conn.add(new StatementImpl(vf.createIRI(litdupsNS, "degreeFrom"), RDFS.SUBPROPERTYOF, vf.createIRI(litdupsNS, "memberOf")));
        conn.add(new StatementImpl(vf.createIRI(litdupsNS, "memberOf"), RDFS.SUBPROPERTYOF, vf.createIRI(litdupsNS, "associatedWith")));
        conn.add(new StatementImpl(vf.createIRI(litdupsNS, "UgradA"), vf.createIRI(litdupsNS, "undergradDegreeFrom"), vf.createIRI(litdupsNS, "Harvard")));
        conn.add(new StatementImpl(vf.createIRI(litdupsNS, "GradB"), vf.createIRI(litdupsNS, "gradDegreeFrom"), vf.createIRI(litdupsNS, "Yale")));
        conn.add(new StatementImpl(vf.createIRI(litdupsNS, "ProfessorC"), vf.createIRI(litdupsNS, "memberOf"), vf.createIRI(litdupsNS, "Harvard")));
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
        conn.add(new StatementImpl(vf.createIRI(litdupsNS, "undergradDegreeFrom"), OWL.EQUIVALENTPROPERTY, vf.createIRI(litdupsNS, "ugradDegreeFrom")));
        conn.add(new StatementImpl(vf.createIRI(litdupsNS, "UgradA"), vf.createIRI(litdupsNS, "undergradDegreeFrom"), vf.createIRI(litdupsNS, "Harvard")));
        conn.add(new StatementImpl(vf.createIRI(litdupsNS, "GradB"), vf.createIRI(litdupsNS, "ugradDegreeFrom"), vf.createIRI(litdupsNS, "Harvard")));
        conn.add(new StatementImpl(vf.createIRI(litdupsNS, "GradC"), vf.createIRI(litdupsNS, "ugraduateDegreeFrom"), vf.createIRI(litdupsNS, "Harvard")));
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
        conn.add(new StatementImpl(vf.createIRI(litdupsNS, "friendOf"), RDF.TYPE, OWL.SYMMETRICPROPERTY));
        conn.add(new StatementImpl(vf.createIRI(litdupsNS, "Bob"), vf.createIRI(litdupsNS, "friendOf"), vf.createIRI(litdupsNS, "Jeff")));
        conn.add(new StatementImpl(vf.createIRI(litdupsNS, "James"), vf.createIRI(litdupsNS, "friendOf"), vf.createIRI(litdupsNS, "Jeff")));
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
        conn.add(new StatementImpl(vf.createIRI(litdupsNS, "subRegionOf"), RDF.TYPE, OWL.TRANSITIVEPROPERTY));
        conn.add(new StatementImpl(vf.createIRI(litdupsNS, "Queens"), vf.createIRI(litdupsNS, "subRegionOf"), vf.createIRI(litdupsNS, "NYC")));
        conn.add(new StatementImpl(vf.createIRI(litdupsNS, "NYC"), vf.createIRI(litdupsNS, "subRegionOf"), vf.createIRI(litdupsNS, "NY")));
        conn.add(new StatementImpl(vf.createIRI(litdupsNS, "NY"), vf.createIRI(litdupsNS, "subRegionOf"), vf.createIRI(litdupsNS, "US")));
        conn.add(new StatementImpl(vf.createIRI(litdupsNS, "US"), vf.createIRI(litdupsNS, "subRegionOf"), vf.createIRI(litdupsNS, "NorthAmerica")));
        conn.add(new StatementImpl(vf.createIRI(litdupsNS, "NorthAmerica"), vf.createIRI(litdupsNS, "subRegionOf"), vf.createIRI(litdupsNS, "World")));
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
        conn.add(new StatementImpl(vf.createIRI(litdupsNS, "degreeFrom"), OWL.INVERSEOF, vf.createIRI(litdupsNS, "hasAlumnus")));
        conn.add(new StatementImpl(vf.createIRI(litdupsNS, "UgradA"), vf.createIRI(litdupsNS, "degreeFrom"), vf.createIRI(litdupsNS, "Harvard")));
        conn.add(new StatementImpl(vf.createIRI(litdupsNS, "GradB"), vf.createIRI(litdupsNS, "degreeFrom"), vf.createIRI(litdupsNS, "Harvard")));
        conn.add(new StatementImpl(vf.createIRI(litdupsNS, "Harvard"), vf.createIRI(litdupsNS, "hasAlumnus"), vf.createIRI(litdupsNS, "AlumC")));
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
        conn.add(new StatementImpl(vf.createIRI(litdupsNS, "UndergraduateStudent"), RDFS.SUBCLASSOF, vf.createIRI(litdupsNS, "Student")));
        conn.add(new StatementImpl(vf.createIRI(litdupsNS, "Student"), RDFS.SUBCLASSOF, vf.createIRI(litdupsNS, "Person")));
        conn.add(new StatementImpl(vf.createIRI(litdupsNS, "UgradA"), RDF.TYPE, vf.createIRI(litdupsNS, "UndergraduateStudent")));
        conn.add(new StatementImpl(vf.createIRI(litdupsNS, "StudentB"), RDF.TYPE, vf.createIRI(litdupsNS, "Student")));
        conn.add(new StatementImpl(vf.createIRI(litdupsNS, "PersonC"), RDF.TYPE, vf.createIRI(litdupsNS, "Person")));
        conn.commit();
        conn.close();

        internalInferenceEngine.refreshGraph();

        conn = repository.getConnection();

        //simple api first
        RepositoryResult<Statement> person = conn.getStatements(null, RDF.TYPE, vf.createIRI(litdupsNS, "Person"), true);
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
        conn.add(new StatementImpl(vf.createIRI(litdupsNS, "StudentA1"), OWL.SAMEAS, vf.createIRI(litdupsNS, "StudentA2")));
        conn.add(new StatementImpl(vf.createIRI(litdupsNS, "StudentA2"), OWL.SAMEAS, vf.createIRI(litdupsNS, "StudentA3")));
        conn.add(new StatementImpl(vf.createIRI(litdupsNS, "StudentB1"), OWL.SAMEAS, vf.createIRI(litdupsNS, "StudentB2")));
        conn.add(new StatementImpl(vf.createIRI(litdupsNS, "StudentB2"), OWL.SAMEAS, vf.createIRI(litdupsNS, "StudentB3")));
        conn.add(new StatementImpl(vf.createIRI(litdupsNS, "StudentA1"), vf.createIRI(litdupsNS, "pred1"), vf.createIRI(litdupsNS, "StudentB3")));
        conn.add(new StatementImpl(vf.createIRI(litdupsNS, "StudentB1"), vf.createIRI(litdupsNS, "pred2"), vf.createIRI(litdupsNS, "StudentA3")));
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

        RepositoryResult<Statement> statements = conn.getStatements(null, vf.createIRI("http://www.example.org/vocabulary#name"), null, true, vf.createIRI("http://www.example.org/exampleDocument#G1"));
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
//        URI auth1 = vf.createIRI(RdfCloudTripleStoreConstants.AUTH_NAMESPACE, "1");
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
//        tupleQuery.setBinding(RdfTripleStoreConfiguration.CONF_QUERY_AUTH, vf.createLiteral("1"));
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
        if (r instanceof IRI) {
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
        tupleQuery.setBinding(RdfTripleStoreConfiguration.CONF_QUERY_AUTH, vf.createLiteral("2"));
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
        tupleQuery.setBinding(RdfTripleStoreConfiguration.CONF_QUERY_AUTH, vf.createLiteral("2"));
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
