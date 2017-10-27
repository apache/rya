package org.apache.rya.rdftriplestore.evaluation;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.domain.StatementMetadata;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.rdftriplestore.RdfCloudTripleStoreConnection.StoreTripleSource;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.eclipse.rdf4j.query.algebra.helpers.StatementPatternCollector;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class StatementPatternEvalTest {
    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    private AccumuloRyaDAO dao;
    private AccumuloRdfConfiguration conf;
    private ParallelEvaluationStrategyImpl eval;

    @Before
    public void init() throws Exception {
        conf = getConf();
        Instance mock = new MockInstance("instance");
        Connector conn = mock.getConnector("root", new PasswordToken(""));
        dao = new AccumuloRyaDAO();
        dao.setConnector(conn);
        dao.init();
        eval = new ParallelEvaluationStrategyImpl(new StoreTripleSource(conf, dao), null, null, conf);
    }

    @After
    public void close() throws RyaDAOException {
        eval.shutdown();
        dao.destroy();
    }
    
    @Test
    public void simpleQueryWithoutBindingSets()
            throws MalformedQueryException, QueryEvaluationException, RyaDAOException {
        //query is used to build statement that will be evaluated
        String query = "select ?x ?c where{ graph ?c  {?x <uri:talksTo> <uri:Bob>. }}";
        SPARQLParser parser = new SPARQLParser();
        ParsedQuery pq = parser.parseQuery(query, null);
        List<StatementPattern> spList = StatementPatternCollector.process(pq.getTupleExpr());
        
        RyaStatement statement1 = new RyaStatement(new RyaURI("uri:Joe"), new RyaURI("uri:talksTo"),
                new RyaType("uri:Bob"), new RyaURI("uri:context1"), "", new StatementMetadata());
        dao.add(statement1);
        
        RyaStatement statement2 = new RyaStatement(new RyaURI("uri:Doug"), new RyaURI("uri:talksTo"),
                new RyaType("uri:Bob"), new RyaURI("uri:context2"), "", new StatementMetadata());
        dao.add(statement2);
        
        RyaStatement statement3 = new RyaStatement(new RyaURI("uri:Eric"), new RyaURI("uri:talksTo"),
                new RyaType("uri:Bob"), new RyaURI("uri:context3"), "", new StatementMetadata());
        dao.add(statement3);

        QueryBindingSet bsConstraint1 = new QueryBindingSet();
        
        CloseableIteration<BindingSet, QueryEvaluationException> iteration = eval.evaluate(spList.get(0), Arrays.asList(bsConstraint1));

        List<BindingSet> bsList = new ArrayList<>();
        while (iteration.hasNext()) {
            bsList.add(iteration.next());
        }

        Assert.assertEquals(3, bsList.size());
        
        QueryBindingSet expected1 = new QueryBindingSet();
        expected1.addBinding("x", VF.createIRI("uri:Joe"));
        expected1.addBinding("c", VF.createIRI("uri:context1"));

        QueryBindingSet expected2 = new QueryBindingSet();
        expected2.addBinding("x", VF.createIRI("uri:Doug"));
        expected2.addBinding("c", VF.createIRI("uri:context2"));
        
        QueryBindingSet expected3 = new QueryBindingSet();
        expected3.addBinding("x", VF.createIRI("uri:Eric"));
        expected3.addBinding("c", VF.createIRI("uri:context3"));
        
        Set<BindingSet> expected = new HashSet<>(Arrays.asList(expected1, expected2, expected3));
        Set<BindingSet> actual = new HashSet<>(bsList);
        
        Assert.assertEquals(expected, actual);
        dao.delete(Arrays.asList(statement1, statement2, statement3).iterator(), conf);
    }

    @Test
    public void simpleQueryWithBindingSets()
            throws MalformedQueryException, QueryEvaluationException, RyaDAOException {
        //query is used to build statement that will be evaluated
        String query = "select ?x ?c where{ graph ?c  {?x <uri:talksTo> <uri:Bob>. }}";
        SPARQLParser parser = new SPARQLParser();
        ParsedQuery pq = parser.parseQuery(query, null);
        List<StatementPattern> spList = StatementPatternCollector.process(pq.getTupleExpr());
        
        RyaStatement statement1 = new RyaStatement(new RyaURI("uri:Joe"), new RyaURI("uri:talksTo"),
                new RyaType("uri:Bob"), new RyaURI("uri:context1"), "", new StatementMetadata());
        dao.add(statement1);
        
        RyaStatement statement2 = new RyaStatement(new RyaURI("uri:Doug"), new RyaURI("uri:talksTo"),
                new RyaType("uri:Bob"), new RyaURI("uri:context2"), "", new StatementMetadata());
        dao.add(statement2);
        
        RyaStatement statement3 = new RyaStatement(new RyaURI("uri:Eric"), new RyaURI("uri:talksTo"),
                new RyaType("uri:Bob"), new RyaURI("uri:context3"), "", new StatementMetadata());
        dao.add(statement3);

        QueryBindingSet bsConstraint1 = new QueryBindingSet();
        bsConstraint1.addBinding("c", VF.createIRI("uri:context2"));
        
        QueryBindingSet bsConstraint2 = new QueryBindingSet();
        bsConstraint2.addBinding("c", VF.createIRI("uri:context1"));

        
        CloseableIteration<BindingSet, QueryEvaluationException> iteration = eval.evaluate(spList.get(0), Arrays.asList(bsConstraint1, bsConstraint2));

        List<BindingSet> bsList = new ArrayList<>();
        while (iteration.hasNext()) {
            bsList.add(iteration.next());
        }

        Assert.assertEquals(2, bsList.size());
        
        QueryBindingSet expected1 = new QueryBindingSet();
        expected1.addBinding("x", VF.createIRI("uri:Joe"));
        expected1.addBinding("c", VF.createIRI("uri:context1"));

        QueryBindingSet expected2 = new QueryBindingSet();
        expected2.addBinding("x", VF.createIRI("uri:Doug"));
        expected2.addBinding("c", VF.createIRI("uri:context2"));
        
        Set<BindingSet> expected = new HashSet<>(Arrays.asList(expected1, expected2));
        Set<BindingSet> actual = new HashSet<>(bsList);
        
        Assert.assertEquals(expected, actual);
        
        dao.delete(Arrays.asList(statement1, statement2, statement3).iterator(), conf);
    }

    
    @Test
    public void simpleQueryWithBindingSetSameContext()
            throws MalformedQueryException, QueryEvaluationException, RyaDAOException {
        //query is used to build statement that will be evaluated
        String query = "select ?x ?c where{ graph ?c  {?x <uri:talksTo> <uri:Bob>. }}";
        SPARQLParser parser = new SPARQLParser();
        ParsedQuery pq = parser.parseQuery(query, null);
        List<StatementPattern> spList = StatementPatternCollector.process(pq.getTupleExpr());
        
        RyaStatement statement1 = new RyaStatement(new RyaURI("uri:Joe"), new RyaURI("uri:talksTo"),
                new RyaType("uri:Bob"), new RyaURI("uri:context1"), "", new StatementMetadata());
        dao.add(statement1);
        
        RyaStatement statement2 = new RyaStatement(new RyaURI("uri:Doug"), new RyaURI("uri:talksTo"),
                new RyaType("uri:Bob"), new RyaURI("uri:context2"), "", new StatementMetadata());
        dao.add(statement2);
        
        RyaStatement statement3 = new RyaStatement(new RyaURI("uri:Eric"), new RyaURI("uri:talksTo"),
                new RyaType("uri:Bob"), new RyaURI("uri:context3"), "", new StatementMetadata());
        dao.add(statement3);

        QueryBindingSet bsConstraint1 = new QueryBindingSet();
        bsConstraint1.addBinding("c", VF.createIRI("uri:context1"));
        
        QueryBindingSet bsConstraint2 = new QueryBindingSet();
        bsConstraint2.addBinding("c", VF.createIRI("uri:context1"));

        
        CloseableIteration<BindingSet, QueryEvaluationException> iteration = eval.evaluate(spList.get(0), Arrays.asList(bsConstraint1, bsConstraint2));

        List<BindingSet> bsList = new ArrayList<>();
        while (iteration.hasNext()) {
            bsList.add(iteration.next());
        }

        Assert.assertEquals(1, bsList.size());
        
        QueryBindingSet expected = new QueryBindingSet();
        expected.addBinding("x", VF.createIRI("uri:Joe"));
        expected.addBinding("c", VF.createIRI("uri:context1"));
        
        Assert.assertEquals(expected, bsList.get(0));

        dao.delete(Arrays.asList(statement1, statement2, statement3).iterator(), conf);
    }
    
    @Test
    public void simpleQueryNoBindingSetConstantContext()
            throws MalformedQueryException, QueryEvaluationException, RyaDAOException {
        //query is used to build statement that will be evaluated
        String query = "select ?x ?c where{ graph <uri:context1>  {?x <uri:talksTo> <uri:Bob>. }}";
        SPARQLParser parser = new SPARQLParser();
        ParsedQuery pq = parser.parseQuery(query, null);
        List<StatementPattern> spList = StatementPatternCollector.process(pq.getTupleExpr());
        
        RyaStatement statement1 = new RyaStatement(new RyaURI("uri:Joe"), new RyaURI("uri:talksTo"),
                new RyaType("uri:Bob"), new RyaURI("uri:context1"), "", new StatementMetadata());
        dao.add(statement1);
        
        RyaStatement statement2 = new RyaStatement(new RyaURI("uri:Doug"), new RyaURI("uri:talksTo"),
                new RyaType("uri:Bob"), new RyaURI("uri:context2"), "", new StatementMetadata());
        dao.add(statement2);
        
        RyaStatement statement3 = new RyaStatement(new RyaURI("uri:Eric"), new RyaURI("uri:talksTo"),
                new RyaType("uri:Bob"), new RyaURI("uri:context3"), "", new StatementMetadata());
        dao.add(statement3);

        QueryBindingSet bsConstraint1 = new QueryBindingSet();
        
        CloseableIteration<BindingSet, QueryEvaluationException> iteration = eval.evaluate(spList.get(0), Arrays.asList(bsConstraint1));

        List<BindingSet> bsList = new ArrayList<>();
        while (iteration.hasNext()) {
            bsList.add(iteration.next());
        }
        
        Assert.assertEquals(1, bsList.size());
       
        QueryBindingSet expected = new QueryBindingSet();
        expected.addBinding("x", VF.createIRI("uri:Joe"));
        
        Assert.assertEquals(expected, bsList.get(0));
        
        dao.delete(Arrays.asList(statement1, statement2, statement3).iterator(), conf);
    }

    @Test
    public void simpleQueryWithBindingSetConstantContext()
            throws MalformedQueryException, QueryEvaluationException, RyaDAOException {
        //query is used to build statement that will be evaluated
        String query = "select ?x ?c where{ graph <uri:context1>  {?x <uri:talksTo> <uri:Bob>. }}";
        SPARQLParser parser = new SPARQLParser();
        ParsedQuery pq = parser.parseQuery(query, null);
        List<StatementPattern> spList = StatementPatternCollector.process(pq.getTupleExpr());
        
        RyaStatement statement1 = new RyaStatement(new RyaURI("uri:Joe"), new RyaURI("uri:talksTo"),
                new RyaType("uri:Bob"), new RyaURI("uri:context1"), "", new StatementMetadata());
        dao.add(statement1);
        
        RyaStatement statement2 = new RyaStatement(new RyaURI("uri:Doug"), new RyaURI("uri:talksTo"),
                new RyaType("uri:Bob"), new RyaURI("uri:context1"), "", new StatementMetadata());
        dao.add(statement2);
        
        RyaStatement statement3 = new RyaStatement(new RyaURI("uri:Doug"), new RyaURI("uri:talksTo"),
                new RyaType("uri:Bob"), new RyaURI("uri:context2"), "", new StatementMetadata());
        dao.add(statement3);

        QueryBindingSet bsConstraint1 = new QueryBindingSet();
        bsConstraint1.addBinding("x", VF.createIRI("uri:Doug"));
        
        CloseableIteration<BindingSet, QueryEvaluationException> iteration = eval.evaluate(spList.get(0), Arrays.asList(bsConstraint1));

        List<BindingSet> bsList = new ArrayList<>();
        while (iteration.hasNext()) {
            bsList.add(iteration.next());
        }

        Assert.assertEquals(1, bsList.size());
        
        QueryBindingSet expected = new QueryBindingSet();
        expected.addBinding("x", VF.createIRI("uri:Doug"));

        Assert.assertEquals(expected, bsList.get(0));
        
        dao.delete(Arrays.asList(statement1, statement2, statement3).iterator(), conf);
    }

    
    private static AccumuloRdfConfiguration getConf() {

        final AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();

        conf.set(RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX, "rya_");
        conf.set("sc.cloudbase.username", "root");
        conf.set("sc.cloudbase.password", "");
        conf.set("sc.cloudbase.instancename", "instance");
        conf.set("sc.cloudbase.authorizations", "");

        return conf;
    }


}

