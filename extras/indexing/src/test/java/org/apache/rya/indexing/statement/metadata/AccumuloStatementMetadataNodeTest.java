package org.apache.rya.indexing.statement.metadata;

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
import java.util.List;

import org.apache.accumulo.core.client.Connector;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.api.domain.StatementMetadata;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.statement.metadata.matching.StatementMetadataNode;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
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

public class AccumuloStatementMetadataNodeTest {
    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    private AccumuloRyaDAO dao;
    private AccumuloRdfConfiguration conf;
    private final String query = "prefix owl: <http://www.w3.org/2002/07/owl#> prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> select ?x ?y where {_:blankNode rdf:type owl:Annotation; owl:annotatedSource <http://Joe>; "
            + "owl:annotatedProperty <http://worksAt>; owl:annotatedTarget ?x; <http://createdBy> ?y; <http://createdOn> \'2017-01-04\'^^xsd:date }";
    private final String query2 = "prefix owl: <http://www.w3.org/2002/07/owl#> prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> select ?x ?y where {_:blankNode rdf:type owl:Annotation; owl:annotatedSource ?x; "
            + "owl:annotatedProperty <http://worksAt>; owl:annotatedTarget ?y; <http://createdBy> ?x; <http://createdOn> \'2017-01-04\'^^xsd:date }";

    @Before
    public void init() throws Exception {
        conf = getConf();
        Connector conn = ConfigUtils.getConnector(conf);
        dao = new AccumuloRyaDAO();
        dao.setConnector(conn);
        dao.init();
    }

    @After
    public void close() throws RyaDAOException {
        dao.destroy();
    }

    @Test
    public void simpleQueryWithoutBindingSet()
            throws MalformedQueryException, QueryEvaluationException, RyaDAOException {
        StatementMetadata metadata = new StatementMetadata();
        metadata.addMetadata(new RyaIRI("http://createdBy"), new RyaType("Joe"));
        metadata.addMetadata(new RyaIRI("http://createdOn"), new RyaType(XMLSchema.DATE, "2017-01-04"));

        RyaStatement statement = new RyaStatement(new RyaIRI("http://Joe"), new RyaIRI("http://worksAt"),
                new RyaType("CoffeeShop"), new RyaIRI("http://context"), "", metadata);
        dao.add(statement);

        SPARQLParser parser = new SPARQLParser();
        ParsedQuery pq = parser.parseQuery(query, null);
        List<StatementPattern> spList = StatementPatternCollector.process(pq.getTupleExpr());
        // RyaQueryEngine<RdfCloudTripleStoreConfiguration> engine =
        // (RyaQueryEngine<>) dao.getQueryEngine();

        StatementMetadataNode<?> node = new StatementMetadataNode<>(spList, conf);
        CloseableIteration<BindingSet, QueryEvaluationException> iteration = node.evaluate(new QueryBindingSet());

        QueryBindingSet bs = new QueryBindingSet();
        bs.addBinding("x", VF.createLiteral("CoffeeShop"));
        bs.addBinding("y", VF.createLiteral("Joe"));

        List<BindingSet> bsList = new ArrayList<>();
        while (iteration.hasNext()) {
            bsList.add(iteration.next());
        }

        Assert.assertEquals(1, bsList.size());
        Assert.assertEquals(bs, bsList.get(0));
        dao.delete(statement, conf);
    }

    /**
     * Tests if results are filtered correctly using the metadata properties. In
     * this case, the date for the ingested RyaStatement differs from the date
     * specified in the query.
     * 
     * @throws MalformedQueryException
     * @throws QueryEvaluationException
     * @throws RyaDAOException
     */
    @Test
    public void simpleQueryWithoutBindingSetInvalidProperty()
            throws MalformedQueryException, QueryEvaluationException, RyaDAOException {
        StatementMetadata metadata = new StatementMetadata();
        metadata.addMetadata(new RyaIRI("http://createdBy"), new RyaType("Doug"));
        metadata.addMetadata(new RyaIRI("http://createdOn"), new RyaType(XMLSchema.DATE, "2017-02-15"));

        RyaStatement statement = new RyaStatement(new RyaIRI("http://Joe"), new RyaIRI("http://worksAt"),
                new RyaType("CoffeeShop"), new RyaIRI("http://context"), "", metadata);
        dao.add(statement);

        SPARQLParser parser = new SPARQLParser();
        ParsedQuery pq = parser.parseQuery(query, null);
        List<StatementPattern> spList = StatementPatternCollector.process(pq.getTupleExpr());
        StatementMetadataNode<AccumuloRdfConfiguration> node = new StatementMetadataNode<>(spList, conf);
        CloseableIteration<BindingSet, QueryEvaluationException> iteration = node.evaluate(new QueryBindingSet());

        List<BindingSet> bsList = new ArrayList<>();
        while (iteration.hasNext()) {
            bsList.add(iteration.next());
        }
        Assert.assertEquals(0, bsList.size());
        dao.delete(statement, conf);
    }

    @Test
    public void simpleQueryWithBindingSet() throws MalformedQueryException, QueryEvaluationException, RyaDAOException {

        StatementMetadata metadata = new StatementMetadata();
        metadata.addMetadata(new RyaIRI("http://createdBy"), new RyaType("Joe"));
        metadata.addMetadata(new RyaIRI("http://createdOn"), new RyaType(XMLSchema.DATE, "2017-01-04"));

        RyaStatement statement1 = new RyaStatement(new RyaIRI("http://Joe"), new RyaIRI("http://worksAt"),
                new RyaType("CoffeeShop"), new RyaIRI("http://context"), "", metadata);
        RyaStatement statement2 = new RyaStatement(new RyaIRI("http://Joe"), new RyaIRI("http://worksAt"),
                new RyaType("HardwareStore"), new RyaIRI("http://context"), "", metadata);
        dao.add(statement1);
        dao.add(statement2);

        SPARQLParser parser = new SPARQLParser();
        ParsedQuery pq = parser.parseQuery(query, null);
        List<StatementPattern> spList = StatementPatternCollector.process(pq.getTupleExpr());
        StatementMetadataNode<AccumuloRdfConfiguration> node = new StatementMetadataNode<>(spList, conf);

        QueryBindingSet bsConstraint = new QueryBindingSet();
        bsConstraint.addBinding("x", VF.createLiteral("CoffeeShop"));
        bsConstraint.addBinding("z", VF.createLiteral("Virginia"));

        CloseableIteration<BindingSet, QueryEvaluationException> iteration = node.evaluate(bsConstraint);

        QueryBindingSet expected = new QueryBindingSet();
        expected.addBinding("x", VF.createLiteral("CoffeeShop"));
        expected.addBinding("y", VF.createLiteral("Joe"));
        expected.addBinding("z", VF.createLiteral("Virginia"));

        List<BindingSet> bsList = new ArrayList<>();
        while (iteration.hasNext()) {
            bsList.add(iteration.next());
        }

        Assert.assertEquals(1, bsList.size());
        Assert.assertEquals(expected, bsList.get(0));

        dao.delete(statement1, conf);
        dao.delete(statement2, conf);
    }

    /**
     * Tests to see if correct result is passed back when a metadata statement
     * is joined with a StatementPattern statement (i.e. a common variable
     * appears in a StatementPattern statement and a metadata statement).
     * StatementPattern statements have either rdf:subject, rdf:predicate, or
     * rdf:object as the predicate while a metadata statement is any statement
     * in the reified query whose predicate is not rdf:type and not a
     * StatementPattern predicate.
     * 
     * @throws MalformedQueryException
     * @throws QueryEvaluationException
     * @throws RyaDAOException
     */
    @Test
    public void simpleQueryWithBindingSetJoinPropertyToSubject()
            throws MalformedQueryException, QueryEvaluationException, RyaDAOException {

        StatementMetadata metadata = new StatementMetadata();
        metadata.addMetadata(new RyaIRI("http://createdBy"), new RyaIRI("http://Joe"));
        metadata.addMetadata(new RyaIRI("http://createdOn"), new RyaType(XMLSchema.DATE, "2017-01-04"));

        RyaStatement statement1 = new RyaStatement(new RyaIRI("http://Joe"), new RyaIRI("http://worksAt"),
                new RyaType("CoffeeShop"), new RyaIRI("http://context"), "", metadata);
        RyaStatement statement2 = new RyaStatement(new RyaIRI("http://Bob"), new RyaIRI("http://worksAt"),
                new RyaType("HardwareStore"), new RyaIRI("http://context"), "", metadata);
        dao.add(statement1);
        dao.add(statement2);

        SPARQLParser parser = new SPARQLParser();
        ParsedQuery pq = parser.parseQuery(query2, null);
        List<StatementPattern> spList = StatementPatternCollector.process(pq.getTupleExpr());
        StatementMetadataNode<AccumuloRdfConfiguration> node = new StatementMetadataNode<>(spList, conf);

        List<BindingSet> bsCollection = new ArrayList<>();
        QueryBindingSet bsConstraint1 = new QueryBindingSet();
        bsConstraint1.addBinding("y", VF.createLiteral("CoffeeShop"));
        bsConstraint1.addBinding("z", VF.createLiteral("Virginia"));

        QueryBindingSet bsConstraint2 = new QueryBindingSet();
        bsConstraint2.addBinding("y", VF.createLiteral("HardwareStore"));
        bsConstraint2.addBinding("z", VF.createLiteral("Maryland"));
        bsCollection.add(bsConstraint1);
        bsCollection.add(bsConstraint2);

        CloseableIteration<BindingSet, QueryEvaluationException> iteration = node.evaluate(bsCollection);

        QueryBindingSet expected = new QueryBindingSet();
        expected.addBinding("y", VF.createLiteral("CoffeeShop"));
        expected.addBinding("x", VF.createIRI("http://Joe"));
        expected.addBinding("z", VF.createLiteral("Virginia"));

        List<BindingSet> bsList = new ArrayList<>();
        while (iteration.hasNext()) {
            bsList.add(iteration.next());
        }

        Assert.assertEquals(1, bsList.size());
        Assert.assertEquals(expected, bsList.get(0));

        dao.delete(statement1, conf);
        dao.delete(statement2, conf);
    }

    /**
     * Tests if the StatementMetadataNode joins BindingSet correctly for
     * variables appearing in metadata statements. In this case, the metadata
     * statements are (_:blankNode <http://createdOn 2017-01-04 ) and
     * (_:blankNode <http://createdBy> ?y). The variable ?y appears as the
     * object in the above metadata statement and its values are joined to the
     * constraint BindingSets in the example below.
     * 
     * @throws MalformedQueryException
     * @throws QueryEvaluationException
     * @throws RyaDAOException
     */
    @Test
    public void simpleQueryWithBindingSetJoinOnProperty()
            throws MalformedQueryException, QueryEvaluationException, RyaDAOException {

        StatementMetadata metadata = new StatementMetadata();
        metadata.addMetadata(new RyaIRI("http://createdBy"), new RyaType("Joe"));
        metadata.addMetadata(new RyaIRI("http://createdOn"), new RyaType(XMLSchema.DATE, "2017-01-04"));

        RyaStatement statement1 = new RyaStatement(new RyaIRI("http://Joe"), new RyaIRI("http://worksAt"),
                new RyaType("CoffeeShop"), new RyaIRI("http://context"), "", metadata);
        dao.add(statement1);

        SPARQLParser parser = new SPARQLParser();
        ParsedQuery pq = parser.parseQuery(query, null);
        List<StatementPattern> spList = StatementPatternCollector.process(pq.getTupleExpr());
        StatementMetadataNode<AccumuloRdfConfiguration> node = new StatementMetadataNode<>(spList, conf);

        QueryBindingSet bsConstraint = new QueryBindingSet();
        bsConstraint.addBinding("x", VF.createLiteral("CoffeeShop"));
        bsConstraint.addBinding("y", VF.createLiteral("Doug"));

        CloseableIteration<BindingSet, QueryEvaluationException> iteration = node.evaluate(bsConstraint);

        List<BindingSet> bsList = new ArrayList<>();
        while (iteration.hasNext()) {
            bsList.add(iteration.next());
        }

        Assert.assertEquals(0, bsList.size());
        dao.delete(statement1, conf);
    }

    /**
     * Tests if StatementMetadataNode joins BindingSet values correctly for
     * variables appearing as the object in one of the StatementPattern
     * statements (in the case ?x appears as the Object in the statement
     * _:blankNode rdf:object ?x). StatementPattern statements have either
     * rdf:subject, rdf:predicate, or rdf:object as the predicate.
     * 
     * @throws MalformedQueryException
     * @throws QueryEvaluationException
     * @throws RyaDAOException
     */
    @Test
    public void simpleQueryWithBindingSetCollection()
            throws MalformedQueryException, QueryEvaluationException, RyaDAOException {

        StatementMetadata metadata = new StatementMetadata();
        metadata.addMetadata(new RyaIRI("http://createdBy"), new RyaType("Joe"));
        metadata.addMetadata(new RyaIRI("http://createdOn"), new RyaType(XMLSchema.DATE, "2017-01-04"));

        RyaStatement statement1 = new RyaStatement(new RyaIRI("http://Joe"), new RyaIRI("http://worksAt"),
                new RyaType("CoffeeShop"), new RyaIRI("http://context"), "", metadata);
        RyaStatement statement2 = new RyaStatement(new RyaIRI("http://Joe"), new RyaIRI("http://worksAt"),
                new RyaType("HardwareStore"), new RyaIRI("http://context"), "", metadata);
        dao.add(statement1);
        dao.add(statement2);

        SPARQLParser parser = new SPARQLParser();
        ParsedQuery pq = parser.parseQuery(query, null);
        List<StatementPattern> spList = StatementPatternCollector.process(pq.getTupleExpr());
        StatementMetadataNode<AccumuloRdfConfiguration> node = new StatementMetadataNode<>(spList, conf);

        List<BindingSet> bsCollection = new ArrayList<>();
        QueryBindingSet bsConstraint1 = new QueryBindingSet();
        bsConstraint1.addBinding("x", VF.createLiteral("CoffeeShop"));
        bsConstraint1.addBinding("z", VF.createLiteral("Virginia"));

        QueryBindingSet bsConstraint2 = new QueryBindingSet();
        bsConstraint2.addBinding("x", VF.createLiteral("HardwareStore"));
        bsConstraint2.addBinding("z", VF.createLiteral("Maryland"));

        QueryBindingSet bsConstraint3 = new QueryBindingSet();
        bsConstraint3.addBinding("x", VF.createLiteral("BurgerShack"));
        bsConstraint3.addBinding("z", VF.createLiteral("Delaware"));
        bsCollection.add(bsConstraint1);
        bsCollection.add(bsConstraint2);
        bsCollection.add(bsConstraint3);

        CloseableIteration<BindingSet, QueryEvaluationException> iteration = node.evaluate(bsCollection);

        QueryBindingSet expected1 = new QueryBindingSet();
        expected1.addBinding("x", VF.createLiteral("CoffeeShop"));
        expected1.addBinding("y", VF.createLiteral("Joe"));
        expected1.addBinding("z", VF.createLiteral("Virginia"));

        QueryBindingSet expected2 = new QueryBindingSet();
        expected2.addBinding("x", VF.createLiteral("HardwareStore"));
        expected2.addBinding("y", VF.createLiteral("Joe"));
        expected2.addBinding("z", VF.createLiteral("Maryland"));

        List<BindingSet> bsList = new ArrayList<>();
        while (iteration.hasNext()) {
            bsList.add(iteration.next());
        }

        Assert.assertEquals(2, bsList.size());
        Assert.assertEquals(expected1, bsList.get(1));
        Assert.assertEquals(expected2, bsList.get(0));

        dao.delete(statement1, conf);
        dao.delete(statement2, conf);
    }

    /**
     * Tests if StatementMetadataNode joins BindingSet values correctly for
     * variables appearing as the object in one of the StatementPattern
     * statements (in the case ?x appears as the Object in the statement
     * _:blankNode rdf:object ?x). StatementPattern statements have either
     * rdf:subject, rdf:predicate, or rdf:object as the predicate. Additionally,
     * this test also determines whether node uses specified context as a query
     * constraint.
     * 
     * @throws MalformedQueryException
     * @throws QueryEvaluationException
     * @throws RyaDAOException
     */
    @Test
    public void simpleQueryWithConstantContext()
            throws MalformedQueryException, QueryEvaluationException, RyaDAOException {

        // query used to create StatementPatternMetadataNode
        String contextQuery = "prefix owl: <http://www.w3.org/2002/07/owl#> prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> select ?x ?y where { graph <http://context_1> {_:blankNode rdf:type owl:Annotation; owl:annotatedSource <http://Joe>; "
                + "owl:annotatedProperty <http://worksAt>; owl:annotatedTarget ?x; <http://createdBy> ?y; <http://createdOn> \'2017-01-04\'^^xsd:date }}";

        StatementMetadata metadata = new StatementMetadata();
        metadata.addMetadata(new RyaIRI("http://createdBy"), new RyaType("Joe"));
        metadata.addMetadata(new RyaIRI("http://createdOn"), new RyaType(XMLSchema.DATE, "2017-01-04"));

        RyaStatement statement1 = new RyaStatement(new RyaIRI("http://Joe"), new RyaIRI("http://worksAt"),
                new RyaType("CoffeeShop"), new RyaIRI("http://context_1"), "", metadata);
        RyaStatement statement2 = new RyaStatement(new RyaIRI("http://Joe"), new RyaIRI("http://worksAt"),
                new RyaType("HardwareStore"), new RyaIRI("http://context_2"), "", metadata);
        dao.add(statement1);
        dao.add(statement2);

        SPARQLParser parser = new SPARQLParser();
        ParsedQuery pq = parser.parseQuery(contextQuery, null);
        List<StatementPattern> spList = StatementPatternCollector.process(pq.getTupleExpr());
        StatementMetadataNode<AccumuloRdfConfiguration> node = new StatementMetadataNode<>(spList, conf);

        List<BindingSet> bsCollection = new ArrayList<>();
        QueryBindingSet bsConstraint1 = new QueryBindingSet();
        bsConstraint1.addBinding("x", VF.createLiteral("CoffeeShop"));
        bsConstraint1.addBinding("z", VF.createLiteral("Virginia"));

        QueryBindingSet bsConstraint2 = new QueryBindingSet();
        bsConstraint2.addBinding("x", VF.createLiteral("HardwareStore"));
        bsConstraint2.addBinding("z", VF.createLiteral("Maryland"));

        QueryBindingSet bsConstraint3 = new QueryBindingSet();
        bsConstraint3.addBinding("x", VF.createLiteral("BurgerShack"));
        bsConstraint3.addBinding("z", VF.createLiteral("Delaware"));
        bsCollection.add(bsConstraint1);
        bsCollection.add(bsConstraint2);
        bsCollection.add(bsConstraint3);

        CloseableIteration<BindingSet, QueryEvaluationException> iteration = node.evaluate(bsCollection);

        QueryBindingSet expected1 = new QueryBindingSet();
        expected1.addBinding("x", VF.createLiteral("CoffeeShop"));
        expected1.addBinding("y", VF.createLiteral("Joe"));
        expected1.addBinding("z", VF.createLiteral("Virginia"));

        List<BindingSet> bsList = new ArrayList<>();
        while (iteration.hasNext()) {
            bsList.add(iteration.next());
        }

        Assert.assertEquals(1, bsList.size());
        Assert.assertEquals(expected1, bsList.get(0));

        dao.delete(statement1, conf);
        dao.delete(statement2, conf);
    }

    /**
     * Tests if StatementMetadataNode joins BindingSet values correctly for
     * variables appearing as the object in one of the StatementPattern
     * statements (in the case ?x appears as the Object in the statement
     * _:blankNode rdf:object ?x). StatementPattern statements have either
     * rdf:subject, rdf:predicate, or rdf:object as the predicate. Additionally,
     * this test also determines whether node passes back bindings corresponding
     * to a specified context.
     * 
     * @throws MalformedQueryException
     * @throws QueryEvaluationException
     * @throws RyaDAOException
     */
    @Test
    public void simpleQueryWithVariableContext()
            throws MalformedQueryException, QueryEvaluationException, RyaDAOException {

        // query used to create StatementPatternMetadataNode
        String contextQuery = "prefix owl: <http://www.w3.org/2002/07/owl#> prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> select ?x ?y ?c where { graph ?c {_:blankNode rdf:type owl:Annotation; owl:annotatedSource <http://Joe>; "
                + "owl:annotatedProperty <http://worksAt>; owl:annotatedTarget ?x; <http://createdBy> ?y; <http://createdOn> \'2017-01-04\'^^xsd:date }}";

        StatementMetadata metadata = new StatementMetadata();
        metadata.addMetadata(new RyaIRI("http://createdBy"), new RyaType("Joe"));
        metadata.addMetadata(new RyaIRI("http://createdOn"), new RyaType(XMLSchema.DATE, "2017-01-04"));

        RyaStatement statement1 = new RyaStatement(new RyaIRI("http://Joe"), new RyaIRI("http://worksAt"),
                new RyaType("CoffeeShop"), new RyaIRI("http://context_1"), "", metadata);
        RyaStatement statement2 = new RyaStatement(new RyaIRI("http://Joe"), new RyaIRI("http://worksAt"),
                new RyaType("HardwareStore"), new RyaIRI("http://context_2"), "", metadata);
        dao.add(statement1);
        dao.add(statement2);

        SPARQLParser parser = new SPARQLParser();
        ParsedQuery pq = parser.parseQuery(contextQuery, null);
        List<StatementPattern> spList = StatementPatternCollector.process(pq.getTupleExpr());
        StatementMetadataNode<AccumuloRdfConfiguration> node = new StatementMetadataNode<>(spList, conf);

        List<BindingSet> bsCollection = new ArrayList<>();
        QueryBindingSet bsConstraint1 = new QueryBindingSet();
        bsConstraint1.addBinding("x", VF.createLiteral("CoffeeShop"));
        bsConstraint1.addBinding("z", VF.createLiteral("Virginia"));

        QueryBindingSet bsConstraint2 = new QueryBindingSet();
        bsConstraint2.addBinding("x", VF.createLiteral("HardwareStore"));
        bsConstraint2.addBinding("z", VF.createLiteral("Maryland"));

        QueryBindingSet bsConstraint3 = new QueryBindingSet();
        bsConstraint3.addBinding("x", VF.createLiteral("BurgerShack"));
        bsConstraint3.addBinding("z", VF.createLiteral("Delaware"));
        bsCollection.add(bsConstraint1);
        bsCollection.add(bsConstraint2);
        bsCollection.add(bsConstraint3);

        CloseableIteration<BindingSet, QueryEvaluationException> iteration = node.evaluate(bsCollection);

        QueryBindingSet expected1 = new QueryBindingSet();
        expected1.addBinding("x", VF.createLiteral("CoffeeShop"));
        expected1.addBinding("y", VF.createLiteral("Joe"));
        expected1.addBinding("z", VF.createLiteral("Virginia"));
        expected1.addBinding("c", VF.createIRI("http://context_1"));

        QueryBindingSet expected2 = new QueryBindingSet();
        expected2.addBinding("x", VF.createLiteral("HardwareStore"));
        expected2.addBinding("y", VF.createLiteral("Joe"));
        expected2.addBinding("z", VF.createLiteral("Maryland"));
        expected2.addBinding("c", VF.createIRI("http://context_2"));

        List<BindingSet> bsList = new ArrayList<>();
        while (iteration.hasNext()) {
            bsList.add(iteration.next());
        }

        Assert.assertEquals(2, bsList.size());
        Assert.assertEquals(expected1, bsList.get(1));
        Assert.assertEquals(expected2, bsList.get(0));

        dao.delete(statement1, conf);
        dao.delete(statement2, conf);
    }

    /**
     * Tests if StatementMetadataNode joins BindingSet values correctly for
     * variables appearing as the object in one of the StatementPattern
     * statements (in the case ?x appears as the Object in the statement
     * _:blankNode rdf:object ?x). StatementPattern statements have either
     * rdf:subject, rdf:predicate, or rdf:object as the predicate. Additionally,
     * this test also determines whether node passes back bindings corresponding
     * to a specified context and that a join across variable context is
     * performed properly.
     * 
     * @throws MalformedQueryException
     * @throws QueryEvaluationException
     * @throws RyaDAOException
     */
    @Test
    public void simpleQueryWithVariableContextAndJoinOnContext()
            throws MalformedQueryException, QueryEvaluationException, RyaDAOException {

        // query used to create StatementPatternMetadataNode
        String contextQuery = "prefix owl: <http://www.w3.org/2002/07/owl#> prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> select ?x ?y ?c where { graph ?c {_:blankNode rdf:type owl:Annotation; owl:annotatedSource <http://Joe>; "
                + "owl:annotatedProperty <http://worksAt>; owl:annotatedTarget ?x; <http://createdBy> ?y; <http://createdOn> \'2017-01-04\'^^xsd:date }}";

        StatementMetadata metadata = new StatementMetadata();
        metadata.addMetadata(new RyaIRI("http://createdBy"), new RyaType("Joe"));
        metadata.addMetadata(new RyaIRI("http://createdOn"), new RyaType(XMLSchema.DATE, "2017-01-04"));

        RyaStatement statement1 = new RyaStatement(new RyaIRI("http://Joe"), new RyaIRI("http://worksAt"),
                new RyaType("CoffeeShop"), new RyaIRI("http://context_1"), "", metadata);
        RyaStatement statement2 = new RyaStatement(new RyaIRI("http://Joe"), new RyaIRI("http://worksAt"),
                new RyaType("HardwareStore"), new RyaIRI("http://context_2"), "", metadata);
        dao.add(statement1);
        dao.add(statement2);


        SPARQLParser parser = new SPARQLParser();
        ParsedQuery pq = parser.parseQuery(contextQuery, null);
        List<StatementPattern> spList = StatementPatternCollector.process(pq.getTupleExpr());
        StatementMetadataNode<AccumuloRdfConfiguration> node = new StatementMetadataNode<>(spList, conf);

        List<BindingSet> bsCollection = new ArrayList<>();
        QueryBindingSet bsConstraint1 = new QueryBindingSet();
        bsConstraint1.addBinding("x", VF.createLiteral("CoffeeShop"));
        bsConstraint1.addBinding("z", VF.createLiteral("Virginia"));
        bsConstraint1.addBinding("c", VF.createIRI("http://context_1"));

        QueryBindingSet bsConstraint2 = new QueryBindingSet();
        bsConstraint2.addBinding("x", VF.createLiteral("CoffeeShop"));
        bsConstraint2.addBinding("z", VF.createLiteral("Maryland"));
        bsConstraint2.addBinding("c", VF.createIRI("http://context_2"));

        QueryBindingSet bsConstraint4 = new QueryBindingSet();
        bsConstraint4.addBinding("x", VF.createLiteral("HardwareStore"));
        bsConstraint4.addBinding("z", VF.createLiteral("WestVirginia"));
        bsConstraint4.addBinding("c", VF.createIRI("http://context_2"));

        QueryBindingSet bsConstraint3 = new QueryBindingSet();
        bsConstraint3.addBinding("x", VF.createLiteral("BurgerShack"));
        bsConstraint3.addBinding("z", VF.createLiteral("Delaware"));
        bsConstraint3.addBinding("c", VF.createIRI("http://context_1"));
        bsCollection.add(bsConstraint1);
        bsCollection.add(bsConstraint2);
        bsCollection.add(bsConstraint3);
        bsCollection.add(bsConstraint4);
        
//        AccumuloRyaQueryEngine engine = dao.getQueryEngine();
////        CloseableIteration<RyaStatement, RyaDAOException> iter = engine.query(new RyaStatement(new RyaIRI("http://Joe"),
////                new RyaIRI("http://worksAt"), new RyaType("HardwareStore"), new RyaIRI("http://context_2")), conf);
//        CloseableIteration<? extends Map.Entry<RyaStatement,BindingSet>, RyaDAOException> iter = engine.queryWithBindingSet(Arrays.asList(new RdfCloudTripleStoreUtils.CustomEntry<RyaStatement, BindingSet>(
//                new RyaStatement(new RyaIRI("http://Joe"),
//                        new RyaIRI("http://worksAt"), new RyaType("HardwareStore"), new RyaIRI("http://context_2")), bsConstraint4)), conf);
//        while (iter.hasNext()) {
//            System.out.println(iter.next());
//        }
//
        CloseableIteration<BindingSet, QueryEvaluationException> iteration = node.evaluate(bsCollection);

        QueryBindingSet expected1 = new QueryBindingSet();
        expected1.addBinding("x", VF.createLiteral("CoffeeShop"));
        expected1.addBinding("y", VF.createLiteral("Joe"));
        expected1.addBinding("z", VF.createLiteral("Virginia"));
        expected1.addBinding("c", VF.createIRI("http://context_1"));

        QueryBindingSet expected2 = new QueryBindingSet();
        expected2.addBinding("x", VF.createLiteral("HardwareStore"));
        expected2.addBinding("y", VF.createLiteral("Joe"));
        expected2.addBinding("z", VF.createLiteral("WestVirginia"));
        expected2.addBinding("c", VF.createIRI("http://context_2"));

        List<BindingSet> bsList = new ArrayList<>();
        while (iteration.hasNext()) {
            bsList.add(iteration.next());
        }

        Assert.assertEquals(2, bsList.size());
        Assert.assertEquals(expected1, bsList.get(1));
        Assert.assertEquals(expected2, bsList.get(0));

        dao.delete(statement1, conf);
        dao.delete(statement2, conf);
    }

    private static AccumuloRdfConfiguration getConf() {

        final AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();

        conf.setBoolean(ConfigUtils.USE_MOCK_INSTANCE, true);
        conf.set(RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX, "rya_");
        conf.set(ConfigUtils.CLOUDBASE_USER, "root");
        conf.set(ConfigUtils.CLOUDBASE_PASSWORD, "");
        conf.set(ConfigUtils.CLOUDBASE_INSTANCE, "instance");
        conf.set(ConfigUtils.CLOUDBASE_AUTHS, "");

        return conf;
    }

}
