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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.client.Connector;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.domain.StatementMetadata;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.sail.config.RyaSailFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.Sail;

public class AccumuloStatementMetadataOptimizerIT {

    private RdfCloudTripleStoreConfiguration conf;
    private Sail sail;
    private SailRepository repo;
    private SailRepositoryConnection conn;
    private AccumuloRyaDAO dao;
    private final String query1 = "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> select ?x ?y where {_:blankNode rdf:type rdf:Statement; rdf:subject <http://Joe>; "
            + "rdf:predicate <http://worksAt>; rdf:object ?x; <http://createdBy> ?y; <http://createdOn> \'2017-01-04\'^^xsd:date }";
    private final String query2 = "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> select ?a ?b ?c where {_:blankNode1 rdf:type rdf:Statement; rdf:subject ?a; "
            + "rdf:predicate <http://worksAt>; rdf:object <http://BurgerShack>; <http://createdBy> ?c; <http://createdOn> \'2017-01-04\'^^xsd:date. "
            + "_:blankNode2 rdf:type rdf:Statement; rdf:subject ?a; "
            + "rdf:predicate <http://talksTo>; rdf:object ?b; <http://createdBy> ?c; <http://createdOn> \'2017-01-04\'^^xsd:date }";

    @Before
    public void init() throws Exception {
        conf = getConf();
        sail = RyaSailFactory.getInstance(conf);
        repo = new SailRepository(sail);
        conn = repo.getConnection();

        Connector conn = ConfigUtils.getConnector(conf);
        dao = new AccumuloRyaDAO();
        dao.setConnector(conn);
        dao.init();
    }

    @After
    public void close() throws Exception {
        conn.close();
        repo.shutDown();
        sail.shutDown();
        sail.shutDown();
        dao.destroy();
    }

    @Test
    public void simpleQueryWithoutBindingSet() throws Exception {
        StatementMetadata metadata = new StatementMetadata();
        metadata.addMetadata(new RyaURI("http://createdBy"), new RyaType("Joe"));
        metadata.addMetadata(new RyaURI("http://createdOn"), new RyaType(XMLSchema.DATE, "2017-01-04"));

        RyaStatement statement = new RyaStatement(new RyaURI("http://Joe"), new RyaURI("http://worksAt"),
                new RyaType("CoffeeShop"), new RyaURI("http://context"), "", metadata);
        dao.add(statement);

        TupleQueryResult result = conn.prepareTupleQuery(QueryLanguage.SPARQL, query1).evaluate();

        QueryBindingSet bs = new QueryBindingSet();
        bs.addBinding("x", new LiteralImpl("CoffeeShop"));
        bs.addBinding("y", new LiteralImpl("Joe"));

        List<BindingSet> bsList = new ArrayList<>();
        while (result.hasNext()) {
            bsList.add(result.next());
        }

        System.out.println(bsList);
        Assert.assertEquals(1, bsList.size());
        Assert.assertEquals(bs, bsList.get(0));
        dao.delete(statement, (AccumuloRdfConfiguration) conf);
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
    public void simpleQueryWithoutBindingSetInvalidProperty() throws Exception {
        StatementMetadata metadata = new StatementMetadata();
        metadata.addMetadata(new RyaURI("http://createdBy"), new RyaType("Doug"));
        metadata.addMetadata(new RyaURI("http://createdOn"), new RyaType(XMLSchema.DATE, "2017-02-15"));

        RyaStatement statement = new RyaStatement(new RyaURI("http://Joe"), new RyaURI("http://worksAt"),
                new RyaType("CoffeeShop"), new RyaURI("http://context"), "", metadata);
        dao.add(statement);

        TupleQueryResult result = conn.prepareTupleQuery(QueryLanguage.SPARQL, query1).evaluate();

        List<BindingSet> bsList = new ArrayList<>();
        while (result.hasNext()) {
            bsList.add(result.next());
        }
        Assert.assertEquals(0, bsList.size());
        dao.delete(statement, (AccumuloRdfConfiguration) conf);
    }

//    @Test
//    public void simpleDataTypeTest() throws Exception {
//        StatementMetadata metadata = new StatementMetadata();
//        metadata.addMetadata(new RyaURI("http://createdBy"), new RyaType("Doug"));
//        metadata.addMetadata(new RyaURI("http://createdOn"), new RyaType(XMLSchema.DATE, "2017-02-15"));
//
//        RyaStatement statement = new RyaStatement(new RyaURI("http://Joe"), new RyaURI("http://worksAt"),
//                new RyaType("http://BurgerShack"), new RyaURI("http://context"), "", metadata);
//        dao.add(statement);
//        RyaStatement statement2 = new RyaStatement(new RyaURI("http://Bob"), new RyaURI("http://worksAt"),
//                new RyaURI("http://BurgerShack"), new RyaURI("http://context"), "", metadata);
//        dao.add(statement2);
//        
//        String temp = "select ?x where { ?x <http://worksAt> 'http://BurgerShack' . }";
//        TupleQueryResult result = conn.prepareTupleQuery(QueryLanguage.SPARQL, temp).evaluate();
//        
//        List<BindingSet> bsList = new ArrayList<>();
//        while (result.hasNext()) {
//            bsList.add(result.next());
//        }
//        System.out.println("Bindings are: " + bsList);
//        dao.delete(statement, (AccumuloRdfConfiguration) conf);
//        dao.delete(statement2, (AccumuloRdfConfiguration) conf);
//    }

    
    @Test
    public void simpleQueryWithBindingSet() throws Exception {

        StatementMetadata metadata = new StatementMetadata();
        metadata.addMetadata(new RyaURI("http://createdBy"), new RyaType("Joe"));
        metadata.addMetadata(new RyaURI("http://createdOn"), new RyaType(XMLSchema.DATE, "2017-01-04"));

        RyaStatement statement1 = new RyaStatement(new RyaURI("http://Joe"), new RyaURI("http://worksAt"),
                new RyaType("CoffeeShop"), new RyaURI("http://context"), "", metadata);
        RyaStatement statement2 = new RyaStatement(new RyaURI("http://Joe"), new RyaURI("http://worksAt"),
                new RyaType("HardwareStore"), new RyaURI("http://context"), "", metadata);
        dao.add(statement1);
        dao.add(statement2);

        TupleQueryResult result = conn.prepareTupleQuery(QueryLanguage.SPARQL, query1).evaluate();

        Set<BindingSet> expected = new HashSet<>();
        QueryBindingSet expected1 = new QueryBindingSet();
        expected1.addBinding("x", new LiteralImpl("CoffeeShop"));
        expected1.addBinding("y", new LiteralImpl("Joe"));
        QueryBindingSet expected2 = new QueryBindingSet();
        expected2.addBinding("x", new LiteralImpl("HardwareStore"));
        expected2.addBinding("y", new LiteralImpl("Joe"));
        expected.add(expected1);
        expected.add(expected2);

        Set<BindingSet> bsSet = new HashSet<>();
        while (result.hasNext()) {
            bsSet.add(result.next());
        }

        Assert.assertEquals(expected, bsSet);

        dao.delete(statement1, (AccumuloRdfConfiguration) conf);
        dao.delete(statement2, (AccumuloRdfConfiguration) conf);
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
    public void simpleQueryWithBindingSetJoinPropertyToSubject() throws Exception {

        StatementMetadata metadata1 = new StatementMetadata();
        metadata1.addMetadata(new RyaURI("http://createdBy"), new RyaURI("http://Doug"));
        metadata1.addMetadata(new RyaURI("http://createdOn"), new RyaType(XMLSchema.DATE, "2017-01-04"));
        StatementMetadata metadata2 = new StatementMetadata();
        metadata2.addMetadata(new RyaURI("http://createdBy"), new RyaURI("http://Bob"));
        metadata2.addMetadata(new RyaURI("http://createdOn"), new RyaType(XMLSchema.DATE, "2017-02-04"));

        RyaStatement statement1 = new RyaStatement(new RyaURI("http://Joe"), new RyaURI("http://worksAt"),
                new RyaURI("http://BurgerShack"), new RyaURI("http://context"), "", metadata1);
        RyaStatement statement2 = new RyaStatement(new RyaURI("http://Joe"), new RyaURI("http://talksTo"),
                new RyaURI("http://Betty"), new RyaURI("http://context"), "", metadata1);
        RyaStatement statement3 = new RyaStatement(new RyaURI("http://Fred"), new RyaURI("http://talksTo"),
                new RyaType("http://Amanda"), new RyaURI("http://context"), "", metadata1);
        RyaStatement statement4 = new RyaStatement(new RyaURI("http://Joe"), new RyaURI("http://talksTo"),
                new RyaType("http://Wanda"), new RyaURI("http://context"), "", metadata2);
        dao.add(statement1);
        dao.add(statement2);
        dao.add(statement3);
        dao.add(statement4);

        TupleQueryResult result = conn.prepareTupleQuery(QueryLanguage.SPARQL, query2).evaluate();

        Set<BindingSet> expected = new HashSet<>();
        QueryBindingSet expected1 = new QueryBindingSet();
        expected1.addBinding("b", new URIImpl("http://Betty"));
        expected1.addBinding("a", new URIImpl("http://Joe"));
        expected1.addBinding("c", new URIImpl("http://Doug"));
        expected.add(expected1);

        Set<BindingSet> bsSet = new HashSet<>();
        while (result.hasNext()) {
            bsSet.add(result.next());
        }

        Assert.assertEquals(expected, bsSet);

        dao.delete(statement1, (AccumuloRdfConfiguration) conf);
        dao.delete(statement2, (AccumuloRdfConfiguration) conf);
        dao.delete(statement3, (AccumuloRdfConfiguration) conf);
        dao.delete(statement4, (AccumuloRdfConfiguration) conf);
    }

    private static RdfCloudTripleStoreConfiguration getConf() {

        RdfCloudTripleStoreConfiguration conf;
        Set<RyaURI> propertySet = new HashSet<RyaURI>(
                Arrays.asList(new RyaURI("http://createdBy"), new RyaURI("http://createdOn")));
        conf = new AccumuloRdfConfiguration();
        conf.setBoolean(ConfigUtils.USE_MOCK_INSTANCE, true);
        conf.set(RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX, "rya_");
        conf.set(ConfigUtils.CLOUDBASE_USER, "root");
        conf.set(ConfigUtils.CLOUDBASE_PASSWORD, "");
        conf.set(ConfigUtils.CLOUDBASE_INSTANCE, "instance");
        conf.set(ConfigUtils.CLOUDBASE_AUTHS, "");
        conf.setUseStatementMetadata(true);
        conf.setStatementMetadataProperties(propertySet);
        return conf;
    }

}
