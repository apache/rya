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
package org.apache.rya.indexing.statement.metadata;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.api.domain.StatementMetadata;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;
import org.apache.rya.mongodb.MongoDBRyaDAO;
import org.apache.rya.mongodb.MongoITBase;
import org.apache.rya.sail.config.RyaSailFactory;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.sail.Sail;
import org.junit.Test;

public class MongoStatementMetadataIT extends MongoITBase {
    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    private final String query1 = "prefix owl: <http://www.w3.org/2002/07/owl#> prefix ano: <http://www.w3.org/2002/07/owl#annotated> prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> select ?x ?y where {_:blankNode rdf:type owl:Annotation; ano:Source <http://Joe>; "
            + "ano:Property <http://worksAt>; ano:Target ?x; <http://createdBy> ?y; <http://createdOn> \'2017-01-04\'^^xsd:date }";
    private final String query2 = "prefix owl: <http://www.w3.org/2002/07/owl#> prefix ano: <http://www.w3.org/2002/07/owl#annotated> prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> select ?a ?b ?c where {_:blankNode1 rdf:type owl:Annotation; ano:Source ?a; "
            + "ano:Property <http://worksAt>; ano:Target <http://BurgerShack>; <http://createdBy> ?c; <http://createdOn> \'2017-01-04\'^^xsd:date. "
            + "_:blankNode2 rdf:type owl:Annotation; ano:Source ?a; "
            + "ano:Property <http://talksTo>; ano:Target ?b; <http://createdBy> ?c; <http://createdOn> \'2017-01-04\'^^xsd:date }";

    @Override
    protected void updateConfiguration(final MongoDBRdfConfiguration conf) {
        final Set<RyaIRI> propertySet = new HashSet<>(
                Arrays.asList(new RyaIRI("http://createdBy"), new RyaIRI("http://createdOn")));
        conf.setUseStatementMetadata(true);
        conf.setStatementMetadataProperties(propertySet);
    }

    @Test
    public void simpleQueryWithoutBindingSet() throws Exception {
        Sail sail = RyaSailFactory.getInstance(conf);
        MongoDBRyaDAO dao = new MongoDBRyaDAO();
        try {
            dao.setConf(conf);
            dao.init();

            final StatementMetadata metadata = new StatementMetadata();
            metadata.addMetadata(new RyaIRI("http://createdBy"), new RyaType("Joe"));
            metadata.addMetadata(new RyaIRI("http://createdOn"), new RyaType(XMLSchema.DATE, "2017-01-04"));

            final RyaStatement statement = new RyaStatement(new RyaIRI("http://Joe"), new RyaIRI("http://worksAt"),
                    new RyaType("CoffeeShop"), new RyaIRI("http://context"), "", metadata);
            dao.add(statement);

            SailRepositoryConnection conn = new SailRepository(sail).getConnection();
            final TupleQueryResult result = conn.prepareTupleQuery(QueryLanguage.SPARQL, query1).evaluate();

            final QueryBindingSet bs = new QueryBindingSet();
            bs.addBinding("x", VF.createLiteral("CoffeeShop"));
            bs.addBinding("y", VF.createLiteral("Joe"));

            final List<BindingSet> bsList = new ArrayList<>();
            while (result.hasNext()) {
                bsList.add(result.next());
            }

            assertEquals(1, bsList.size());
            assertEquals(bs, bsList.get(0));
            dao.delete(statement, conf);
        } finally {
            dao.destroy();
            sail.shutDown();
        }
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
        Sail sail = RyaSailFactory.getInstance(conf);
        MongoDBRyaDAO dao = new MongoDBRyaDAO();
        try {
            dao.setConf(conf);
            dao.init();
            final StatementMetadata metadata = new StatementMetadata();
            metadata.addMetadata(new RyaIRI("http://createdBy"), new RyaType("Doug"));
            metadata.addMetadata(new RyaIRI("http://createdOn"), new RyaType(XMLSchema.DATE, "2017-02-15"));

            final RyaStatement statement = new RyaStatement(new RyaIRI("http://Joe"), new RyaIRI("http://worksAt"),
                    new RyaType("CoffeeShop"), new RyaIRI("http://context"), "", metadata);
            dao.add(statement);

            SailRepositoryConnection conn = new SailRepository(sail).getConnection();
            final TupleQueryResult result = conn.prepareTupleQuery(QueryLanguage.SPARQL, query1).evaluate();

            final List<BindingSet> bsList = new ArrayList<>();
            while (result.hasNext()) {
                bsList.add(result.next());
            }
            assertEquals(0, bsList.size());
            dao.delete(statement, conf);
        } finally {
            dao.destroy();
            sail.shutDown();
        }
    }

    @Test
    public void simpleQueryWithBindingSet() throws Exception {
        Sail sail = RyaSailFactory.getInstance(conf);
        MongoDBRyaDAO dao = new MongoDBRyaDAO();
        try {
            dao.setConf(conf);
            dao.init();
            final StatementMetadata metadata = new StatementMetadata();
            metadata.addMetadata(new RyaIRI("http://createdBy"), new RyaType("Joe"));
            metadata.addMetadata(new RyaIRI("http://createdOn"), new RyaType(XMLSchema.DATE, "2017-01-04"));

            final RyaStatement statement1 = new RyaStatement(new RyaIRI("http://Joe"), new RyaIRI("http://worksAt"),
                    new RyaType("CoffeeShop"), new RyaIRI("http://context"), "", metadata);
            final RyaStatement statement2 = new RyaStatement(new RyaIRI("http://Joe"), new RyaIRI("http://worksAt"),
                    new RyaType("HardwareStore"), new RyaIRI("http://context"), "", metadata);
            dao.add(statement1);
            dao.add(statement2);

            SailRepositoryConnection conn = new SailRepository(sail).getConnection();
            final TupleQueryResult result = conn.prepareTupleQuery(QueryLanguage.SPARQL, query1).evaluate();

            final Set<BindingSet> expected = new HashSet<>();
            final QueryBindingSet expected1 = new QueryBindingSet();
            expected1.addBinding("x", VF.createLiteral("CoffeeShop"));
            expected1.addBinding("y", VF.createLiteral("Joe"));
            final QueryBindingSet expected2 = new QueryBindingSet();
            expected2.addBinding("x", VF.createLiteral("HardwareStore"));
            expected2.addBinding("y", VF.createLiteral("Joe"));
            expected.add(expected1);
            expected.add(expected2);

            final Set<BindingSet> bsSet = new HashSet<>();
            while (result.hasNext()) {
                bsSet.add(result.next());
            }

            assertEquals(expected, bsSet);

            dao.delete(statement1, conf);
            dao.delete(statement2, conf);
        } finally {
            dao.destroy();
            sail.shutDown();
        }
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
        Sail sail = RyaSailFactory.getInstance(conf);
        MongoDBRyaDAO dao = new MongoDBRyaDAO();
        try {
            dao.setConf(conf);
            dao.init();
            final StatementMetadata metadata1 = new StatementMetadata();
            metadata1.addMetadata(new RyaIRI("http://createdBy"), new RyaIRI("http://Doug"));
            metadata1.addMetadata(new RyaIRI("http://createdOn"), new RyaType(XMLSchema.DATE, "2017-01-04"));
            final StatementMetadata metadata2 = new StatementMetadata();
            metadata2.addMetadata(new RyaIRI("http://createdBy"), new RyaIRI("http://Bob"));
            metadata2.addMetadata(new RyaIRI("http://createdOn"), new RyaType(XMLSchema.DATE, "2017-02-04"));

            final RyaStatement statement1 = new RyaStatement(new RyaIRI("http://Joe"), new RyaIRI("http://worksAt"),
                    new RyaIRI("http://BurgerShack"), new RyaIRI("http://context"), "", metadata1);
            final RyaStatement statement2 = new RyaStatement(new RyaIRI("http://Joe"), new RyaIRI("http://talksTo"),
                    new RyaIRI("http://Betty"), new RyaIRI("http://context"), "", metadata1);
            final RyaStatement statement3 = new RyaStatement(new RyaIRI("http://Fred"), new RyaIRI("http://talksTo"),
                    new RyaIRI("http://Amanda"), new RyaIRI("http://context"), "", metadata1);
            final RyaStatement statement4 = new RyaStatement(new RyaIRI("http://Joe"), new RyaIRI("http://talksTo"),
                    new RyaIRI("http://Wanda"), new RyaIRI("http://context"), "", metadata2);
            dao.add(statement1);
            dao.add(statement2);
            dao.add(statement3);
            dao.add(statement4);

            SailRepositoryConnection conn = new SailRepository(sail).getConnection();
            final TupleQueryResult result = conn.prepareTupleQuery(QueryLanguage.SPARQL, query2).evaluate();

            final Set<BindingSet> expected = new HashSet<>();
            final QueryBindingSet expected1 = new QueryBindingSet();
            expected1.addBinding("b", VF.createIRI("http://Betty"));
            expected1.addBinding("a", VF.createIRI("http://Joe"));
            expected1.addBinding("c", VF.createIRI("http://Doug"));
            expected.add(expected1);

            final Set<BindingSet> bsSet = new HashSet<>();
            while (result.hasNext()) {
                bsSet.add(result.next());
            }

            assertEquals(expected, bsSet);

            dao.delete(statement1, conf);
            dao.delete(statement2, conf);
            dao.delete(statement3, conf);
            dao.delete(statement4, conf);
        } finally {
            dao.destroy();
            sail.shutDown();
        }
    }
}
