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

import java.util.*;

import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.external.matching.JoinSegment;
import org.apache.rya.indexing.statement.metadata.matching.StatementMetadataExternalSetProvider;
import org.apache.rya.indexing.statement.metadata.matching.StatementMetadataNode;
import org.apache.rya.indexing.statement.metadata.matching.StatementMetadataOptimizer;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.algebra.*;
import org.eclipse.rdf4j.query.algebra.helpers.StatementPatternCollector;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assert.assertEquals;

@RunWith(value = Parameterized.class)
public class StatementMetadataOptimizerTest {

    private static final String query1 = "prefix owl: <http://www.w3.org/2002/07/owl#> prefix ano: <http://www.w3.org/2002/07/owl#annotated> prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> select ?x ?y where {_:blankNode rdf:type owl:Annotated; ano:Source <http://Joe>; "
            + "ano:Property <http://worksAt>; ano:Target ?x; <http://createdBy> ?y; <http://createdOn> \'2017-01-04\'^^xsd:date }";
    private static final String query2 = "prefix owl: <http://www.w3.org/2002/07/owl#> prefix ano: <http://www.w3.org/2002/07/owl#annotated> prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> select ?x ?y where {_:blankNode2 rdf:type owl:Annotated; ano:Source <http://Bob>; "
            + "ano:Property <http://worksAt>; ano:Target ?x; <http://createdBy> ?y; <http://createdOn> \'2017-02-04\'^^xsd:date }";
    private static final String query3 = "prefix owl: <http://www.w3.org/2002/07/owl#> prefix ano: <http://www.w3.org/2002/07/owl#annotated> prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> select ?x ?y where {_:blankNode2 rdf:type owl:Annotated; ano:Source <http://Frank>; "
            + "ano:Property <http://worksAt>; ano:Target ?x; <http://createdBy> ?y; <http://createdOn> \'2017-03-04\'^^xsd:date }";
    private static final String query4 = "prefix owl: <http://www.w3.org/2002/07/owl#> prefix ano: <http://www.w3.org/2002/07/owl#annotated> prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> select ?x ?y where {_:blankNode1 rdf:type owl:Annotated; ano:Source <http://Joe>; "
            + "ano:Property <http://worksAt>; ano:Target ?x; <http://createdBy> ?y; <http://createdOn> \'2017-01-04\'^^xsd:date. "
            + "_:blankNode2 rdf:type rdf:Statement; rdf:subject <http://Bob>; "
            + "ano:Property <http://worksAt>; ano:Target ?x; <http://createdBy> ?y; <http://createdOn> \'2017-02-04\'^^xsd:date }";
    private static final String query5 = "prefix owl: <http://www.w3.org/2002/07/owl#> prefix ano: <http://www.w3.org/2002/07/owl#annotated> prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> select ?x ?y where {_:blankNode1 rdf:type owl:Annotated; ano:Source <http://Joe>; "
            + "ano:Property <http://worksAt>; ano:Target ?x; <http://createdBy> ?y; <http://createdOn> \'2017-01-04\'^^xsd:date. "
            + "_:blankNode2 rdf:type owl:Annotated; rdf:subject <http://Bob>; "
            + "ano:Property <http://worksAt>; ano:Target ?a; <http://createdBy> ?b; <http://createdOn> \'2017-02-04\'^^xsd:date. "
            + "OPTIONAL{ _:blankNode3 rdf:type owl:Annotated; ano:Source <http://Frank>; "
            + "ano:Property <http://worksAt>; ano:Target ?c; <http://createdBy> <http://Doug>; <http://createdOn> \'2017-03-04\'^^xsd:date } }";
    private static final String query6 = "prefix owl: <http://www.w3.org/2002/07/owl#> prefix ano: <http://www.w3.org/2002/07/owl#annotated> prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> select ?x ?y where {?m rdf:type owl:Annotated; ano:Source <http://Joe>; "
            + "ano:Property <http://worksAt>; ano:Target ?x; <http://createdBy> ?y; <http://createdOn> \'2017-01-04\'^^xsd:date. "
            + "{ ?o rdf:type owl:Annotated; ano:Source <http://Frank>; "
            + "ano:Property <http://worksAt>; ano:Target ?c; <http://createdBy> ?p; <http://createdOn> \'2017-03-04\'^^xsd:date . } "
            + "UNION {?n ano:Property <http://worksAt>; ano:Target ?a; <http://createdBy> ?c; <http://createdOn> \'2017-02-04\'^^xsd:date; "
            + "rdf:type owl:Annotated; ano:Source ?p. } }";

    private String query;
    private Set<StatementMetadataNode<?>> expected;
    private static SPARQLParser parser = new SPARQLParser();
    private StatementMetadataOptimizer mongoOptimizer;
    private StatementMetadataOptimizer accumuloOptimizer;

    public StatementMetadataOptimizerTest(String query, Set<StatementMetadataNode<?>> expected) {
        this.query = query;
        this.expected = expected;
    }

    @Before
    public void init() {
        RdfCloudTripleStoreConfiguration mongoConf = getConf(true);
        RdfCloudTripleStoreConfiguration accumuloConf = getConf(false);
        mongoOptimizer = new StatementMetadataOptimizer(mongoConf);
        accumuloOptimizer = new StatementMetadataOptimizer(accumuloConf);
    }

    @Parameters
    public static Collection<Object[]> data() throws MalformedQueryException {
        return Arrays.asList(new Object[][] { { query1, getExpected(query1) }, { query2, getExpected(query2) },
                { query3, getExpected(query3) }, { query4, getExpected(query4) }, { query5, getExpected(query5) },
                { query6, getExpected(query6) } });
    }

    @Test
    public void testAccumuloMatchQuery() throws MalformedQueryException {
        ParsedQuery pq = parser.parseQuery(query, null);
        TupleExpr te = pq.getTupleExpr();
        System.out.println("Parametrized query is : " + te);
        accumuloOptimizer.optimize(te, null, null);
        System.out.println("Result of optimization is : " + te);
        assertEquals(expected, StatementMetadataTestUtils.getMetadataNodes(te));
    }

    @Test
    public void testMongoMatchQuery() throws MalformedQueryException {
        SPARQLParser parser = new SPARQLParser();
        ParsedQuery pq = parser.parseQuery(query, null);
        TupleExpr te = pq.getTupleExpr();
        System.out.println("Parametrized query is : " + te);
        mongoOptimizer.optimize(te, null, null);
        System.out.println("Result of optimization is : " + te);
        assertEquals(expected, StatementMetadataTestUtils.getMetadataNodes(te));
    }

    private static RdfCloudTripleStoreConfiguration getConf(boolean useMongo) {

        RdfCloudTripleStoreConfiguration conf;
        Set<RyaURI> propertySet = new HashSet<RyaURI>(
                Arrays.asList(new RyaURI("http://createdBy"), new RyaURI("http://createdOn")));
        if (useMongo) {
            MongoDBRdfConfiguration mConf = new MongoDBRdfConfiguration();
            mConf.setBoolean("sc.useMongo", true);
            mConf.setMongoInstance("localhost");
            mConf.setMongoPort("27017");
            mConf.setMongoDBName("rya_");
            conf = mConf;
        } else {
            conf = new AccumuloRdfConfiguration();
            conf.setBoolean(ConfigUtils.USE_MOCK_INSTANCE, true);
            conf.set(RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX, "rya_");
            conf.set(ConfigUtils.CLOUDBASE_USER, "root");
            conf.set(ConfigUtils.CLOUDBASE_PASSWORD, "");
            conf.set(ConfigUtils.CLOUDBASE_INSTANCE, "instance");
            conf.set(ConfigUtils.CLOUDBASE_AUTHS, "");
        }
        conf.setStatementMetadataProperties(propertySet);
        return conf;
    }

    private static Set<StatementMetadataNode<?>> getExpected(String query) throws MalformedQueryException {
        ParsedQuery pq = parser.parseQuery(query, null);
        StatementMetadataExternalSetProvider provider = new StatementMetadataExternalSetProvider(
                getConf(false));
        List<StatementPattern> patterns = StatementPatternCollector.process(pq.getTupleExpr());
        JoinSegment<StatementMetadataNode<?>> segment = new JoinSegment<StatementMetadataNode<?>>(
                new HashSet<QueryModelNode>(patterns), new ArrayList<QueryModelNode>(patterns),
                new HashMap<ValueExpr, Filter>());
        return new HashSet<>(provider.getExternalSets(segment));
    }
}
