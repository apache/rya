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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.domain.VarNameUtils;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.external.matching.JoinSegment;
import org.apache.rya.indexing.statement.metadata.matching.StatementMetadataExternalSetProvider;
import org.apache.rya.indexing.statement.metadata.matching.StatementMetadataNode;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.algebra.Filter;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.helpers.StatementPatternCollector;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;
import org.junit.Assert;
import org.junit.Test;

public class StatementMetadataExternalSetProviderTest {

    private final String query = "prefix owl: <http://www.w3.org/2002/07/owl#> prefix ano: <http://www.w3.org/2002/07/owl#annotated> prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> select ?x ?y where {_:blankNode rdf:type owl:Annotation; ano:Source <http://Joe>; "
            + "ano:Property <http://worksAt>; ano:Target ?x; <http://createdBy> ?y; <http://createdOn> \'2017-01-04\'^^xsd:date }";
    private final String query3 = "prefix owl: <http://www.w3.org/2002/07/owl#> prefix ano: <http://www.w3.org/2002/07/owl#annotated> prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> select ?x ?y where {_:blankNode1 rdf:type owl:Annotation. _:blankNode2 rdf:type owl:Annotation; ano:Source <http://Bob>; "
            + "ano:Property <http://worksAt>; ano:Target ?x; <http://createdBy> ?y; <http://createdOn> \'2017-02-04\'^^xsd:date }";
    private final String query2 = "prefix owl: <http://www.w3.org/2002/07/owl#> prefix ano: <http://www.w3.org/2002/07/owl#annotated> prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> select ?x ?y where {_:blankNode1 rdf:type owl:Annotation; ano:Source <http://Joe>; "
            + "ano:Property <http://worksAt>; ano:Target ?x; <http://createdBy> ?y; <http://createdOn> \'2017-01-04\'^^xsd:date. "
            + "_:blankNode2 rdf:type owl:Annotation; ano:Source <http://Bob>; "
            + "ano:Property <http://worksAt>; ano:Target ?x; <http://createdBy> ?y; <http://createdOn> \'2017-02-04\'^^xsd:date }";

    @Test
    public void createSingleAccumuloMetadataNode() throws MalformedQueryException {

        AccumuloRdfConfiguration conf = (AccumuloRdfConfiguration) getConf(false);
        Set<RyaURI> propertySet = new HashSet<>();
        propertySet.add(new RyaURI("http://createdBy"));
        conf.setStatementMetadataProperties(propertySet);
        StatementMetadataExternalSetProvider metaProvider = new StatementMetadataExternalSetProvider(
                conf);
        SPARQLParser parser = new SPARQLParser();
        ParsedQuery pq = parser.parseQuery(query, null);

        List<QueryModelNode> patterns = new ArrayList<>();
        List<StatementMetadataNode<?>> expected = new ArrayList<>();
        Set<StatementPattern> sp = StatementMetadataTestUtils.getMetadataStatementPatterns(pq.getTupleExpr(), propertySet);

        patterns.addAll(StatementPatternCollector.process(pq.getTupleExpr()));
        JoinSegment<StatementMetadataNode<?>> segment = new JoinSegment<>(
                new HashSet<QueryModelNode>(patterns), patterns, new HashMap<ValueExpr, Filter>());
        List<StatementMetadataNode<?>> extSets = metaProvider.getExternalSets(segment);

        expected.add(new StatementMetadataNode<>(sp, conf));

        Assert.assertEquals(expected, extSets);

    }
    
    @Test
    public void createSingleMongoMetadataNode() throws MalformedQueryException {

        MongoDBRdfConfiguration conf = (MongoDBRdfConfiguration) getConf(true);
        Set<RyaURI> propertySet = new HashSet<>();
        propertySet.add(new RyaURI("http://createdBy"));
        conf.setStatementMetadataProperties(propertySet);
        StatementMetadataExternalSetProvider metaProvider = new StatementMetadataExternalSetProvider(conf);
        SPARQLParser parser = new SPARQLParser();
        ParsedQuery pq = parser.parseQuery(query, null);

        List<QueryModelNode> patterns = new ArrayList<>();
        List<StatementMetadataNode<?>> expected = new ArrayList<>();
        Set<StatementPattern> sp = StatementMetadataTestUtils.getMetadataStatementPatterns(pq.getTupleExpr(), propertySet);

        patterns.addAll(StatementPatternCollector.process(pq.getTupleExpr()));
        JoinSegment<StatementMetadataNode<?>> segment = new JoinSegment<>(
                new HashSet<QueryModelNode>(patterns), patterns, new HashMap<ValueExpr, Filter>());
        List<StatementMetadataNode<?>> extSets = metaProvider.getExternalSets(segment);

        expected.add(new StatementMetadataNode<>(sp,conf));

        Assert.assertEquals(expected, extSets);

    }
    
    
    @Test
    public void createMultipleMetadataNode() throws MalformedQueryException {

        MongoDBRdfConfiguration conf = (MongoDBRdfConfiguration) getConf(true);
        Set<RyaURI> propertySet = new HashSet<>();
        propertySet.add(new RyaURI("http://createdBy"));
        propertySet.add(new RyaURI("http://createdOn"));
        conf.setStatementMetadataProperties(propertySet);
        StatementMetadataExternalSetProvider metaProvider = new StatementMetadataExternalSetProvider(conf);
        SPARQLParser parser = new SPARQLParser();
        ParsedQuery pq2 = parser.parseQuery(query2, null);
        ParsedQuery pq3 = parser.parseQuery(query3, null);
        ParsedQuery pq1 = parser.parseQuery(query, null);

        List<QueryModelNode> patterns = new ArrayList<>();
        List<StatementMetadataNode<?>> expected = new ArrayList<>();
        Set<StatementPattern> sp1 = StatementMetadataTestUtils.getMetadataStatementPatterns(pq1.getTupleExpr(), propertySet);
        Set<StatementPattern> sp3 = StatementMetadataTestUtils.getMetadataStatementPatterns(pq3.getTupleExpr(), propertySet);
        //added extra blankNode into query3 to make blankNode names line up with query2.  Need to remove it now so that
        //StatementMetadataNode doesn't blow up because all subjects aren't the same.
        removePatternWithGivenSubject(VarNameUtils.prependAnonymous("1"), sp3);

        patterns.addAll(StatementPatternCollector.process(pq2.getTupleExpr()));
        JoinSegment<StatementMetadataNode<?>> segment = new JoinSegment<>(
                new HashSet<QueryModelNode>(patterns), patterns, new HashMap<ValueExpr, Filter>());
        List<StatementMetadataNode<?>> extSets = metaProvider.getExternalSets(segment);

        expected.add(new StatementMetadataNode<>(sp1,conf));
        expected.add(new StatementMetadataNode<>(sp3,conf));

        Assert.assertEquals(expected, extSets);
    }

    private static Configuration getConf(boolean useMongo) {

        if (useMongo) {
            MongoDBRdfConfiguration conf = new MongoDBRdfConfiguration();
            conf.setBoolean("sc.useMongo", true);
            conf.setMongoInstance("localhost");
            conf.setMongoPort("27017");
            conf.setMongoDBName("rya_");
            return conf;
        } else {

            AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
            conf.setBoolean(ConfigUtils.USE_MOCK_INSTANCE, true);
            conf.set(RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX, "rya_");
            conf.set(ConfigUtils.CLOUDBASE_USER, "root");
            conf.set(ConfigUtils.CLOUDBASE_PASSWORD, "");
            conf.set(ConfigUtils.CLOUDBASE_INSTANCE, "instance");
            conf.set(ConfigUtils.CLOUDBASE_AUTHS, "");

            return conf;
        }
    }
    
    private void removePatternWithGivenSubject(String subject, Set<StatementPattern> patterns) {
        Iterator<StatementPattern> spIter = patterns.iterator();
        while(spIter.hasNext()) {
            StatementPattern sp = spIter.next();
            if(sp.getSubjectVar().getName().equals(subject)) {
                spIter.remove();
            }
        }
    }

}
