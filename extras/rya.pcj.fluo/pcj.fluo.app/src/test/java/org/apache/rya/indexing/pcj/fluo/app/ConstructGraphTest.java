package org.apache.rya.indexing.pcj.fluo.app;
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.model.VisibilityBindingSet;
import org.junit.Test;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

import com.google.common.collect.Sets;

public class ConstructGraphTest {

    private ValueFactory vf = new ValueFactoryImpl();
    
    @Test
    public void testConstructGraph() throws MalformedQueryException, UnsupportedEncodingException {
        String query = "select ?x where { ?x <uri:talksTo> <uri:Bob>. ?y <uri:worksAt> ?z }";

        SPARQLParser parser = new SPARQLParser();
        ParsedQuery pq = parser.parseQuery(query, null);
        List<StatementPattern> patterns = StatementPatternCollector.process(pq.getTupleExpr());
        ConstructGraph graph = new ConstructGraph(patterns);

        QueryBindingSet bs = new QueryBindingSet();
        bs.addBinding("x", vf.createURI("uri:Joe"));
        bs.addBinding("y", vf.createURI("uri:Bob"));
        bs.addBinding("z", vf.createURI("uri:BurgerShack"));
        VisibilityBindingSet vBs = new VisibilityBindingSet(bs,"FOUO");
        Set<RyaStatement> statements = graph.createGraphFromBindingSet(vBs);
        
        RyaStatement statement1 = new RyaStatement(new RyaURI("uri:Joe"), new RyaURI("uri:talksTo"), new RyaURI("uri:Bob"));
        RyaStatement statement2 = new RyaStatement(new RyaURI("uri:Bob"), new RyaURI("uri:worksAt"), new RyaURI("uri:BurgerShack"));
        Set<RyaStatement> expected = Sets.newHashSet(Arrays.asList(statement1, statement2));
        expected.forEach(x-> x.setColumnVisibility("FOUO".getBytes()));
        ConstructGraphTestUtils.ryaStatementSetsEqualIgnoresTimestamp(expected, statements);
    }
    
    @Test
    public void testConstructGraphBNode() throws MalformedQueryException {
        String query = "select ?x where { _:b <uri:talksTo> ?x. _:b <uri:worksAt> ?z }";

        SPARQLParser parser = new SPARQLParser();
        ParsedQuery pq = parser.parseQuery(query, null);
        List<StatementPattern> patterns = StatementPatternCollector.process(pq.getTupleExpr());
        ConstructGraph graph = new ConstructGraph(patterns);

        QueryBindingSet bs = new QueryBindingSet();
        bs.addBinding("x", vf.createURI("uri:Joe"));
        bs.addBinding("z", vf.createURI("uri:BurgerShack"));
        VisibilityBindingSet vBs = new VisibilityBindingSet(bs, "FOUO");
        Set<RyaStatement> statements = graph.createGraphFromBindingSet(vBs);
        Set<RyaStatement> statements2 = graph.createGraphFromBindingSet(vBs);
        
        RyaURI subject = null;
        for(RyaStatement statement: statements) {
            RyaURI subjURI = statement.getSubject();
            if(subject == null) {
                subject = subjURI;
            } else {
                assertEquals(subjURI, subject);
            }
        }
        RyaURI subject2 = null;
        for(RyaStatement statement: statements2) {
            RyaURI subjURI = statement.getSubject();
            if(subject2 == null) {
                subject2 = subjURI;
            } else {
                assertEquals(subjURI, subject2);
            }
        }
        
        assertTrue(!subject.equals(subject2));

        ConstructGraphTestUtils.ryaStatementsEqualIgnoresBlankNode(statements, statements2);
    }
    
    
    @Test
    public void testConstructGraphSerializer() throws MalformedQueryException {
        
        String query = "select ?x where { ?x <uri:talksTo> <uri:Bob>. ?y <uri:worksAt> ?z }";

        SPARQLParser parser = new SPARQLParser();
        ParsedQuery pq = parser.parseQuery(query, null);
        List<StatementPattern> patterns = StatementPatternCollector.process(pq.getTupleExpr());
        ConstructGraph graph = new ConstructGraph(patterns);
        
        String constructString = ConstructGraphSerializer.toConstructString(graph);
        ConstructGraph deserialized = ConstructGraphSerializer.toConstructGraph(constructString);
        
        assertEquals(graph, deserialized);
        
    }
    
    @Test
    public void testConstructGraphSerializerBlankNode() throws MalformedQueryException {
        
        String query = "select ?x where { _:b <uri:talksTo> ?x. _:b <uri:worksAt> ?y }";

        SPARQLParser parser = new SPARQLParser();
        ParsedQuery pq = parser.parseQuery(query, null);
        List<StatementPattern> patterns = StatementPatternCollector.process(pq.getTupleExpr());
        ConstructGraph graph = new ConstructGraph(patterns);
        
        String constructString = ConstructGraphSerializer.toConstructString(graph);
        ConstructGraph deserialized = ConstructGraphSerializer.toConstructGraph(constructString);
        
        assertEquals(graph, deserialized);
        
    }
    
}
