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
package org.apache.rya.indexing.pcj.fluo.app.query;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants;
import org.apache.rya.indexing.pcj.fluo.app.NodeType;
import org.apache.rya.indexing.pcj.fluo.app.util.PeriodicQueryUtil;
import org.apache.rya.indexing.pcj.fluo.app.util.PeriodicQueryUtil.PeriodicQueryNodeRelocator;
import org.apache.rya.indexing.pcj.fluo.app.util.PeriodicQueryUtil.PeriodicQueryNodeVisitor;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.junit.Assert;
import org.junit.Test;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.FunctionCall;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.ValueConstant;
import org.openrdf.query.algebra.ValueExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

public class PeriodicQueryUtilTest {

    private static final ValueFactory vf = new ValueFactoryImpl();

   
    
    @Test
    public void periodicNodeNotPresentTest() throws Exception {
        
        List<ValueExpr> values = Arrays.asList(new Var("time"), new ValueConstant(vf.createLiteral(12.0)), new ValueConstant(vf.createLiteral(6.0)), new ValueConstant(vf.createURI(PeriodicQueryUtil.temporalNameSpace + "hours")));
        FunctionCall func = new FunctionCall("uri:func", values);
        Optional<PeriodicQueryNode> node1 = PeriodicQueryUtil.getPeriodicQueryNode(func, new Join());
        Assert.assertEquals(false, node1.isPresent());
    }

    
    
    @Test
    public void periodicNodePresentTest() throws Exception {
        
        List<ValueExpr> values = Arrays.asList(new Var("time"), new ValueConstant(vf.createLiteral(12.0)), new ValueConstant(vf.createLiteral(6.0)), new ValueConstant(vf.createURI(PeriodicQueryUtil.temporalNameSpace + "hours")));
        FunctionCall func = new FunctionCall(PeriodicQueryUtil.PeriodicQueryURI, values);
        Optional<PeriodicQueryNode> node1 = PeriodicQueryUtil.getPeriodicQueryNode(func, new Join());
        Assert.assertEquals(true, node1.isPresent());
        
        PeriodicQueryNode node2 = new PeriodicQueryNode(12*60*60*1000L, 6*3600*1000L, TimeUnit.MILLISECONDS, "time", new Join());
        
        Assert.assertEquals(true, periodicNodesEqualIgnoreArg(node1.get(), node2));
    }
    
    
    @Test
    public void periodicNodeFractionalDurationTest() throws Exception {
        
        List<ValueExpr> values = Arrays.asList(new Var("time"), new ValueConstant(vf.createLiteral(1)), new ValueConstant(vf.createLiteral(.5)), new ValueConstant(vf.createURI(PeriodicQueryUtil.temporalNameSpace + "hours")));
        FunctionCall func = new FunctionCall(PeriodicQueryUtil.PeriodicQueryURI, values);
        Optional<PeriodicQueryNode> node1 = PeriodicQueryUtil.getPeriodicQueryNode(func, new Join());
        Assert.assertEquals(true, node1.isPresent());
        
        double window = 1*60*60*1000;
        double period = .5*3600*1000;
        
        PeriodicQueryNode node2 = new PeriodicQueryNode((long) window, (long) period, TimeUnit.MILLISECONDS, "time", new Join());
        
        Assert.assertEquals(true, periodicNodesEqualIgnoreArg(node1.get(), node2));
    }
    
    @Test
    public void testPeriodicNodePlacement() throws MalformedQueryException {
         String query = "prefix function: <http://org.apache.rya/function#> " //n
                + "prefix time: <http://www.w3.org/2006/time#> " //n
                + "prefix fn: <http://www.w3.org/2006/fn#> " //n
                + "select ?obs ?time ?lat where {" //n
                + "Filter(function:periodic(?time, 12.0, 6.0,time:hours)) " //n
                + "Filter(fn:test(?lat, 25)) " //n
                + "?obs <uri:hasTime> ?time. " //n
                + "?obs <uri:hasLattitude> ?lat }"; //n
         
         SPARQLParser parser = new SPARQLParser();
         ParsedQuery pq = parser.parseQuery(query, null);
         TupleExpr te = pq.getTupleExpr();
         te.visit(new PeriodicQueryNodeVisitor());
         
         PeriodicNodeCollector collector = new PeriodicNodeCollector();
         te.visit(collector);
         
         PeriodicQueryNode node2 = new PeriodicQueryNode(12*60*60*1000L, 6*3600*1000L, TimeUnit.MILLISECONDS, "time", new Join());

         Assert.assertEquals(true, periodicNodesEqualIgnoreArg(node2, collector.getPeriodicQueryNode()));
         
    }
    
    @Test
    public void testPeriodicNodeLocation() throws MalformedQueryException {
         String query = "prefix function: <http://org.apache.rya/function#> " //n
                + "prefix time: <http://www.w3.org/2006/time#> " //n
                + "prefix fn: <http://www.w3.org/2006/fn#> " //n
                + "select ?obs ?time ?lat where {" //n
                + "Filter(function:periodic(?time, 1,.5,time:hours)) " //n
                + "Filter(fn:test(?lat, 25)) " //n
                + "?obs <uri:hasTime> ?time. " //n
                + "?obs <uri:hasLattitude> ?lat }"; //n
         
         SPARQLParser parser = new SPARQLParser();
         ParsedQuery pq = parser.parseQuery(query, null);
         TupleExpr te = pq.getTupleExpr();
         te.visit(new PeriodicQueryNodeVisitor());
        
         PeriodicNodeCollector collector = new PeriodicNodeCollector();
         te.visit(collector);
         Assert.assertEquals(2, collector.getPos());
         
         te.visit(new PeriodicQueryNodeRelocator());
         collector.resetCount();
         te.visit(collector);
         Assert.assertEquals(1, collector.getPos());
         
         double window = 1*60*60*1000;
         double period = .5*3600*1000;
         PeriodicQueryNode node2 = new PeriodicQueryNode((long) window, (long) period, TimeUnit.MILLISECONDS, "time", new Join());
         Assert.assertEquals(true, periodicNodesEqualIgnoreArg(node2, collector.getPeriodicQueryNode()));
         
    }
    
    @Test
    public void testFluoQueryVarOrders() throws MalformedQueryException, UnsupportedQueryException {
        String query = "prefix function: <http://org.apache.rya/function#> " //n
                + "prefix time: <http://www.w3.org/2006/time#> " //n
                + "select (count(?obs) as ?total) where {" //n
                + "Filter(function:periodic(?time, 12.4, 6.2,time:hours)) " //n
                + "?obs <uri:hasTime> ?time. " //n
                + "?obs <uri:hasLattitude> ?lat }"; //n
         
         SparqlFluoQueryBuilder builder = new SparqlFluoQueryBuilder();
         builder.setSparql(query);
         builder.setFluoQueryId(NodeType.generateNewFluoIdForType(NodeType.QUERY));
         FluoQuery fluoQuery = builder.build();
         
         PeriodicQueryMetadata periodicMeta = fluoQuery.getPeriodicQueryMetadata().orNull();
         Assert.assertEquals(true, periodicMeta != null);
         VariableOrder periodicVars = periodicMeta.getVariableOrder();
         Assert.assertEquals(IncrementalUpdateConstants.PERIODIC_BIN_ID, periodicVars.getVariableOrders().get(0));
         
         QueryMetadata queryMeta = fluoQuery.getQueryMetadata();
         VariableOrder queryVars = queryMeta.getVariableOrder();
         Assert.assertEquals(IncrementalUpdateConstants.PERIODIC_BIN_ID, queryVars.getVariableOrders().get(0));
         
         Collection<AggregationMetadata> aggMetaCollection = fluoQuery.getAggregationMetadata();
         Assert.assertEquals(1, aggMetaCollection.size());
         AggregationMetadata aggMeta = aggMetaCollection.iterator().next();
         VariableOrder aggVars = aggMeta.getVariableOrder();
         Assert.assertEquals(IncrementalUpdateConstants.PERIODIC_BIN_ID, aggVars.getVariableOrders().get(0));
         
         System.out.println(fluoQuery);
    }

    private boolean periodicNodesEqualIgnoreArg(PeriodicQueryNode node1, PeriodicQueryNode node2) {
        return new EqualsBuilder().append(node1.getPeriod(), node2.getPeriod()).append(node1.getWindowSize(), node2.getWindowSize())
                .append(node1.getTemporalVariable(), node2.getTemporalVariable()).append(node1.getUnit(), node2.getUnit()).build();
    }
    
    private static class PeriodicNodeCollector extends QueryModelVisitorBase<RuntimeException>{
        
        private PeriodicQueryNode periodicNode;
        int count = 0;
        
        public PeriodicQueryNode getPeriodicQueryNode() {
            return periodicNode;
        }
        
        public int getPos() {
            return count;
        }
        
        public void resetCount() {
            count = 0;
        }
        
        public void meet(Filter node) {
            count++;
            node.getArg().visit(this);
        }
        
        public void meet(Projection node) {
            count++;
            node.getArg().visit(this);
        }
        
        @Override
        public void meetOther(QueryModelNode node) {
            if(node instanceof PeriodicQueryNode) {
                periodicNode = (PeriodicQueryNode) node;
            }
        }
        
    }

}
