package org.apache.rya.indexing.accumulo.entity;

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


import info.aduna.iteration.CloseableIteration;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.accumulo.entity.StarQuery.CardinalityStatementPattern;
import org.apache.rya.joinselect.AccumuloSelectivityEvalDAO;
import org.apache.rya.prospector.service.ProspectorServiceEvalStatsDAO;
import org.apache.rya.rdftriplestore.RdfCloudTripleStore;
import org.apache.rya.rdftriplestore.RdfCloudTripleStoreConnection;
import org.apache.rya.rdftriplestore.evaluation.ExternalBatchingIterator;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.commons.io.IOUtils;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.algebra.evaluation.impl.ExternalSet;
import org.openrdf.sail.SailException;

import com.beust.jcommander.internal.Sets;
import com.google.common.base.Joiner;

public class EntityTupleSet extends ExternalSet implements ExternalBatchingIterator {


    private StarQuery starQuery;
    private RdfCloudTripleStoreConfiguration conf;
    private Set<String> variables;
    private double cardinality = -1;
    private StatementPattern minSp;
    private double minCard;
    private Connector accCon = null;
    private boolean evalOptUsed = false;

    public EntityTupleSet() {

    }

    public EntityTupleSet(StarQuery sq, RdfCloudTripleStoreConfiguration conf) {
        this.starQuery = sq;
        this.conf = conf;

        variables = Sets.newHashSet();
        if(!starQuery.commonVarConstant()) {
            variables.add(starQuery.getCommonVarName());
        }
        variables.addAll(starQuery.getUnCommonVars());

        init();

    }

    public EntityTupleSet(StarQuery sq, RdfCloudTripleStoreConfiguration conf, boolean evalOptUsed) {
        this(sq,conf);
        this.evalOptUsed = evalOptUsed;
    }

    private void init() {

        try {
            accCon = ConfigUtils.getConnector(conf);
        } catch (AccumuloException e) {
            e.printStackTrace();
        } catch (AccumuloSecurityException e) {
            e.printStackTrace();
        }
        if (conf.isUseStats() && conf.isUseSelectivity()) {

            ProspectorServiceEvalStatsDAO evalDao = new ProspectorServiceEvalStatsDAO(accCon, conf);
            evalDao.init();
            AccumuloSelectivityEvalDAO ase = new AccumuloSelectivityEvalDAO(conf, accCon);
            ase.setRdfEvalDAO(evalDao);
            ase.init();

            cardinality = starQuery.getCardinality(ase);
            CardinalityStatementPattern csp = starQuery.getMinCardSp(ase);

            minCard = csp.getCardinality();
            minSp = csp.getSp();
        } else {
            // TODO come up with a better default if cardinality is not
            // initialized
            cardinality = minCard = 1;
            minSp = starQuery.getNodes().get(0);
        }

    }

    @Override
    public Set<String> getBindingNames() {
        return starQuery.getBindingNames();
    }

    @Override
    public Set<String> getAssuredBindingNames() {
        return starQuery.getAssuredBindingNames();
    }

    public Set<String> getVariables() {
        return variables;
    }


    @Override
    public String getSignature() {
        return "(EntityCentric Projection) " + " common Var: " + starQuery.getCommonVarName() + "  variables: " + Joiner.on(", ").join(variables).replaceAll("\\s+", " ");
    }

    public StarQuery getStarQuery() {
        return starQuery;
    }

    public void setStarQuery(StarQuery sq) {
        this.starQuery = sq;
    }


    @Override
    public EntityTupleSet clone() {
        StarQuery sq = new StarQuery(starQuery);
        return new EntityTupleSet(sq, conf);
    }


    @Override
    public double cardinality() {
        return cardinality;
    }


    public double getMinSpCard() {
        return minCard;
    }


    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(BindingSet bindings) throws QueryEvaluationException {

        // if starQuery contains node with cardinality less than 1000 and node
        // only has one variable, and number of SPs in starQuery is greater than 2, it is
        // more efficient to first evaluate this node and then pass the bindings
        // into the remainder of the star query to be evaluated
        if (minCard < 1000 && starQuery.size() > 2 && numberOfSpVars(minSp) == 1 && !starQuery.commonVarConstant()) {

            try {
                RdfCloudTripleStoreConnection conn = getRyaSailConnection();
                CloseableIteration<BindingSet, QueryEvaluationException> sol = (CloseableIteration<BindingSet, QueryEvaluationException>) conn
                        .evaluate(minSp, null, bindings, false);

                Set<BindingSet> bSet = Sets.newHashSet();
                while (sol.hasNext()) {
                    //TODO this is not optimal - should check if bindings variables intersect minSp variables
                    //creating the following QueryBindingSet is only necessary if no intersection occurs
                    QueryBindingSet bs = new QueryBindingSet();
                    bs.addAll(sol.next());
                    bs.addAll(bindings);
                    bSet.add(bs);
                }

                List<StatementPattern> spList = starQuery.getNodes();
                spList.remove(minSp);

                StarQuery sq = new StarQuery(spList);
                conn.close();

                return new EntityTupleSet(sq, conf, true).evaluate(bSet);

            } catch (Exception e) {
                throw new QueryEvaluationException(e);
            }
        } else {
            this.evalOptUsed = true;
            return this.evaluate(Collections.singleton(bindings));
        }

    }


    private int numberOfSpVars(StatementPattern sp) {
        List<Var> varList = sp.getVarList();
        int varCount = 0;

        for(int i = 0; i < 3; i++) {
           if(!varList.get(i).isConstant()) {
               varCount++;
           }
        }

        return varCount;
    }


    @Override
    public CloseableIteration<BindingSet,QueryEvaluationException> evaluate(final Collection<BindingSet> bindingset) throws QueryEvaluationException {

        if(bindingset.size() < 2 && !this.evalOptUsed) {
            BindingSet bs = new QueryBindingSet();
            if (bindingset.size() == 1) {
                bs = bindingset.iterator().next();
            }
            return this.evaluate(bs);
        }
        //TODO possibly refactor if bindingset.size() > 0 to take advantage of optimization in evaluate(BindingSet bindingset)
        AccumuloDocIdIndexer adi = null;
        try {
            adi = new AccumuloDocIdIndexer(conf);
            return adi.queryDocIndex(starQuery, bindingset);
        } catch (Exception e) {
            throw new QueryEvaluationException(e);
        } finally {
            IOUtils.closeQuietly(adi);
        }
    }


    private RdfCloudTripleStoreConnection getRyaSailConnection() throws AccumuloException,
            AccumuloSecurityException, SailException {
        final RdfCloudTripleStore store = new RdfCloudTripleStore();
        AccumuloRyaDAO crdfdao = new AccumuloRyaDAO();
        crdfdao.setConnector(accCon);
        AccumuloRdfConfiguration acc = new AccumuloRdfConfiguration(conf);
        crdfdao.setConf(acc);
        store.setRyaDAO(crdfdao);
        store.initialize();

        return (RdfCloudTripleStoreConnection) store.getConnection();
    }


}
