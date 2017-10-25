package org.apache.rya.indexing;

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


import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.indexing.IndexingFunctionRegistry.FUNCTION_TYPE;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.accumulo.freetext.AccumuloFreeTextIndexer;
import org.apache.rya.indexing.accumulo.freetext.FreeTextTupleSet;
import org.apache.rya.indexing.accumulo.geo.GeoParseUtils;
import org.apache.rya.indexing.accumulo.geo.GeoTupleSet;
import org.apache.rya.indexing.accumulo.geo.OptionalConfigUtils;
import org.apache.rya.indexing.accumulo.temporal.AccumuloTemporalIndexer;
import org.apache.rya.indexing.mongodb.freetext.MongoFreeTextIndexer;
import org.apache.rya.indexing.mongodb.temporal.MongoTemporalIndexer;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.And;
import org.eclipse.rdf4j.query.algebra.Filter;
import org.eclipse.rdf4j.query.algebra.FunctionCall;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.LeftJoin;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.ValueConstant;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.helpers.QueryModelVisitorBase;

import com.google.common.collect.Lists;

public class GeoEnabledFilterFunctionOptimizer implements QueryOptimizer, Configurable {
    private static final Logger LOG = Logger.getLogger(GeoEnabledFilterFunctionOptimizer.class);
    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    private Configuration conf;
    private GeoIndexer geoIndexer;
    private FreeTextIndexer freeTextIndexer;
    private TemporalIndexer temporalIndexer;
    private boolean init = false;

    public GeoEnabledFilterFunctionOptimizer() {
    }

    public GeoEnabledFilterFunctionOptimizer(final AccumuloRdfConfiguration conf) throws AccumuloException, AccumuloSecurityException,
    TableNotFoundException, IOException, TableExistsException, NumberFormatException, UnknownHostException {
        this.conf = conf;
        init();
    }

    //setConf initializes FilterFunctionOptimizer so reflection can be used
    //to create optimizer in RdfCloudTripleStoreConnection
    @Override
    public void setConf(final Configuration conf) {
        this.conf = conf;
        //reset the init.
        init = false;
            init();
    }
    /**
     * Load instances of the selected indexers.  This is tricky because some (geomesa vs geowave) have incompatible dependencies (geotools versions).
     */
    private synchronized void init() {
        if (!init) {
			if (ConfigUtils.getUseMongo(conf)) {
				// create a new MongoGeoIndexer() without having it at compile time.
				geoIndexer = instantiate(GeoIndexerType.MONGO_DB.getGeoIndexerClassString(), GeoIndexer.class);
				geoIndexer.setConf(conf);
				freeTextIndexer = new MongoFreeTextIndexer();
				freeTextIndexer.setConf(conf);
				temporalIndexer = new MongoTemporalIndexer();
				temporalIndexer.setConf(conf);
			} else {
				final GeoIndexerType geoIndexerType = OptionalConfigUtils.getGeoIndexerType(conf);
				if (geoIndexerType == GeoIndexerType.UNSPECIFIED) {
					geoIndexer = instantiate(GeoIndexerType.GEO_MESA.getGeoIndexerClassString(), GeoIndexer.class);
				} else {
					geoIndexer = instantiate(geoIndexerType.getGeoIndexerClassString(), GeoIndexer.class);
				}
				geoIndexer.setConf(conf);
				freeTextIndexer = new AccumuloFreeTextIndexer();
				freeTextIndexer.setConf(conf);
                temporalIndexer = new AccumuloTemporalIndexer();
                temporalIndexer.setConf(conf);
            }
            init = true;
        }
    }


    @Override
    public void optimize(final TupleExpr tupleExpr, final Dataset dataset, final BindingSet bindings) {
     // find variables used in property and resource based searches:
        final SearchVarVisitor searchVars = new SearchVarVisitor();
        tupleExpr.visit(searchVars);
        // rewrites for property searches:
        processPropertySearches(tupleExpr, searchVars.searchProperties);

    }

    /**
     * helper to instantiate a class from a string class name.
     * @param className name of class to instantiate.
     * @param type base interface that the class immplements
     * @return the instance.
     */
    public static <T> T instantiate(final String className, final Class<T> type){
        try{
            return type.cast(Class.forName(className).newInstance());
        } catch(InstantiationException
              | IllegalAccessException
              | ClassNotFoundException e){
            throw new IllegalStateException(e);
        }
    }

    private void processPropertySearches(final TupleExpr tupleExpr, final Collection<Var> searchProperties) {
        final MatchStatementVisitor matchStatements = new MatchStatementVisitor(searchProperties);
        tupleExpr.visit(matchStatements);
        for (final StatementPattern matchStatement: matchStatements.matchStatements) {
            final Var subject = matchStatement.getSubjectVar();
            if (subject.hasValue() && !(subject.getValue() instanceof Resource)) {
                throw new IllegalArgumentException("Query error: Found " + subject.getValue() + ", expected an URI or BNode");
            }
            Validate.isTrue(subject.hasValue() || subject.getName() != null);
            Validate.isTrue(!matchStatement.getObjectVar().hasValue() && matchStatement.getObjectVar().getName() != null);
            buildQuery(tupleExpr, matchStatement);
        }
    }

    private void buildQuery(final TupleExpr tupleExpr, final StatementPattern matchStatement) {
        //If our IndexerExpr (to be) is the rhs-child of LeftJoin, we can safely make that a Join:
        //  the IndexerExpr will (currently) not return results that can deliver unbound variables.
        //This optimization should probably be generalized into a LeftJoin -> Join optimizer under certain conditions. Until that
        //  has been done, this code path at least takes care of queries generated by OpenSahara SparqTool that filter on OPTIONAL
        //  projections. E.g. summary~'full text search' (summary is optional). See #379
        if (matchStatement.getParentNode() instanceof LeftJoin) {
            final LeftJoin leftJoin = (LeftJoin)matchStatement.getParentNode();
            if (leftJoin.getRightArg() == matchStatement && leftJoin.getCondition() == null) {
                matchStatement.getParentNode().replaceWith(new Join(leftJoin.getLeftArg(), leftJoin.getRightArg()));
            }
        }
        final FilterFunction fVisitor = new FilterFunction(matchStatement.getObjectVar().getName());
        tupleExpr.visit(fVisitor);
        final List<IndexingExpr> results = Lists.newArrayList();
        for(int i = 0; i < fVisitor.func.size(); i++){
            results.add(new IndexingExpr(fVisitor.func.get(i), matchStatement, fVisitor.args.get(i)));
        }
        removeMatchedPattern(tupleExpr, matchStatement, new IndexerExprReplacer(results));
    }

    //find vars contained in filters
    private static class SearchVarVisitor extends QueryModelVisitorBase<RuntimeException> {
        private final Collection<Var> searchProperties = new ArrayList<Var>();

        @Override
        public void meet(final FunctionCall fn) {
            final IRI fun = VF.createIRI(fn.getURI());
            final Var result = IndexingFunctionRegistry.getResultVarFromFunctionCall(fun, fn.getArgs());
            if (result != null && !searchProperties.contains(result)) {
                searchProperties.add(result);
            }
        }
    }

    //find StatementPatterns containing filter variables
    private static class MatchStatementVisitor extends QueryModelVisitorBase<RuntimeException> {
        private final Collection<Var> propertyVars;
        private final Collection<Var> usedVars = new ArrayList<Var>();
        private final List<StatementPattern> matchStatements = new ArrayList<StatementPattern>();

        public MatchStatementVisitor(final Collection<Var> propertyVars) {
            this.propertyVars = propertyVars;
        }

        @Override public void meet(final StatementPattern statement) {
            final Var object = statement.getObjectVar();
            if (propertyVars.contains(object)) {
                if (usedVars.contains(object)) {
                    throw new IllegalArgumentException("Illegal search, variable is used multiple times as object: " + object.getName());
                } else {
                    usedVars.add(object);
                    matchStatements.add(statement);
                }
            }
        }
    }

    private abstract class AbstractEnhanceVisitor extends AbstractQueryModelVisitor<RuntimeException> {
        final String matchVar;
        List<IRI> func = Lists.newArrayList();
        List<Object[]> args = Lists.newArrayList();

        public AbstractEnhanceVisitor(final String matchVar) {
            this.matchVar = matchVar;
        }

        protected void addFilter(final IRI uri, final Object[] values) {
            func.add(uri);
            args.add(values);
        }
    }

    //create indexing expression for each filter matching var in filter StatementPattern
    //replace old filter condition with true condition
    private class FilterFunction extends AbstractEnhanceVisitor {
        public FilterFunction(final String matchVar) {
            super(matchVar);
        }

        @Override
        public void meet(final FunctionCall call) {
            final IRI fnUri = VF.createIRI(call.getURI());
            final Var resultVar = IndexingFunctionRegistry.getResultVarFromFunctionCall(fnUri, call.getArgs());
            if (resultVar != null && resultVar.getName().equals(matchVar)) {
                addFilter(VF.createIRI(call.getURI()), GeoParseUtils.extractArguments(matchVar, call));
                if (call.getParentNode() instanceof Filter || call.getParentNode() instanceof And || call.getParentNode() instanceof LeftJoin) {
                    call.replaceWith(new ValueConstant(VF.createLiteral(true)));
                } else {
                    throw new IllegalArgumentException("Query error: Found " + call + " as part of an expression that is too complex");
                }
            }
        }

        @Override
        public void meet(final Filter filter) {
            //First visit children, then condition (reverse of default):
            filter.getArg().visit(this);
            filter.getCondition().visit(this);
        }
    }

    private void removeMatchedPattern(final TupleExpr tupleExpr, final StatementPattern pattern, final TupleExprReplacer replacer) {
        final List<TupleExpr> indexTuples = replacer.createReplacement(pattern);
        if (indexTuples.size() > 1) {
            final VarExchangeVisitor vev = new VarExchangeVisitor(pattern);
            tupleExpr.visit(vev);
            Join join = new Join(indexTuples.remove(0), indexTuples.remove(0));
            for (final TupleExpr geo : indexTuples) {
                join = new Join(join, geo);
            }
            pattern.replaceWith(join);
        } else if (indexTuples.size() == 1) {
            pattern.replaceWith(indexTuples.get(0));
            pattern.setParentNode(null);
        } else {
            throw new IllegalStateException("Must have at least one replacement for matched StatementPattern.");
        }
    }

    private interface TupleExprReplacer {
        List<TupleExpr> createReplacement(TupleExpr org);
    }

    //replace each filter pertinent StatementPattern with corresponding index expr
    private class IndexerExprReplacer implements TupleExprReplacer {
        private final List<IndexingExpr> indxExpr;
        private final FUNCTION_TYPE type;

        public IndexerExprReplacer(final List<IndexingExpr> indxExpr) {
            this.indxExpr = indxExpr;
            final IRI func = indxExpr.get(0).getFunction();
            type = IndexingFunctionRegistry.getFunctionType(func);
        }

        @Override
        public List<TupleExpr> createReplacement(final TupleExpr org) {
            final List<TupleExpr> indexTuples = Lists.newArrayList();
            switch (type) {
            case GEO:
                for (final IndexingExpr indx : indxExpr) {
                    indexTuples.add(new GeoTupleSet(indx, geoIndexer));
                }
                break;
            case FREETEXT:
                for (final IndexingExpr indx : indxExpr) {
                    indexTuples.add(new FreeTextTupleSet(indx, freeTextIndexer));
                }
                break;
            case TEMPORAL:
                for (final IndexingExpr indx : indxExpr) {
                    indexTuples.add(new TemporalTupleSet(indx, temporalIndexer));
                }
                break;
            default:
                throw new IllegalArgumentException("Incorrect type!");
            }
            return indexTuples;
        }
    }

    private static class VarExchangeVisitor extends QueryModelVisitorBase<RuntimeException> {
        private final  StatementPattern exchangeVar;
        public VarExchangeVisitor(final StatementPattern sp) {
            exchangeVar = sp;
        }

        @Override
        public void meet(final Join node) {
            final QueryModelNode lNode = node.getLeftArg();
            if (lNode instanceof StatementPattern) {
                exchangeVar.replaceWith(lNode);
                node.setLeftArg(exchangeVar);
            } else {
                super.meet(node);
            }
        }
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
}
