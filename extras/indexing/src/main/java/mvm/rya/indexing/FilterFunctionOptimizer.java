package mvm.rya.indexing;

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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.indexing.IndexingFunctionRegistry.FUNCTION_TYPE;
import mvm.rya.indexing.accumulo.ConfigUtils;
import mvm.rya.indexing.accumulo.freetext.AccumuloFreeTextIndexer;
import mvm.rya.indexing.accumulo.freetext.FreeTextTupleSet;
import mvm.rya.indexing.accumulo.geo.GeoMesaGeoIndexer;
import mvm.rya.indexing.accumulo.geo.GeoTupleSet;
import mvm.rya.indexing.accumulo.temporal.AccumuloTemporalIndexer;
import mvm.rya.indexing.accumulo.temporal.TemporalTupleSet;
import mvm.rya.indexing.mongodb.MongoGeoIndexer;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.geotools.feature.SchemaException;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.And;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.FunctionCall;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.LeftJoin;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.ValueConstant;
import org.openrdf.query.algebra.ValueExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.evaluation.QueryOptimizer;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

import com.google.common.collect.Lists;

public class FilterFunctionOptimizer implements QueryOptimizer, Configurable {

    private ValueFactory valueFactory = new ValueFactoryImpl();
    
    private Configuration conf;
    private GeoIndexer geoIndexer;
    private FreeTextIndexer freeTextIndexer;
    private TemporalIndexer temporalIndexer;
    private boolean init = false;
    
    
    public FilterFunctionOptimizer() {
    }
    
    
    public FilterFunctionOptimizer(AccumuloRdfConfiguration conf) throws AccumuloException, AccumuloSecurityException, 
    TableNotFoundException, IOException, SchemaException, TableExistsException {
        this.conf = conf;
        init();
    }
    
    //setConf initializes FilterFunctionOptimizer so reflection can be used
    //to create optimizer in RdfCloudTripleStoreConnection
    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        init();
    }
    
    
    private void init() {
        if (!init) {
            if (ConfigUtils.getUseMongo(conf)) {
                this.geoIndexer = new MongoGeoIndexer();
                geoIndexer.setConf(conf);
            } else {
                this.geoIndexer = new GeoMesaGeoIndexer();
                geoIndexer.setConf(conf);
                this.freeTextIndexer = new AccumuloFreeTextIndexer();
                freeTextIndexer.setConf(conf);
                this.temporalIndexer = new AccumuloTemporalIndexer();
                temporalIndexer.setConf(conf);
                init = true; 
            }
        }
    }
    
    @Override
    public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
     // find variables used in property and resource based searches:
        SearchVarVisitor searchVars = new SearchVarVisitor();
        tupleExpr.visit(searchVars);
        // rewrites for property searches:
        processPropertySearches(tupleExpr, searchVars.searchProperties);

    }
    
    
    
    private void processPropertySearches(TupleExpr tupleExpr, Collection<Var> searchProperties) {
        MatchStatementVisitor matchStatements = new MatchStatementVisitor(searchProperties);
        tupleExpr.visit(matchStatements);
        for (StatementPattern matchStatement: matchStatements.matchStatements) {
            Var subject = matchStatement.getSubjectVar();
            if (subject.hasValue() && !(subject.getValue() instanceof Resource))
                throw new IllegalArgumentException("Query error: Found " + subject.getValue() + ", expected an URI or BNode");
            Validate.isTrue(subject.hasValue() || subject.getName() != null);
            Validate.isTrue(!matchStatement.getObjectVar().hasValue() && matchStatement.getObjectVar().getName() != null);
            buildQuery(tupleExpr, matchStatement);
        }
        
    }
    
    
    private void buildQuery(TupleExpr tupleExpr, StatementPattern matchStatement) {
        //If our IndexerExpr (to be) is the rhs-child of LeftJoin, we can safely make that a Join:
        //  the IndexerExpr will (currently) not return results that can deliver unbound variables.
        //This optimization should probably be generalized into a LeftJoin -> Join optimizer under certain conditions. Until that
        //  has been done, this code path at least takes care of queries generated by OpenSahara SparqTool that filter on OPTIONAL
        //  projections. E.g. summary~'full text search' (summary is optional). See #379
        if (matchStatement.getParentNode() instanceof LeftJoin) {
            LeftJoin leftJoin = (LeftJoin)matchStatement.getParentNode();
            if (leftJoin.getRightArg() == matchStatement && leftJoin.getCondition() == null)
                matchStatement.getParentNode().replaceWith(new Join(leftJoin.getLeftArg(), leftJoin.getRightArg()));
        }
        FilterFunction fVisitor = new FilterFunction(matchStatement.getObjectVar().getName());
        tupleExpr.visit(fVisitor);
        List<IndexingExpr> results = Lists.newArrayList();
        for(int i = 0; i < fVisitor.func.size(); i++){
            results.add(new IndexingExpr(fVisitor.func.get(i), matchStatement, fVisitor.args.get(i)));  
        }
        removeMatchedPattern(tupleExpr, matchStatement, new IndexerExprReplacer(results));
        
    }
    
    //find vars contained in filters
    private static class SearchVarVisitor extends QueryModelVisitorBase<RuntimeException> {
        
        private final Collection<Var> searchProperties = new ArrayList<Var>(); 

        @Override
        public void meet(FunctionCall fn) {
            URI fun = new URIImpl(fn.getURI());
            Var result = IndexingFunctionRegistry.getResultVarFromFunctionCall(fun, fn.getArgs());
            if (result != null && !searchProperties.contains(result))
                searchProperties.add(result);
        }
    }

    //find StatementPatterns containing filter variables
    private static class MatchStatementVisitor extends QueryModelVisitorBase<RuntimeException> {
        private final Collection<Var> propertyVars;
        private final Collection<Var> usedVars = new ArrayList<Var>();
        private final List<StatementPattern> matchStatements = new ArrayList<StatementPattern>();

        public MatchStatementVisitor(Collection<Var> propertyVars) {
            this.propertyVars = propertyVars;
        }

        @Override public void meet(StatementPattern statement) {
            Var object = statement.getObjectVar();
            if (propertyVars.contains(object))
                if (usedVars.contains(object))
                    throw new IllegalArgumentException("Illegal search, variable is used multiple times as object: " + object.getName());
                else {
                    usedVars.add(object);
                    matchStatements.add(statement);
                }
        }
    }

    private abstract class AbstractEnhanceVisitor extends QueryModelVisitorBase<RuntimeException> {
        final String matchVar;
        List<URI> func = Lists.newArrayList();
        List<Value[]> args = Lists.newArrayList();

        public AbstractEnhanceVisitor(String matchVar) {
            this.matchVar = matchVar;
        }

        protected void addFilter(URI uri, Value[] values) {
            func.add(uri);
            args.add(values);
        }
    }

    //create indexing expression for each filter matching var in filter StatementPattern
    //replace old filter condition with true condition
    private class FilterFunction extends AbstractEnhanceVisitor {
        public FilterFunction(String matchVar) {
            super(matchVar);
        }

        @Override
        public void meet(FunctionCall call) {
            URI fnUri = valueFactory.createURI(call.getURI());
            Var resultVar = IndexingFunctionRegistry.getResultVarFromFunctionCall(fnUri, call.getArgs());
            if (resultVar != null && resultVar.getName().equals(matchVar)) {
                addFilter(valueFactory.createURI(call.getURI()), extractArguments(matchVar, call));
                if (call.getParentNode() instanceof Filter || call.getParentNode() instanceof And || call.getParentNode() instanceof LeftJoin)
                    call.replaceWith(new ValueConstant(valueFactory.createLiteral(true)));
                else
                    throw new IllegalArgumentException("Query error: Found " + call + " as part of an expression that is too complex");
            }
        }


        private Value[] extractArguments(String matchName, FunctionCall call) {
            Value args[] = new Value[call.getArgs().size() - 1];
            int argI = 0;
            for (int i = 0; i != call.getArgs().size(); ++i) {
                ValueExpr arg = call.getArgs().get(i);
                if (argI == i && arg instanceof Var && matchName.equals(((Var)arg).getName()))
                    continue;
                if (arg instanceof ValueConstant)
                    args[argI] = ((ValueConstant)arg).getValue();
                else if (arg instanceof Var && ((Var)arg).hasValue())
                    args[argI] = ((Var)arg).getValue();
                else
                    throw new IllegalArgumentException("Query error: Found " + arg + ", expected a Literal, BNode or URI");
                ++argI;
            }
            return args;
        }

        @Override
        public void meet(Filter filter) {
            //First visit children, then condition (reverse of default):
            filter.getArg().visit(this);
            filter.getCondition().visit(this);
        }
    }

    private void removeMatchedPattern(TupleExpr tupleExpr, StatementPattern pattern, TupleExprReplacer replacer) {
        List<TupleExpr> indexTuples = replacer.createReplacement(pattern);
        if (indexTuples.size() > 1) {
            VarExchangeVisitor vev = new VarExchangeVisitor(pattern);
            tupleExpr.visit(vev);
            Join join = new Join(indexTuples.remove(0), indexTuples.remove(0));
            for (TupleExpr geo : indexTuples) {
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
        private FUNCTION_TYPE type;

        public IndexerExprReplacer(List<IndexingExpr> indxExpr) {
            this.indxExpr = indxExpr;
            URI func = indxExpr.get(0).getFunction();
            this.type = IndexingFunctionRegistry.getFunctionType(func);
        }

        @Override
        public List<TupleExpr> createReplacement(TupleExpr org) {
            List<TupleExpr> indexTuples = Lists.newArrayList();
            switch (type) {
            case GEO:
                for (IndexingExpr indx : indxExpr) {
                    indexTuples.add(new GeoTupleSet(indx, geoIndexer));
                }
                break;
            case FREETEXT:
                for (IndexingExpr indx : indxExpr) {
                    indexTuples.add(new FreeTextTupleSet(indx, freeTextIndexer));
                }
                break;
            case TEMPORAL:
                for (IndexingExpr indx : indxExpr) {
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
              
        public VarExchangeVisitor(StatementPattern sp) {
            this.exchangeVar = sp;
        }

        @Override
        public void meet(Join node) {
            QueryModelNode lNode = node.getLeftArg();
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
