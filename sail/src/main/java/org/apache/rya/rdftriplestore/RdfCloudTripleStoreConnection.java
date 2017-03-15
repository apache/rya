package org.apache.rya.rdftriplestore;

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



import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import info.aduna.iteration.CloseableIteration;

import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.RdfCloudTripleStoreConstants;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.domain.StatementMetadata;
import org.apache.rya.api.persist.RdfEvalStatsDAO;
import org.apache.rya.api.persist.RyaDAO;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.api.persist.joinselect.SelectivityEvalDAO;
import org.apache.rya.api.persist.utils.RyaDAOHelper;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.rdftriplestore.evaluation.FilterRangeVisitor;
import org.apache.rya.rdftriplestore.evaluation.ParallelEvaluationStrategyImpl;
import org.apache.rya.rdftriplestore.evaluation.QueryJoinSelectOptimizer;
import org.apache.rya.rdftriplestore.evaluation.RdfCloudTripleStoreEvaluationStatistics;
import org.apache.rya.rdftriplestore.evaluation.RdfCloudTripleStoreSelectivityEvaluationStatistics;
import org.apache.rya.rdftriplestore.evaluation.SeparateFilterJoinsVisitor;
import org.apache.rya.rdftriplestore.inference.InferenceEngine;
import org.apache.rya.rdftriplestore.inference.InverseOfVisitor;
import org.apache.rya.rdftriplestore.inference.PropertyChainVisitor;
import org.apache.rya.rdftriplestore.inference.SameAsVisitor;
import org.apache.rya.rdftriplestore.inference.SubClassOfVisitor;
import org.apache.rya.rdftriplestore.inference.SubPropertyOfVisitor;
import org.apache.rya.rdftriplestore.inference.SymmetricPropertyVisitor;
import org.apache.rya.rdftriplestore.inference.TransitivePropertyVisitor;
import org.apache.rya.rdftriplestore.namespace.NamespaceManager;
import org.apache.rya.rdftriplestore.provenance.ProvenanceCollectionException;
import org.apache.rya.rdftriplestore.provenance.ProvenanceCollector;
import org.apache.rya.rdftriplestore.utils.DefaultStatistics;

import org.apache.hadoop.conf.Configurable;
import org.openrdf.model.Namespace;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ContextStatementImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.QueryRoot;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.evaluation.EvaluationStrategy;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.algebra.evaluation.QueryOptimizer;
import org.openrdf.query.algebra.evaluation.TripleSource;
import org.openrdf.query.algebra.evaluation.impl.BindingAssigner;
import org.openrdf.query.algebra.evaluation.impl.CompareOptimizer;
import org.openrdf.query.algebra.evaluation.impl.ConjunctiveConstraintSplitter;
import org.openrdf.query.algebra.evaluation.impl.ConstantOptimizer;
import org.openrdf.query.algebra.evaluation.impl.DisjunctiveConstraintOptimizer;
import org.openrdf.query.algebra.evaluation.impl.EvaluationStatistics;
import org.openrdf.query.algebra.evaluation.impl.FilterOptimizer;
import org.openrdf.query.algebra.evaluation.impl.IterativeEvaluationOptimizer;
import org.openrdf.query.algebra.evaluation.impl.OrderLimitOptimizer;
import org.openrdf.query.algebra.evaluation.impl.QueryModelNormalizer;
import org.openrdf.query.algebra.evaluation.impl.SameTermFilterOptimizer;
import org.openrdf.query.impl.EmptyBindingSet;
import org.openrdf.sail.SailException;
import org.openrdf.sail.helpers.SailConnectionBase;

public class RdfCloudTripleStoreConnection extends SailConnectionBase {

    private RdfCloudTripleStore store;

    private RdfEvalStatsDAO rdfEvalStatsDAO;
    private SelectivityEvalDAO selectEvalDAO;
    private RyaDAO ryaDAO;
    private InferenceEngine inferenceEngine;
    private NamespaceManager namespaceManager;
    private RdfCloudTripleStoreConfiguration conf;
    

	private ProvenanceCollector provenanceCollector;

    public RdfCloudTripleStoreConnection(RdfCloudTripleStore sailBase, RdfCloudTripleStoreConfiguration conf, ValueFactory vf)
            throws SailException {
        super(sailBase);
        this.store = sailBase;
        this.conf = conf;
        initialize();
    }

    protected void initialize() throws SailException {
        refreshConnection();
    }

    protected void refreshConnection() throws SailException {
        try {
            checkNotNull(store.getRyaDAO());
            checkArgument(store.getRyaDAO().isInitialized());
            checkNotNull(store.getNamespaceManager());

            this.ryaDAO = store.getRyaDAO();
            this.rdfEvalStatsDAO = store.getRdfEvalStatsDAO();
            this.selectEvalDAO = store.getSelectEvalDAO();
            this.inferenceEngine = store.getInferenceEngine();
            this.namespaceManager = store.getNamespaceManager();
            this.provenanceCollector = store.getProvenanceCollector();

        } catch (Exception e) {
            throw new SailException(e);
        }
    }

    @Override
    protected void addStatementInternal(Resource subject, URI predicate,
                                        Value object, Resource... contexts) throws SailException {
        try {
            String cv_s = conf.getCv();
            byte[] cv = cv_s == null ? null : cv_s.getBytes();
            if (contexts != null && contexts.length > 0) {
                for (Resource context : contexts) {
                    RyaStatement statement = new RyaStatement(
                            RdfToRyaConversions.convertResource(subject),
                            RdfToRyaConversions.convertURI(predicate),
                            RdfToRyaConversions.convertValue(object),
                            RdfToRyaConversions.convertResource(context),
                            null, new StatementMetadata(), cv);

                    ryaDAO.add(statement);
                }
            } else {
                RyaStatement statement = new RyaStatement(
                        RdfToRyaConversions.convertResource(subject),
                        RdfToRyaConversions.convertURI(predicate),
                        RdfToRyaConversions.convertValue(object),
                        null, null, new StatementMetadata(), cv);

                ryaDAO.add(statement);
            }
        } catch (RyaDAOException e) {
            throw new SailException(e);
        }
    }

    
    
    
    @Override
    protected void clearInternal(Resource... aresource) throws SailException {
        try {
            RyaURI[] graphs = new RyaURI[aresource.length];
            for (int i = 0 ; i < graphs.length ; i++){
                graphs[i] = RdfToRyaConversions.convertResource(aresource[i]);
            }
            ryaDAO.dropGraph(conf, graphs);
        } catch (RyaDAOException e) {
            throw new SailException(e);
        }
    }

    @Override
    protected void clearNamespacesInternal() throws SailException {
        logger.error("Clear Namespace Repository method not implemented");
    }

    @Override
    protected void closeInternal() throws SailException {
        verifyIsOpen();
    }

    @Override
    protected void commitInternal() throws SailException {
        verifyIsOpen();
        //There is no transactional layer
    }

    @Override
    protected CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluateInternal(
            TupleExpr tupleExpr, Dataset dataset, BindingSet bindings,
            boolean flag) throws SailException {
        verifyIsOpen();
        logger.trace("Incoming query model:\n{}", tupleExpr.toString());
        if (provenanceCollector != null){
        	try {
				provenanceCollector.recordQuery(tupleExpr.toString());
			} catch (ProvenanceCollectionException e) {
				// TODO silent fail
				e.printStackTrace();
			}
        }
        tupleExpr = tupleExpr.clone();

        RdfCloudTripleStoreConfiguration queryConf = store.getConf().clone();
        if (bindings != null) {
            Binding dispPlan = bindings.getBinding(RdfCloudTripleStoreConfiguration.CONF_QUERYPLAN_FLAG);
            if (dispPlan != null) {
                queryConf.setDisplayQueryPlan(Boolean.parseBoolean(dispPlan.getValue().stringValue()));
            }

            Binding authBinding = bindings.getBinding(RdfCloudTripleStoreConfiguration.CONF_QUERY_AUTH);
            if (authBinding != null) {
                queryConf.setAuths(authBinding.getValue().stringValue().split(","));
            }

            Binding ttlBinding = bindings.getBinding(RdfCloudTripleStoreConfiguration.CONF_TTL);
            if (ttlBinding != null) {
                queryConf.setTtl(Long.valueOf(ttlBinding.getValue().stringValue()));
            }

            Binding startTimeBinding = bindings.getBinding(RdfCloudTripleStoreConfiguration.CONF_STARTTIME);
            if (startTimeBinding != null) {
                queryConf.setStartTime(Long.valueOf(startTimeBinding.getValue().stringValue()));
            }

            Binding performantBinding = bindings.getBinding(RdfCloudTripleStoreConfiguration.CONF_PERFORMANT);
            if (performantBinding != null) {
                queryConf.setBoolean(RdfCloudTripleStoreConfiguration.CONF_PERFORMANT, Boolean.parseBoolean(performantBinding.getValue().stringValue()));
            }

            Binding inferBinding = bindings.getBinding(RdfCloudTripleStoreConfiguration.CONF_INFER);
            if (inferBinding != null) {
                queryConf.setInfer(Boolean.parseBoolean(inferBinding.getValue().stringValue()));
            }

            Binding useStatsBinding = bindings.getBinding(RdfCloudTripleStoreConfiguration.CONF_USE_STATS);
            if (useStatsBinding != null) {
                queryConf.setUseStats(Boolean.parseBoolean(useStatsBinding.getValue().stringValue()));
            }

            Binding offsetBinding = bindings.getBinding(RdfCloudTripleStoreConfiguration.CONF_OFFSET);
            if (offsetBinding != null) {
                queryConf.setOffset(Long.parseLong(offsetBinding.getValue().stringValue()));
            }

            Binding limitBinding = bindings.getBinding(RdfCloudTripleStoreConfiguration.CONF_LIMIT);
            if (limitBinding != null) {
                queryConf.setLimit(Long.parseLong(limitBinding.getValue().stringValue()));
            }
        } else {
            bindings = new QueryBindingSet();
        }

        if (!(tupleExpr instanceof QueryRoot)) {
            tupleExpr = new QueryRoot(tupleExpr);
        }

        try {
            List<Class<QueryOptimizer>> optimizers = queryConf.getOptimizers();
            Class<QueryOptimizer> pcjOptimizer = queryConf.getPcjOptimizer();
            
            if(pcjOptimizer != null) {
                QueryOptimizer opt = null;
                try {
                    Constructor<QueryOptimizer> construct = pcjOptimizer.getDeclaredConstructor(new Class[] {});
                    opt = construct.newInstance();
                } catch (Exception e) {
                }
                if (opt == null) {
                    throw new NoSuchMethodException("Could not find valid constructor for " + pcjOptimizer.getName());
                }
                if (opt instanceof Configurable) {
                    ((Configurable) opt).setConf(conf);
                }
                opt.optimize(tupleExpr, dataset, bindings);
            }
            
            final ParallelEvaluationStrategyImpl strategy = new ParallelEvaluationStrategyImpl(
                    new StoreTripleSource(queryConf, ryaDAO), inferenceEngine, dataset, queryConf);
            
                (new BindingAssigner()).optimize(tupleExpr, dataset, bindings);
                (new ConstantOptimizer(strategy)).optimize(tupleExpr, dataset,
                        bindings);
                (new CompareOptimizer()).optimize(tupleExpr, dataset, bindings);
                (new ConjunctiveConstraintSplitter()).optimize(tupleExpr, dataset,
                        bindings);
                (new DisjunctiveConstraintOptimizer()).optimize(tupleExpr, dataset,
                        bindings);
                (new SameTermFilterOptimizer()).optimize(tupleExpr, dataset,
                        bindings);
                (new QueryModelNormalizer()).optimize(tupleExpr, dataset, bindings);
    
                (new IterativeEvaluationOptimizer()).optimize(tupleExpr, dataset,
                        bindings);

            if (!optimizers.isEmpty()) {
                for (Class<QueryOptimizer> optclz : optimizers) {
                    QueryOptimizer result = null;
                    try {
                        Constructor<QueryOptimizer> meth = optclz.getDeclaredConstructor(new Class[] {});
                        result = meth.newInstance();
                    } catch (Exception e) {
                    }
                    try {
                        Constructor<QueryOptimizer> meth = optclz.getDeclaredConstructor(EvaluationStrategy.class);
                        result = meth.newInstance(strategy);
                    } catch (Exception e) {
                    }
                    if (result == null) {
                        throw new NoSuchMethodException("Could not find valid constructor for " + optclz.getName());
                    }
                    if (result instanceof Configurable) {
                        ((Configurable) result).setConf(conf);
                    }
                    result.optimize(tupleExpr, dataset, bindings);
                }
            }

            (new FilterOptimizer()).optimize(tupleExpr, dataset, bindings);
            (new OrderLimitOptimizer()).optimize(tupleExpr, dataset, bindings);
            
            logger.trace("Optimized query model:\n{}", tupleExpr.toString());

            if (queryConf.isInfer()
                    && this.inferenceEngine != null
                    ) {
                try {
                    tupleExpr.visit(new PropertyChainVisitor(queryConf, inferenceEngine));
                    tupleExpr.visit(new TransitivePropertyVisitor(queryConf, inferenceEngine));
                    tupleExpr.visit(new SymmetricPropertyVisitor(queryConf, inferenceEngine));
                    tupleExpr.visit(new InverseOfVisitor(queryConf, inferenceEngine));
                    tupleExpr.visit(new SubPropertyOfVisitor(queryConf, inferenceEngine));
                    tupleExpr.visit(new SubClassOfVisitor(queryConf, inferenceEngine));
                    tupleExpr.visit(new SameAsVisitor(queryConf, inferenceEngine));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (queryConf.isPerformant()) {
                tupleExpr.visit(new SeparateFilterJoinsVisitor());
//                tupleExpr.visit(new FilterTimeIndexVisitor(queryConf));
//                tupleExpr.visit(new PartitionFilterTimeIndexVisitor(queryConf));
            }
            FilterRangeVisitor rangeVisitor = new FilterRangeVisitor(queryConf);
            tupleExpr.visit(rangeVisitor);
            tupleExpr.visit(rangeVisitor); //this has to be done twice to get replace the statementpatterns with the right ranges
            EvaluationStatistics stats = null;
            if (!queryConf.isUseStats() && queryConf.isPerformant() || rdfEvalStatsDAO == null) {
                stats = new DefaultStatistics();
            } else if (queryConf.isUseStats()) {

                if (queryConf.isUseSelectivity()) {
                    stats = new RdfCloudTripleStoreSelectivityEvaluationStatistics(queryConf, rdfEvalStatsDAO,
                            selectEvalDAO);
                } else {
                    stats = new RdfCloudTripleStoreEvaluationStatistics(queryConf, rdfEvalStatsDAO);
                }
            }
            if (stats != null) {

                if (stats instanceof RdfCloudTripleStoreSelectivityEvaluationStatistics) {

                    (new QueryJoinSelectOptimizer((RdfCloudTripleStoreSelectivityEvaluationStatistics) stats,
                            selectEvalDAO)).optimize(tupleExpr, dataset, bindings);
                } else {

                    (new org.apache.rya.rdftriplestore.evaluation.QueryJoinOptimizer(stats)).optimize(tupleExpr, dataset,
                            bindings); // TODO: Make pluggable
                }
            }

            final CloseableIteration<BindingSet, QueryEvaluationException> iter = strategy
                    .evaluate(tupleExpr, EmptyBindingSet.getInstance());
            CloseableIteration<BindingSet, QueryEvaluationException> iterWrap = new CloseableIteration<BindingSet, QueryEvaluationException>() {
                
                @Override
                public void remove() throws QueryEvaluationException {
                  iter.remove();
                }
                
                @Override
                public BindingSet next() throws QueryEvaluationException {
                    return iter.next();
                }
                
                @Override
                public boolean hasNext() throws QueryEvaluationException {
                    return iter.hasNext();
                }
                
                @Override
                public void close() throws QueryEvaluationException {
                    iter.close();
                    strategy.shutdown();
                }
            };
            return iterWrap;
        } catch (QueryEvaluationException e) {
            throw new SailException(e);
        } catch (Exception e) {
            throw new SailException(e);
        }
    }

    @Override
    protected CloseableIteration<? extends Resource, SailException> getContextIDsInternal()
            throws SailException {
        verifyIsOpen();

        // iterate through all contextids
        return null;
    }

    @Override
    protected String getNamespaceInternal(String s) throws SailException {
        return namespaceManager.getNamespace(s);
    }

    @Override
    protected CloseableIteration<? extends Namespace, SailException> getNamespacesInternal()
            throws SailException {
        return namespaceManager.iterateNamespace();
    }

    @Override
    protected CloseableIteration<? extends Statement, SailException> getStatementsInternal(
            Resource subject, URI predicate, Value object, boolean flag,
            Resource... contexts) throws SailException {
//        try {
        //have to do this to get the inferred values
        //TODO: Will this method reduce performance?
        final Var subjVar = decorateValue(subject, "s");
        final Var predVar = decorateValue(predicate, "p");
        final Var objVar = decorateValue(object, "o");
        StatementPattern sp = null;
        final boolean hasContext = contexts != null && contexts.length > 0;
        final Resource context = (hasContext) ? contexts[0] : null;
        final Var cntxtVar = decorateValue(context, "c");
        //TODO: Only using one context here
        sp = new StatementPattern(subjVar, predVar, objVar, cntxtVar);
        //return new StoreTripleSource(store.getConf()).getStatements(resource, uri, value, contexts);
        final CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluate = evaluate(sp, null, null, false);
        return new CloseableIteration<Statement, SailException>() {  //TODO: Use a util class to do this
            private boolean isClosed = false;

            @Override
            public void close() throws SailException {
            isClosed = true;
                try {
                    evaluate.close();
                } catch (QueryEvaluationException e) {
                    throw new SailException(e);
                }
            }

            @Override
            public boolean hasNext() throws SailException {
                try {
                    return evaluate.hasNext();
                } catch (QueryEvaluationException e) {
                    throw new SailException(e);
                }
            }

            @Override
            public Statement next() throws SailException {
                if (!hasNext() || isClosed) {
                    throw new NoSuchElementException();
                }

                try {
                    BindingSet next = evaluate.next();
                    Resource bs_subj = (Resource) ((subjVar.hasValue()) ? subjVar.getValue() : next.getBinding(subjVar.getName()).getValue());
                    URI bs_pred = (URI) ((predVar.hasValue()) ? predVar.getValue() : next.getBinding(predVar.getName()).getValue());
                    Value bs_obj = (objVar.hasValue()) ? objVar.getValue() : (Value) next.getBinding(objVar.getName()).getValue();
                    Binding b_cntxt = next.getBinding(cntxtVar.getName());

                    //convert BindingSet to Statement
                    if (b_cntxt != null) {
                        return new ContextStatementImpl(bs_subj, bs_pred, bs_obj, (Resource) b_cntxt.getValue());
                    } else {
                        return new StatementImpl(bs_subj, bs_pred, bs_obj);
                    }
                } catch (QueryEvaluationException e) {
                    throw new SailException(e);
                }
            }

            @Override
            public void remove() throws SailException {
                try {
                    evaluate.remove();
                } catch (QueryEvaluationException e) {
                    throw new SailException(e);
                }
            }
        };
//        } catch (QueryEvaluationException e) {
//            throw new SailException(e);
//        }
    }

    protected Var decorateValue(Value val, String name) {
        if (val == null) {
            return new Var(name);
        } else {
            return new Var(name, val);
        }
    }

    @Override
    protected void removeNamespaceInternal(String s) throws SailException {
        namespaceManager.removeNamespace(s);
    }

    @Override
    protected void removeStatementsInternal(Resource subject, URI predicate,
                                            Value object, Resource... contexts) throws SailException {
        if (!(subject instanceof URI)) {
            throw new SailException("Subject[" + subject + "] must be URI");
        }

        try {
            if (contexts != null && contexts.length > 0) {
                for (Resource context : contexts) {
                    if (!(context instanceof URI)) {
                        throw new SailException("Context[" + context + "] must be URI");
                    }
                    RyaStatement statement = new RyaStatement(
                            RdfToRyaConversions.convertResource(subject),
                            RdfToRyaConversions.convertURI(predicate),
                            RdfToRyaConversions.convertValue(object),
                            RdfToRyaConversions.convertResource(context));

                    ryaDAO.delete(statement, conf);
                }
            } else {
                RyaStatement statement = new RyaStatement(
                        RdfToRyaConversions.convertResource(subject),
                        RdfToRyaConversions.convertURI(predicate),
                        RdfToRyaConversions.convertValue(object),
                        null);

                ryaDAO.delete(statement, conf);
            }
        } catch (RyaDAOException e) {
            throw new SailException(e);
        }
    }

    @Override
    protected void rollbackInternal() throws SailException {
        //TODO: No transactional layer as of yet
    }

    @Override
    protected void setNamespaceInternal(String s, String s1)
            throws SailException {
        namespaceManager.addNamespace(s, s1);
    }

    @Override
    protected long sizeInternal(Resource... contexts) throws SailException {
        logger.error("Cannot determine size as of yet");

        return 0;
    }

    @Override
    protected void startTransactionInternal() throws SailException {
        //TODO: ?
    }

    public static class StoreTripleSource implements TripleSource {

        private RdfCloudTripleStoreConfiguration conf;
        private RyaDAO<?> ryaDAO;

        public StoreTripleSource(RdfCloudTripleStoreConfiguration conf, RyaDAO<?> ryaDAO) {
            this.conf = conf;
            this.ryaDAO = ryaDAO;
        }

        public CloseableIteration<Statement, QueryEvaluationException> getStatements(
                Resource subject, URI predicate, Value object,
                Resource... contexts) throws QueryEvaluationException {
            return RyaDAOHelper.query(ryaDAO, subject, predicate, object, conf, contexts);
        }

        public CloseableIteration<? extends Entry<Statement, BindingSet>, QueryEvaluationException> getStatements(
                Collection<Map.Entry<Statement, BindingSet>> statements,
                Resource... contexts) throws QueryEvaluationException {

            return RyaDAOHelper.query(ryaDAO, statements, conf);
        }

        public ValueFactory getValueFactory() {
            return RdfCloudTripleStoreConstants.VALUE_FACTORY;
        }
    }
    
    public InferenceEngine getInferenceEngine() {
        return inferenceEngine;
    }
    public RdfCloudTripleStoreConfiguration getConf() {
        return conf;
    }
}
