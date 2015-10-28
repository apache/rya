package mvm.rya.indexing.external;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import mvm.rya.api.RdfCloudTripleStoreConfiguration;
import mvm.rya.indexing.IndexPlanValidator.IndexPlanValidator;
import mvm.rya.indexing.IndexPlanValidator.IndexedExecutionPlanGenerator;
import mvm.rya.indexing.IndexPlanValidator.ThreshholdPlanSelector;
import mvm.rya.indexing.IndexPlanValidator.TupleReArranger;
import mvm.rya.indexing.IndexPlanValidator.ValidIndexCombinationGenerator;
import mvm.rya.indexing.accumulo.ConfigUtils;
import mvm.rya.indexing.external.QueryVariableNormalizer.VarCollector;
import mvm.rya.indexing.external.tupleSet.AccumuloIndexSet;
import mvm.rya.indexing.external.tupleSet.ExternalTupleSet;
import mvm.rya.rdftriplestore.inference.DoNotExpandSP;
import mvm.rya.rdftriplestore.utils.FixedStatementPattern;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.BindingSetAssignment;
import org.openrdf.query.algebra.Difference;
import org.openrdf.query.algebra.Distinct;
import org.openrdf.query.algebra.EmptySet;
import org.openrdf.query.algebra.Extension;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.Intersection;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.LeftJoin;
import org.openrdf.query.algebra.Order;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.QueryRoot;
import org.openrdf.query.algebra.Reduced;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.UnaryTupleOperator;
import org.openrdf.query.algebra.Union;
import org.openrdf.query.algebra.ValueExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.evaluation.QueryOptimizer;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.algebra.helpers.VarNameCollector;
import org.openrdf.sail.SailException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

//optimizer which matches TupleExpressions associated with pre-computed queries
//to sub-queries of a given query. Each matched sub-query is replaced by an indexing node
//to delegate that portion of the query to the pre-computed query index
public class PrecompJoinOptimizer implements QueryOptimizer, Configurable {

    private List<ExternalTupleSet> indexSet;
    private Configuration conf;
    private boolean init = false;
    
    public PrecompJoinOptimizer() {
    }
    
    public PrecompJoinOptimizer(Configuration conf) {
        this.conf = conf;
        try {
            indexSet = getAccIndices(conf);
            init = true;
        } catch (MalformedQueryException e) {
            e.printStackTrace();
        } catch (SailException e) {
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            e.printStackTrace();
        } catch (TableNotFoundException e) {
            e.printStackTrace();
        } catch (AccumuloException e) {
            e.printStackTrace();
        } catch (AccumuloSecurityException e) {
            e.printStackTrace();
        }
    }
    
    public PrecompJoinOptimizer(List<ExternalTupleSet> indices, boolean useOptimalPcj) {
        this.indexSet = indices;
        conf = new Configuration();
        conf.setBoolean(ConfigUtils.USE_OPTIMAL_PCJ, useOptimalPcj);
    }
    
    public void setConf(Configuration conf) {
        this.conf = conf;
        if (!init) {
            try {
                indexSet = getAccIndices(conf);
                init = true;
            } catch (MalformedQueryException e) {
                e.printStackTrace();
            } catch (SailException e) {
                e.printStackTrace();
            } catch (QueryEvaluationException e) {
                e.printStackTrace();
            } catch (TableNotFoundException e) {
                e.printStackTrace();
            } catch (AccumuloException e) {
                e.printStackTrace();
            } catch (AccumuloSecurityException e) {
                e.printStackTrace();
            }
        }
    }
    
    @Override
    public Configuration getConf() {
        return conf;
    }
    

    @Override
    public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {

        IndexedExecutionPlanGenerator iep = new IndexedExecutionPlanGenerator(tupleExpr, indexSet);
        JoinVisitor jv = new JoinVisitor();
        
        if (ConfigUtils.getUseOptimalPCJ(conf) && indexSet.size() > 0) {
            
            //get potential relevant index combinations
            ValidIndexCombinationGenerator vic = new ValidIndexCombinationGenerator(tupleExpr);
            Iterator<List<ExternalTupleSet>> iter = vic.getValidIndexCombos(iep.getNormalizedIndices());
            TupleExpr bestTup = null;
            TupleExpr tempTup = null;
            double tempCost = 0;
            double minCost = Double.MAX_VALUE;
            
            while (iter.hasNext()) {
                //apply join visitor to place external index nodes in query
                TupleExpr clone = tupleExpr.clone();
                jv.setExternalTupList(iter.next());
                jv.setSegmentFilters(new ArrayList<Filter>());
                clone.visit(jv);
                
                //get all valid execution plans for given external index combination by considering all 
                //permutations of nodes in TupleExpr
                IndexPlanValidator ipv = new IndexPlanValidator(false);
                Iterator<TupleExpr> validTups = ipv.getValidTuples(TupleReArranger.getTupleReOrderings(clone).iterator());
                
                //set valid plan according to a specified cost threshold, where cost depends on specified weights
                //for number of external index nodes, common variables among joins in execution plan, and number of
                //external products in execution plan
                ThreshholdPlanSelector tps = new ThreshholdPlanSelector(tupleExpr);
                tempTup = tps.getThreshholdQueryPlan(validTups, .4, .5, .2, .3);
                
                //choose best threshhold TupleExpr among all index node combinations
                tempCost = tps.getCost(tempTup, .5, .2, .3);
                if(tempCost < minCost ) {
                    minCost = tempCost;
                    bestTup = tempTup;
                }    
            }
            if (bestTup != null) {
                ((UnaryTupleOperator) tupleExpr).setArg(((UnaryTupleOperator) bestTup).getArg());
            }
            return;
        } else {
            if (indexSet.size() > 0) {
                jv.setExternalTupList(iep.getNormalizedIndices());
                tupleExpr.visit(jv);
            }
            return;
        }
    }

    protected class JoinVisitor extends QueryModelVisitorBase<RuntimeException> {

        private List<ExternalTupleSet> tupList;
        private List<Filter> segmentFilters = Lists.newArrayList();
        
        public void setExternalTupList(List<ExternalTupleSet> tupList) {
            this.tupList = tupList;
        }
        
        public void setSegmentFilters(List<Filter> segmentFilters) {
            this.segmentFilters = segmentFilters;
        }
        
        @Override
        public void meet(Join node) {

            //get all filters with bindings in this segment
            updateFilters(segmentFilters, true);
            
            try {
                if (node.getLeftArg() instanceof FixedStatementPattern && node.getRightArg() instanceof DoNotExpandSP) {
                    return;
                }

                //get nodes in this join segment
                TupleExpr newJoin = null;
                List<QueryModelNode> args = getJoinArgs(node, new ArrayList<QueryModelNode>(), false);
                List<TupleExpr> joinArgs = Lists.newArrayList();
                
                for (QueryModelNode qNode : args) {
                    assert (qNode instanceof TupleExpr);
                    joinArgs.add((TupleExpr) qNode);
                }
                
                //insert all matching ExternalTupleSets in tupList into this segment
                joinArgs = matchExternalTupleSets(joinArgs, tupList);

                //push down any filters that have bindings in lower segments
                //and update the filters in this segment
                updateFilters(segmentFilters, false);
                
                //form join from matching ExternalTupleSets, remaining nodes, and filters
                //that can't be pushed down any further
                newJoin = getNewJoin(joinArgs, getFilterChain(segmentFilters));

                // Replace old join hierarchy
                node.replaceWith(newJoin);

                //visit remaining nodes to match ExternalTupleSets with nodes further down
                for (TupleExpr te : joinArgs) {
                    if (!(te instanceof StatementPattern) && !(te instanceof ExternalTupleSet)) {
                        segmentFilters = Lists.newArrayList();
                        te.visit(this);
                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        
        
        @Override
        public void meet(Filter node) {
            segmentFilters.add(node);
            node.getArg().visit(this);
        }

        //chain filters together and return front and back of chain
        private List<TupleExpr> getFilterChain(List<Filter> filters) {
            List<TupleExpr> filterTopBottom = Lists.newArrayList();
            Filter filterChainTop = null;
            Filter filterChainBottom = null;
            
            for (Filter filter: filters) {
                if (filterChainTop == null) {
                    filterChainTop = filter;
                } else if (filterChainBottom == null) {
                    filterChainBottom = filter;
                    filterChainTop.setArg(filterChainBottom);
                } else {
                    filterChainBottom.setArg(filter);
                    filterChainBottom = filter;
                }
            }
            if(filterChainTop != null) {
                filterTopBottom.add(filterChainTop);
            }
            if(filterChainBottom != null) {
                filterTopBottom.add(filterChainBottom);
            }
            return filterTopBottom;
        }
        
        //build newJoin node given remaining joinArgs and chain of filters
        private TupleExpr getNewJoin(List<TupleExpr> args, List<TupleExpr> filterChain) {
            TupleExpr newJoin;
            List<TupleExpr> joinArgs = Lists.newArrayList(args);

            if (joinArgs.size() > 1) {
                if (filterChain.size() > 0) {
                    TupleExpr finalJoinArg = joinArgs.remove(0);
                    TupleExpr tempJoin;
                    TupleExpr temp = filterChain.get(0);

                    if (joinArgs.size() > 1) {
                        tempJoin = new Join(joinArgs.remove(0), joinArgs.remove(0));
                        for (TupleExpr te : joinArgs) {
                            tempJoin = new Join(tempJoin, te);
                        }
                    } else {
                        tempJoin = joinArgs.remove(0);
                    }

                    if (filterChain.size() == 1) {
                        ((Filter) temp).setArg(tempJoin);
                    } else {
                        ((Filter) filterChain.get(1)).setArg(tempJoin);
                    }
                    newJoin = new Join(temp, finalJoinArg);
                } else {
                    newJoin = new Join(joinArgs.get(0), joinArgs.get(1));
                    joinArgs.remove(0);
                    joinArgs.remove(0);

                    for (TupleExpr te : joinArgs) {
                        newJoin = new Join(newJoin, te);
                    }
                }
            } else if (joinArgs.size() == 1) {
                if (filterChain.size() > 0) {
                    newJoin = filterChain.get(0);
                    if (filterChain.size() == 1) {
                        ((Filter) newJoin).setArg(joinArgs.get(0));
                    } else {
                        ((Filter) filterChain.get(1)).setArg(joinArgs.get(0));
                    }
                } else {
                    newJoin = joinArgs.get(0);
                }
            } else {
                throw new IllegalStateException("JoinArgs size cannot be zero.");
            }
            return newJoin;
        }

      
       private List<TupleExpr> matchExternalTupleSets(List<TupleExpr> joinArgs, List<ExternalTupleSet> tupList) {
           
           Set<QueryModelNode> argSet = Sets.newHashSet();
           argSet.addAll(joinArgs);
           
           if(argSet.size() < joinArgs.size()) {
               throw new IllegalArgumentException("Query has duplicate nodes in segment!");
           }
           
           Set<QueryModelNode> firstJoinFilterCond = Sets.newHashSet();
           
           for(Filter filter: segmentFilters) {
               firstJoinFilterCond.add(filter.getCondition());
           }
           
           argSet.addAll(firstJoinFilterCond);
             
           //see if ExternalTupleSet nodes are a subset of joinArgs, and if so, replacing matching nodes
           //with ExternalTupleSet
            for (ExternalTupleSet tup : tupList) {
                TupleExpr tupleArg = tup.getTupleExpr();
                if (isTupleValid(tupleArg)) {
                    List<QueryModelNode> tupJoinArgs = getJoinArgs(tupleArg, 
                            new ArrayList<QueryModelNode>(), true);
                    Set<QueryModelNode> tupJoinArgSet = Sets.newHashSet(tupJoinArgs);
                    if(tupJoinArgSet.size() < tupJoinArgs.size()) {
                        throw new IllegalArgumentException("ExternalTuple contains duplicate nodes!");
                    }
                    if (argSet.containsAll(tupJoinArgSet)) {
                        argSet = Sets.newHashSet(Sets.difference(argSet, tupJoinArgSet));
                        argSet.add((ExternalTupleSet) tup.clone());
                    }
                }
            }
          
            //update segment filters by removing those use in ExternalTupleSet
            Iterator<Filter> iter = segmentFilters.iterator();
            
            while(iter.hasNext()) {
                Filter filt = iter.next();
                if(!argSet.contains(filt.getCondition())) {
                    filt.replaceWith(filt.getArg());
                    iter.remove();
                }    
            }
            
            //update joinArgs
            joinArgs = Lists.newArrayList();
            for(QueryModelNode node: argSet) {
                if(!(node instanceof ValueExpr)) {
                    joinArgs.add((TupleExpr)node);
                }
            }
           
           return joinArgs;
       }

       
        private void updateFilters(List<Filter> filters, boolean firstJoin) {

            Iterator<Filter> iter = segmentFilters.iterator();

            while (iter.hasNext()) {
                if (!FilterRelocator.relocate(iter.next(), firstJoin)) {
                    iter.remove();
                }
            }
        }
       
        protected List<QueryModelNode> getJoinArgs(TupleExpr tupleExpr, List<QueryModelNode> joinArgs, boolean getFilters) {
            if (tupleExpr instanceof Join) {
                if (!(((Join) tupleExpr).getLeftArg() instanceof FixedStatementPattern)
                        && !(((Join) tupleExpr).getRightArg() instanceof DoNotExpandSP)) {
                    Join join = (Join) tupleExpr;
                    getJoinArgs(join.getLeftArg(), joinArgs, getFilters);
                    getJoinArgs(join.getRightArg(), joinArgs, getFilters);
                } 
            } else if(tupleExpr instanceof Filter) {
                if (getFilters) {
                    joinArgs.add(((Filter) tupleExpr).getCondition());
                }
                getJoinArgs(((Filter)tupleExpr).getArg(), joinArgs, getFilters);
            } else if(tupleExpr instanceof Projection) {
                getJoinArgs(((Projection)tupleExpr).getArg(), joinArgs, getFilters);
            } else {
                joinArgs.add(tupleExpr);
            }

            return joinArgs;
        }
    }
    
    protected static class FilterRelocator extends QueryModelVisitorBase<RuntimeException> {

   
        protected final Filter filter;

        protected final Set<String> filterVars;
        private boolean stopAtFirstJoin = false;
        private boolean isFirstJoinFilter = false;
        private boolean inSegment = true;
        
        
        public FilterRelocator(Filter filter) {
            this.filter = filter;
            filterVars = VarNameCollector.process(filter.getCondition());
        }
        
        public FilterRelocator(Filter filter, boolean stopAtFirstJoin) {
            this.filter = filter;
            filterVars = VarNameCollector.process(filter.getCondition());
            this.stopAtFirstJoin = stopAtFirstJoin;
        }
        
        public static boolean relocate(Filter filter) {
            FilterRelocator fr = new FilterRelocator(filter);
            filter.visit(fr);
            return fr.inSegment;
        }
        
        public static boolean relocate(Filter filter, boolean stopAtFirstJoin) {
            if (stopAtFirstJoin) {
                FilterRelocator fr = new FilterRelocator(filter, stopAtFirstJoin);
                filter.visit(fr);
                return fr.isFirstJoinFilter;
            } else {
                FilterRelocator fr = new FilterRelocator(filter);
                filter.visit(fr);
                return fr.inSegment;
            }
        }

     
        @Override
        protected void meetNode(QueryModelNode node) {
            // By default, do not traverse
            assert node instanceof TupleExpr;
            
            if(node instanceof UnaryTupleOperator) {
                if (((UnaryTupleOperator)node).getArg().getBindingNames().containsAll(filterVars)) {
                    if (stopAtFirstJoin) {
                        ((UnaryTupleOperator) node).getArg().visit(this);
                    } else {
                        inSegment = false;
                        relocate(filter, ((UnaryTupleOperator) node).getArg());
                    }
                }
            }
            
            relocate(filter, (TupleExpr) node);
        }
       

        @Override
        public void meet(Join join) {

            if (stopAtFirstJoin) {
                isFirstJoinFilter = true;
                relocate(filter, join);
            } else {

                if (join.getLeftArg().getBindingNames().containsAll(filterVars)) {
                    // All required vars are bound by the left expr
                    join.getLeftArg().visit(this);
                } else if (join.getRightArg().getBindingNames().containsAll(filterVars)) {
                    // All required vars are bound by the right expr
                    join.getRightArg().visit(this);
                } else {
                    relocate(filter, join);
                }
            }
        }

        @Override
        public void meet(LeftJoin leftJoin) {
            
            if (leftJoin.getLeftArg().getBindingNames().containsAll(filterVars)) {
                inSegment = false;
                if (stopAtFirstJoin) {
                    leftJoin.getLeftArg().visit(this);
                } else {
                    relocate(filter, leftJoin.getLeftArg());
                }
            }
            else {
                relocate(filter, leftJoin);
            }
        }

        @Override
        public void meet(Union union) {
            Filter clone = new Filter();
            clone.setCondition(filter.getCondition().clone());

            relocate(filter, union.getLeftArg());
            relocate(clone, union.getRightArg());
            
            inSegment = false;

        }

        @Override
        public void meet(Difference node) {
            Filter clone = new Filter();
            clone.setCondition(filter.getCondition().clone());
        
            relocate(filter, node.getLeftArg());
            relocate(clone, node.getRightArg());
            
            inSegment = false;
        
        }

        @Override
        public void meet(Intersection node) {
            Filter clone = new Filter();
            clone.setCondition(filter.getCondition().clone());
        
            relocate(filter, node.getLeftArg());
            relocate(clone, node.getRightArg());
            
            inSegment = false;
        
        }

        @Override
        public void meet(Extension node) {
            if (node.getArg().getBindingNames().containsAll(filterVars)) {
                if (stopAtFirstJoin) {
                    node.getArg().visit(this);
                } else {
                    relocate(filter, node.getArg());
                    inSegment = false;
                }
            }
            else {
                relocate(filter, node);
            }
        }

        @Override
        public void meet(EmptySet node) {
            if (filter.getParentNode() != null) {
                // Remove filter from its original location
                filter.replaceWith(filter.getArg());
            }
        }

        @Override
        public void meet(Filter filter) {
            // Filters are commutative
            filter.getArg().visit(this);
        }

        @Override
        public void meet(Distinct node) {
            node.getArg().visit(this);
        }

        @Override
        public void meet(Order node) {
            node.getArg().visit(this);
        }

        @Override
        public void meet(QueryRoot node) {
            node.getArg().visit(this);
        }

        @Override
        public void meet(Reduced node) {
            node.getArg().visit(this);
        }

        protected void relocate(Filter filter, TupleExpr newFilterArg) {
            if (filter.getArg() != newFilterArg) {
                if (filter.getParentNode() != null) {
                    // Remove filter from its original location
                    filter.replaceWith(filter.getArg());
                }

                // Insert filter at the new location
                newFilterArg.replaceWith(filter);
                filter.setArg(newFilterArg);
            }
        }
    }
    
    
    private static boolean isTupleValid(QueryModelNode node) {

        ValidQueryVisitor vqv = new ValidQueryVisitor();
        node.visit(vqv);

        if (vqv.isValid() && vqv.getSPs().size() > 1) {   
            if(vqv.getFilters().size() > 0) {
                Set<String> spVars = getVarNames(vqv.getSPs());
                Set<String> fVarNames = getVarNames(vqv.getFilters());
                //check that all vars contained in filters also occur in SPs
                return Sets.intersection(fVarNames,spVars).equals(fVarNames);
            } else {
                return true;
            }
        } else {
            return false;
        }
    }
    
    
    private static Set<String> getVarNames(Collection<QueryModelNode> nodes) {

        List<String> tempVars;
        Set<String> nodeVarNames = Sets.newHashSet();

        for (QueryModelNode s : nodes) {
            tempVars = VarCollector.process(s);
            for (String t : tempVars)
                nodeVarNames.add(t);
        }
        return nodeVarNames;
    }
    
    
    private static class ValidQueryVisitor extends QueryModelVisitorBase<RuntimeException> {

        private boolean isValid = true;
        private Set<QueryModelNode> filterSet = Sets.newHashSet();
        private Set<QueryModelNode> spSet = Sets.newHashSet();
        
        public Set<QueryModelNode> getFilters() {
            return filterSet;
        }
        
        public Set<QueryModelNode> getSPs() {
            return spSet;
        }

        public boolean isValid() {
            return isValid;
        }

        public void meet(Projection node) {
            node.getArg().visit(this);
        }

        @Override
        public void meet(Filter node) {
            filterSet.add(node.getCondition());
            node.getArg().visit(this);
        }
        
        @Override
        public void meet(StatementPattern node) {
            spSet.add(node);
        }
     
        public void meetNode(QueryModelNode node) {

            if (!((node instanceof Join) || (node instanceof StatementPattern) || (node instanceof BindingSetAssignment) || 
                    (node instanceof Var) || (node instanceof Union) || (node instanceof LeftJoin))) {
                isValid = false;
                return;
           
            } else{
                super.meetNode(node);
            }
        }

    }
    
    
    private static List<ExternalTupleSet> getAccIndices(Configuration conf) throws MalformedQueryException,
            SailException, QueryEvaluationException, TableNotFoundException, AccumuloException,
            AccumuloSecurityException {

        List<String> tables = null;

        if (conf instanceof RdfCloudTripleStoreConfiguration) {
            tables = ((RdfCloudTripleStoreConfiguration) conf).getPcjTables();
        }

        String tablePrefix = conf.get(RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX);
        Connector c = ConfigUtils.getConnector(conf);
        Map<String, String> indexTables = Maps.newLinkedHashMap();

        if (tables != null && !tables.isEmpty()) {
            for (String table : tables) {
                Scanner s = c.createScanner(table, new Authorizations());
                s.setRange(Range.exact(new Text("~SPARQL")));
                for (Entry<Key, Value> e : s) {
                    indexTables.put(table, e.getValue().toString());
                }
            }
        } else {
            for (String table : c.tableOperations().list()) {
                if (table.startsWith(tablePrefix + "INDEX")) {
                    Scanner s = c.createScanner(table, new Authorizations());
                    s.setRange(Range.exact(new Text("~SPARQL")));
                    for (Entry<Key, Value> e : s) {
                        indexTables.put(table, e.getValue().toString());
                    }
                }
            }

        }
        List<ExternalTupleSet> index = Lists.newArrayList();

        if (indexTables.isEmpty()) {
            System.out.println("No Index found");
        } else {
            for (String table : indexTables.keySet()) {
                String indexSparqlString = indexTables.get(table);
                index.add(new AccumuloIndexSet(indexSparqlString, c, table));
            }
        }
        return index;
    }
}
