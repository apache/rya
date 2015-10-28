package mvm.rya.indexing.IndexPlanValidator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import mvm.rya.indexing.external.tupleSet.ExternalTupleSet;
import mvm.rya.rdftriplestore.inference.DoNotExpandSP;
import mvm.rya.rdftriplestore.utils.FixedStatementPattern;

import org.openrdf.query.algebra.BindingSetAssignment;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

import com.beust.jcommander.internal.Lists;
import com.google.common.collect.Collections2;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class TupleExecutionPlanGenerator implements IndexTupleGenerator {

    
    
    @Override
    public Iterator<TupleExpr> getPlans(Iterator<TupleExpr> indexPlans) {

        final Iterator<TupleExpr> iter = indexPlans;
        
        return new Iterator<TupleExpr>() {

            private TupleExpr next = null;
            private boolean hasNextCalled = false;
            private boolean isEmpty = false;
            Iterator<TupleExpr> tuples = null;

            @Override
            public boolean hasNext() {

                if (!hasNextCalled && !isEmpty) {
                    if (tuples != null && tuples.hasNext()) {
                        next = tuples.next();
                        hasNextCalled = true;
                        return true;
                    } else {
                        while (iter.hasNext()) {
                            tuples = getPlans(iter.next()).iterator();
                            if (tuples == null) {
                                throw new IllegalStateException("Plans cannot be null!");
                            }
                            next = tuples.next();
                            hasNextCalled = true;
                            return true;
                        }
                        isEmpty = true;
                        return false;
                    }
                } else if (isEmpty) {
                    return false;
                } else {
                    return true;
                }
            }

            @Override
            public TupleExpr next() {

                if (hasNextCalled) {
                    hasNextCalled = false;
                    return next;
                } else if(isEmpty) {
                    throw new NoSuchElementException();
                }else {
                    if (this.hasNext()) {
                        hasNextCalled = false;
                        return next;
                    } else {
                        throw new NoSuchElementException();
                    }

                }

            }
            
            @Override
            public void remove() {
                throw new UnsupportedOperationException("Cannot delete from iterator!");
            }

        };
              
    }

    private List<TupleExpr> getPlans(TupleExpr te) {

        
        NodeCollector nc = new NodeCollector();
        te.visit(nc);

        Set<QueryModelNode> nodeSet = nc.getNodeSet();
        List<Filter> filterList = nc.getFilterSet();
        Projection projection = nc.getProjection().clone();
  
        List<TupleExpr> queryPlans = Lists.newArrayList();

        Collection<List<QueryModelNode>> plans = Collections2.permutations(nodeSet);

        for (List<QueryModelNode> p : plans) {
            if (p.size() == 0) {
                throw new IllegalArgumentException("Tuple must contain at least one node!");
            } else if (p.size() == 1) {
                queryPlans.add(te);
            } else {
                queryPlans.add(buildTuple(p, filterList, projection));
            }
        }

        return queryPlans;
    }

    private TupleExpr buildTuple(List<QueryModelNode> nodes, List<Filter> filters, Projection projection) {

        Projection proj = (Projection)projection.clone();
        Join join = null;

        join = new Join((TupleExpr) nodes.get(0).clone(), (TupleExpr) nodes.get(1).clone());

        for (int i = 2; i < nodes.size(); i++) {
            join = new Join(join, (TupleExpr) nodes.get(i).clone());
        }

        if (filters.size() == 0) {
            proj.setArg(join);
            return proj;
        } else {
            TupleExpr queryPlan = join;
            for (Filter f : filters) {
                Filter filt = (Filter) f.clone();
                filt.setArg(queryPlan);
                queryPlan = filt;
            }
            proj.setArg(queryPlan);
            return proj;
        }

    }

    public static class NodeCollector extends QueryModelVisitorBase<RuntimeException> {

        private Set<QueryModelNode> nodeSet = Sets.newHashSet();
        private List<Filter> filterSet = Lists.newArrayList();
        private Projection projection;

        public Projection getProjection() {
            return projection;
        }

        public Set<QueryModelNode> getNodeSet() {
            return nodeSet;
        }

        public List<Filter> getFilterSet() {
            return filterSet;
        }

        @Override
        public void meet(Projection node) {
            projection = node;
            node.getArg().visit(this);
        }

        @Override
        public void meetNode(QueryModelNode node) throws RuntimeException {
            if (node instanceof ExternalTupleSet || node instanceof BindingSetAssignment
                    || node instanceof StatementPattern) {
                nodeSet.add(node);
            }
            super.meetNode(node);
        }

        @Override
        public void meet(Filter node) {
            filterSet.add(node);
            node.getArg().visit(this);
        }

    }
    
    
    

}
