package org.apache.rya.indexing.IndexPlanValidator;

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





import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.rya.indexing.external.tupleSet.ExternalTupleSet;
import org.apache.rya.indexing.pcj.matching.QueryVariableNormalizer.VarCollector;

import org.openrdf.query.algebra.BindingSetAssignment;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Processes a {@link TupleExpr} and replaces sets of elements in the tree with {@link ExternalTupleSet} objects.
 */
public class GeneralizedExternalProcessor {


    /**
     * Iterates through list of normalized indexes and replaces all subtrees of query which match index with index.
     *
     * @param query
     * @return TupleExpr
     */
    public static TupleExpr process(TupleExpr query, List<ExternalTupleSet> indexSet) {

        boolean indexPlaced = false;
        TupleExpr rtn = query.clone();
        QueryNodeCount qnc = new QueryNodeCount();
        rtn.visit(qnc);

        if(qnc.getNodeCount()/2 < indexSet.size()) {
            return null;
        }


        //move BindingSetAssignment Nodes out of the way
        organizeBSAs(rtn);


        // test to see if query contains no other nodes
        // than filter, join, projection, and statement pattern and
        // test whether query contains duplicate StatementPatterns and filters
        if (isTupleValid(rtn)) {

            for (ExternalTupleSet index : indexSet) {

                // test to see if index contains at least one StatementPattern,
                // that StatementPatterns are unique,
                // and that all variables found in filters occur in some
                // StatementPattern
                if (isTupleValid(index.getTupleExpr())) {

                    ExternalTupleSet eTup = (ExternalTupleSet) index.clone();
                    SPBubbleDownVisitor indexVistor = new SPBubbleDownVisitor(eTup);
                    rtn.visit(indexVistor);
                    FilterBubbleManager fbmv = new FilterBubbleManager(eTup);
                    rtn.visit(fbmv);
                    SubsetEqualsVisitor subIndexVis = new SubsetEqualsVisitor(eTup, rtn);
                    rtn.visit(subIndexVis);
                    indexPlaced = subIndexVis.indexPlaced();
                    if(!indexPlaced) {
                        break;
                    }

                }

            }
            if(indexPlaced) {
                return rtn;
            } else {
                return null;
            }

        } else {
            throw new IllegalArgumentException("Invalid Query.");
        }
    }





    // determines whether query is valid, which requires that a
    // query must contain a StatementPattern, not contain duplicate
    // Statement Patterns or Filters, not be comprised of only Projection,
    // Join, StatementPattern, and Filter nodes, and that any variable
    // appearing in a Filter must appear in a StatementPattern.
    private static boolean isTupleValid(QueryModelNode node) {

        ValidQueryVisitor vqv = new ValidQueryVisitor();
        node.visit(vqv);

        Set<String> spVars = getVarNames(getQNodes("sp", node));

        if (vqv.isValid() && spVars.size() > 0) {

            FilterCollector fvis = new FilterCollector();
            node.visit(fvis);
            List<QueryModelNode> fList = fvis.getFilters();
            return fList.size() == Sets.newHashSet(fList).size() && getVarNames(fList).size() <= spVars.size();

        } else {
            return false;
        }
    }

    private static Set<QueryModelNode> getQNodes(QueryModelNode queryNode) {
        Set<QueryModelNode> rtns = new HashSet<QueryModelNode>();

        StatementPatternCollector spc = new StatementPatternCollector();
        queryNode.visit(spc);
        rtns.addAll(spc.getStatementPatterns());

        FilterCollector fvis = new FilterCollector();
        queryNode.visit(fvis);
        rtns.addAll(fvis.getFilters());

        ExternalTupleCollector eVis = new ExternalTupleCollector();
        queryNode.visit(eVis);
        rtns.addAll(eVis.getExtTup());

        return rtns;
    }

    private static Set<QueryModelNode> getQNodes(String node, QueryModelNode queryNode) {

        if (node.equals("sp")) {
            Set<QueryModelNode> eSet = new HashSet<QueryModelNode>();
            StatementPatternCollector spc = new StatementPatternCollector();
            queryNode.visit(spc);
            List<StatementPattern> spList = spc.getStatementPatterns();
            eSet.addAll(spList);
            // returns empty set if list contains duplicate StatementPatterns
            if (spList.size() > eSet.size()) {
                return Sets.newHashSet();
            } else {
                return eSet;
            }
        } else if (node.equals("filter")) {

            FilterCollector fvis = new FilterCollector();
            queryNode.visit(fvis);

            return Sets.newHashSet(fvis.getFilters());
        } else {

            throw new IllegalArgumentException("Invalid node type.");
        }
    }

    // moves StatementPatterns in query that also occur in index to bottom of
    // query tree.
    private static class SPBubbleDownVisitor extends QueryModelVisitorBase<RuntimeException> {

        private TupleExpr tuple;
        private QueryModelNode indexQNode;
        private Set<QueryModelNode> sSet = Sets.newHashSet();

        public SPBubbleDownVisitor(ExternalTupleSet index) {

            this.tuple = index.getTupleExpr();
            indexQNode = ((Projection) tuple).getArg();
            sSet = getQNodes("sp", indexQNode);

        }

        @Override
		public void meet(Projection node) {
            // moves external tuples above statement patterns before attempting
            // to bubble down index statement patterns found in query tree

            organizeExtTuples(node);
            super.meet(node);
        }

        @Override
		public void meet(Join node) {
            // if right node contained in index, move it to bottom of query tree
            if (sSet.contains(node.getRightArg())) {

                Set<QueryModelNode> eSet = getQNodes("sp", node);
                Set<QueryModelNode> compSet = Sets.difference(eSet, sSet);

                if (eSet.containsAll(sSet)) {
                    QNodeExchanger qne = new QNodeExchanger(node.getRightArg(), compSet);
                    node.visit(qne);
                    node.replaceChildNode(node.getRightArg(), qne.getReplaced());

                    super.meet(node);
                }
                return;
            }
            // if left node contained in index, move it to bottom of query tree
            else if (sSet.contains(node.getLeftArg())) {

                Set<QueryModelNode> eSet = getQNodes("sp", node);
                Set<QueryModelNode> compSet = Sets.difference(eSet, sSet);

                if (eSet.containsAll(sSet)) {

                    QNodeExchanger qne = new QNodeExchanger(node.getLeftArg(), compSet);
                    node.visit(qne);
                    node.replaceChildNode(node.getLeftArg(), qne.getReplaced());

                    super.meet(node);
                }
                return;

            } else {
                super.meet(node);
            }

        }

        // moves all ExternalTupleSets in query tree above remaining
        // StatementPatterns
        private static void organizeExtTuples(QueryModelNode node) {

            ExternalTupleCollector eVis = new ExternalTupleCollector();
            node.visit(eVis);

            ExtTupleExchangeVisitor oev = new ExtTupleExchangeVisitor(eVis.getExtTup());
            node.visit(oev);
        }

    }

    // given a replacement QueryModelNode and compSet, this visitor replaces the
    // first
    // element in the query tree that occurs in compSet with replacement and
    // returns
    // the element that was replaced.
    private static class QNodeExchanger extends QueryModelVisitorBase<RuntimeException> {

        private QueryModelNode toBeReplaced;
        private QueryModelNode replacement;
        private Set<QueryModelNode> compSet;

        public QNodeExchanger(QueryModelNode replacement, Set<QueryModelNode> compSet) {
            this.replacement = replacement;
            this.toBeReplaced = replacement;
            this.compSet = compSet;
        }

        public QueryModelNode getReplaced() {
            return toBeReplaced;
        }

        @Override
		public void meet(Join node) {

            if (compSet.contains(node.getRightArg())) {
                this.toBeReplaced = node.getRightArg();
                node.replaceChildNode(node.getRightArg(), replacement);
                return;
            } else if (compSet.contains(node.getLeftArg())) {
                this.toBeReplaced = node.getLeftArg();
                node.replaceChildNode(node.getLeftArg(), replacement);
                return;
            } else {
                super.meet(node);
            }

        }

    }

    // moves filter that occurs in both query and index down the query tree so
    // that that it is positioned
    // above statement patterns associated with index. Precondition for calling
    // this method is that
    // SPBubbleDownVisitor has been called to position index StatementPatterns
    // within query tree.
    //could lead to problems if filter optimizer called before external processor
    private static class FilterBubbleDownVisitor extends QueryModelVisitorBase<RuntimeException> {

        private QueryModelNode filter;
        private Set<QueryModelNode> compSet;
        private boolean filterPlaced = false;

        public FilterBubbleDownVisitor(QueryModelNode filter, Set<QueryModelNode> compSet) {
            this.filter = filter;
            this.compSet = compSet;

        }

        public boolean filterPlaced() {
            return filterPlaced;
        }

        @Override
		public void meet(Join node) {

            if (!compSet.contains(node.getRightArg())) {
                // looks for placed to position filter node. if right node is
                // contained in index
                // and left node is statement pattern node contained in index or
                // is a join, place
                // filter above join.
                if (node.getLeftArg() instanceof Join || !compSet.contains(node.getLeftArg())) {

                    QueryModelNode pNode = node.getParentNode();
                    ((Filter) filter).setArg(node);
                    pNode.replaceChildNode(node, filter);
                    filterPlaced = true;

                    return;
                } // otherwise place filter below join and above right arg
                else {
                    ((Filter) filter).setArg(node.getRightArg());
                    node.replaceChildNode(node.getRightArg(), filter);
                    filterPlaced = true;
                    return;

                }
            } else if (node.getLeftArg() instanceof StatementPattern && !compSet.contains(node.getLeftArg())) {

                ((Filter) filter).setArg(node.getLeftArg());
                node.replaceChildNode(node.getLeftArg(), filter);
                filterPlaced = true;

                return;
            } else {
                super.meet(node);
            }
        }

    }

    private static Set<String> getVarNames(Collection<QueryModelNode> nodes) {

        List<String> tempVars;
        Set<String> nodeVarNames = Sets.newHashSet();

        for (QueryModelNode s : nodes) {
            tempVars = VarCollector.process(s);
            for (String t : tempVars) {
				nodeVarNames.add(t);
			}
        }
        return nodeVarNames;

    }

    // visitor which determines whether or not to reposition a filter by calling
    // FilterBubbleDownVisitor
    private static class FilterBubbleManager extends QueryModelVisitorBase<RuntimeException> {

        private TupleExpr tuple;
        private QueryModelNode indexQNode;
        private Set<QueryModelNode> sSet = Sets.newHashSet();
        private Set<QueryModelNode> bubbledFilters = Sets.newHashSet();

        public FilterBubbleManager(ExternalTupleSet index) {
            this.tuple = index.getTupleExpr();
            indexQNode = ((Projection) tuple).getArg();
            sSet = getQNodes(indexQNode);

        }

        @Override
		public void meet(Filter node) {

            Set<QueryModelNode> eSet = getQNodes(node);
            Set<QueryModelNode> compSet = Sets.difference(eSet, sSet);

            // if index contains filter node and it hasn't already been moved,
            // move it down
            // query tree just above position of statement pattern nodes found
            // in both query tree
            // and index (assuming that SPBubbleDownVisitor has already been
            // called)
            if (sSet.contains(node.getCondition()) && !bubbledFilters.contains(node.getCondition())) {
                FilterBubbleDownVisitor fbdv = new FilterBubbleDownVisitor(node.clone(), compSet);
                node.visit(fbdv);
                bubbledFilters.add(node.getCondition());
                // checks if filter correctly placed, and if it has been,
                // removes old copy of filter
                if (fbdv.filterPlaced()) {

                    QueryModelNode pNode = node.getParentNode();
                    TupleExpr cNode = node.getArg();
                    pNode.replaceChildNode(node, cNode);


                    super.meetNode(pNode);
                }
                super.meet(node);

            } else {
                super.meet(node);
            }
        }
    }

    // iterates through the query tree and attempts to match subtrees with
    // index. When a match is
    // found, the subtree is replaced by an ExternalTupleSet formed from the
    // index. Pre-condition for
    // calling this method is that both SPBubbleDownVisitor and
    // FilterBubbleManager have been called
    // to position the StatementPatterns and Filters.
    private static class SubsetEqualsVisitor extends QueryModelVisitorBase<RuntimeException> {

        private TupleExpr tuple;
        private QueryModelNode indexQNode;
        private ExternalTupleSet set;
        private Set<QueryModelNode> sSet = Sets.newHashSet();
        private boolean indexPlaced = false;


        public SubsetEqualsVisitor(ExternalTupleSet index, TupleExpr query) {
            this.tuple = index.getTupleExpr();
            this.set = index;
            indexQNode = ((Projection) tuple).getArg();
            sSet = getQNodes(indexQNode);

        }

        public boolean indexPlaced() {
            return indexPlaced;
        }


        @Override
		public void meet(Join node) {

            Set<QueryModelNode> eSet = getQNodes(node);

            if (eSet.containsAll(sSet) && !(node.getRightArg() instanceof BindingSetAssignment)) {

//                System.out.println("Eset is " + eSet + " and sSet is " + sSet);

                if (eSet.equals(sSet)) {
                    node.replaceWith(set);
                    indexPlaced = true;
                    return;
                } else {
                    if (node.getLeftArg() instanceof StatementPattern && sSet.size() == 1) {
                        if(sSet.contains(node.getLeftArg())) {
                            node.setLeftArg(set);
                            indexPlaced = true;
                        } else if(sSet.contains(node.getRightArg())) {
                            node.setRightArg(set);
                            indexPlaced = true;
                        } else {
                            return;
                        }
                    }
                    else {
                        super.meet(node);
                    }
                }
            } else if (eSet.containsAll(sSet)) {

                super.meet(node);

            } else {
                return;
            }

        }
      //to account for index consisting of only filter and BindingSetAssignment nodes
        @Override
		public void meet(Filter node) {

            Set<QueryModelNode> eSet = getQNodes(node);

            if (eSet.containsAll(sSet)) {

                if (eSet.equals(sSet)) {
                    node.replaceWith(set);
                    indexPlaced = true;
                    return;
                } else {
                    node.getArg().visit(this);
                }
            }
        }


        @Override
		public void meet(StatementPattern node) {
            return;
        }
    }

    // visitor which determines whether a query is valid (i.e. it does not
    // contain nodes other than
    // Projection, Join, Filter, StatementPattern )
    private static class ValidQueryVisitor extends QueryModelVisitorBase<RuntimeException> {

        private boolean isValid = true;

        public boolean isValid() {
            return isValid;
        }

        @Override
		public void meet(Projection node) {
            node.getArg().visit(this);
        }

        @Override
		public void meet(Filter node) {
            node.getArg().visit(this);
        }





        @Override
		public void meetNode(QueryModelNode node) {

            if (!(node instanceof Join || node instanceof StatementPattern || node instanceof BindingSetAssignment || node instanceof Var)) {
                isValid = false;
                return;

            } else{
                super.meetNode(node);
            }
        }

    }

    // repositions ExternalTuples above StatementPatterns within query tree
    private static class ExtTupleExchangeVisitor extends QueryModelVisitorBase<RuntimeException> {

        private Set<QueryModelNode> extTuples;

        public ExtTupleExchangeVisitor(Set<QueryModelNode> extTuples) {
            this.extTuples = extTuples;
        }

        @Override
		public void meet(Join queryNode) {

            // if query tree contains external tuples and they are not
            // positioned above statement pattern node
            // reposition
            if (this.extTuples.size() > 0 && !(queryNode.getRightArg() instanceof ExternalTupleSet)
                    && !(queryNode.getRightArg() instanceof BindingSetAssignment)) {

                if (queryNode.getLeftArg() instanceof ExternalTupleSet) {
                    QueryModelNode temp = queryNode.getLeftArg();
                    queryNode.setLeftArg(queryNode.getRightArg());
                    queryNode.setRightArg((TupleExpr)temp);
                } else {

                    QNodeExchanger qnev = new QNodeExchanger(queryNode.getRightArg(), this.extTuples);
                    queryNode.visit(qnev);
                    queryNode.replaceChildNode(queryNode.getRightArg(), qnev.getReplaced());
                    super.meet(queryNode);
                }
            } else {
                super.meet(queryNode);
            }

        }

    }

    private static class ExternalTupleCollector extends QueryModelVisitorBase<RuntimeException> {

        private Set<QueryModelNode> eSet = new HashSet<QueryModelNode>();

        @Override
        public void meetNode(QueryModelNode node) throws RuntimeException {
            if (node instanceof ExternalTupleSet) {
                eSet.add(node);
            }
            super.meetNode(node);
        }

        public Set<QueryModelNode> getExtTup() {
            return eSet;
        }

    }

    private static class FilterCollector extends QueryModelVisitorBase<RuntimeException> {

        private List<QueryModelNode> filterList = Lists.newArrayList();

        public List<QueryModelNode> getFilters() {
            return filterList;
        }

        @Override
        public void meet(Filter node) {
            filterList.add(node.getCondition());
            super.meet(node);
        }

    }

    private static void organizeBSAs(QueryModelNode node) {

        BindingSetAssignmentCollector bsac = new BindingSetAssignmentCollector();
        node.visit(bsac);

        if (bsac.containsBSAs()) {
            Set<QueryModelNode> bsaSet = bsac.getBindingSetAssignments();
            BindingSetAssignmentExchangeVisitor bsaev = new BindingSetAssignmentExchangeVisitor(bsaSet);
            node.visit(bsaev);
        }
    }

    // repositions ExternalTuples above StatementPatterns within query tree
    private static class BindingSetAssignmentExchangeVisitor extends QueryModelVisitorBase<RuntimeException> {

        private Set<QueryModelNode> bsas;

        public BindingSetAssignmentExchangeVisitor(Set<QueryModelNode> bsas) {
            this.bsas = bsas;
        }

        @Override
		public void meet(Join queryNode) {

            // if query tree contains external tuples and they are not
            // positioned above statement pattern node
            // reposition
            if (this.bsas.size() > 0 && !(queryNode.getRightArg() instanceof BindingSetAssignment)) {
                QNodeExchanger qnev = new QNodeExchanger(queryNode.getRightArg(), bsas);
                queryNode.visit(qnev);
                queryNode.replaceChildNode(queryNode.getRightArg(), qnev.getReplaced());
                super.meet(queryNode);
            } else {
                super.meet(queryNode);
            }

        }

    }


    public static class BindingSetAssignmentCollector extends QueryModelVisitorBase<RuntimeException> {

        private Set<QueryModelNode> bindingSetList = Sets.newHashSet();

        public Set<QueryModelNode> getBindingSetAssignments() {
            return bindingSetList;
        }

        public boolean containsBSAs() {
            return bindingSetList.size() > 0;
        }

        @Override
        public void meet(BindingSetAssignment node) {
            bindingSetList.add(node);
            super.meet(node);
        }

    }



    public static class QueryNodeCount extends QueryModelVisitorBase<RuntimeException> {

        private int nodeCount;

        public QueryNodeCount() {
            nodeCount = 0;
        }

        public int getNodeCount() {
            return nodeCount;
        }


        @Override
        public void meet(StatementPattern node) {
            nodeCount += 1;
            return;
        }

        @Override
        public void meet(Filter node) {
            nodeCount += 1;
            node.getArg().visit(this);
        }

    }



}
