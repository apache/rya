package org.apache.rya.indexing.pcj.matching;

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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.openrdf.model.Literal;
import org.openrdf.model.Value;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.NAryValueOperator;
import org.openrdf.query.algebra.ProjectionElem;
import org.openrdf.query.algebra.ProjectionElemList;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.ValueConstant;
import org.openrdf.query.algebra.ValueExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class QueryVariableNormalizer {

    
    /**
     * @param tuple1
     *            tuple expression from a parsed query
     * @param tuple2
     *            tuple expression from a parsed query (the proposed index whose
     *            variables are to be relabeled)
     * @return list of all possible tuples obtained by substituting the
     *         variables of proposed index with the variables from query
     * @throws Exception
     * @throws IllegalArgumentException
     */
    public static List<TupleExpr> getNormalizedIndex(TupleExpr tuple1, TupleExpr tuple2) throws Exception {

        List<QueryModelNode> nodes1, nodes2;
        TreeMap<String, List<QueryModelNode>> queryMap1, indexMap1;
        List<HashMap<String, String>> varChanges = new ArrayList<HashMap<String, String>>();
        List<TupleExpr> tupleList = new ArrayList<TupleExpr>();
        
       

        // if tuples are equal, no need to do anything
        if (tuple1.equals(tuple2)) {
            tupleList.add((TupleExpr) tuple1.clone());
            return tupleList;
        }

        
        NormalizeQueryVisitor tupNVis = new NormalizeQueryVisitor(false);
        NormalizeQueryVisitor indexNVis = new NormalizeQueryVisitor(true);
        tuple1.visit(tupNVis);
        tuple2.visit(indexNVis);
        
        
        TupleExpr tuple;
        queryMap1 = tupNVis.getMap();
        indexMap1 = indexNVis.getMap();

        // TreeMaps that used for comparators
        TreeMap<String, Integer>[] trees = (TreeMap<String, Integer>[]) new TreeMap[4];
        for (int i = 0; i < 4; i++) {
            trees[i] = new TreeMap<String, Integer>();
        }

        trees[0] = tupNVis.getKeyMap(); // query tuple variable count
        trees[2] = indexNVis.getKeyMap(); // index tuple variable count
         
      
        // if query does not contain as many constant Vars as index,
        // normalization not possible.
//        if (!(trees[0].keySet().size() >= trees[2].keySet().size())) {
//            System.out.println("In here:1");
//            return tupleList;
//        }

        // sort keys according to size of associated StatementPattern list
        // this optimization ensures that initial list of HashMaps (possible
        // variable substitutions)
        // is as small as possible
        // Maybe add additional criteria to comparator taking into account size
        // of query bin lists
        Set<String> keys = indexMap1.keySet();
        List<String> keyList = new ArrayList<String>(keys);
        Collections.sort(keyList, new ConstantKeyComp(indexMap1, queryMap1));

        // iterate through constant values associated with smaller tuple,
        // check that larger tuple constants these constants, and use lists
        // of associated statement patterns to begin to construct variable
        // substitutions
        // that are consistent

        for (String s : keyList) {
            if (queryMap1.containsKey(s)) {
                nodes1 = queryMap1.get(s);
                nodes2 = indexMap1.get(s);
                

                if (!(nodes1.size() >= nodes2.size())) {
//                    System.out.println("In here: 2");
//                    System.out.println("Node lists are " + nodes1 + " and " +
//                            nodes2);
                    return tupleList;
                }

                trees[1] = getListVarCnt(nodes1, tupNVis.getVariableMap()); // query
                                                                          // list
                                                                          // variable
                                                                          // count
                trees[3] = getListVarCnt(nodes2, indexNVis.getVariableMap()); // index
                                                                          // list
                                                                          // variable
                                                                          // count
                Collections.sort(nodes1, new CountComp(trees[1], trees[0]));
                Collections.sort(nodes2, new CountComp(trees[3], trees[2]));

                varChanges = statementCompare(nodes1, nodes2, varChanges, trees);

                if (varChanges.size() == 0) {
                    return tupleList;
                }
            }

            else {
                return tupleList;
            }

        }

        List<QueryModelNode> filters2 = indexNVis.getFilters();
        // determine if index contains filters whose variables need to be relabeled
        if (filters2.size() != 0) {
            List<QueryModelNode> filters1 = tupNVis.getFilters();
            // only attempt to normalize variables if query contains more filters than index
            if (filters1.size() >= filters2.size()) {
                Collections.sort(filters1, new FilterComp());
                Collections.sort(filters2, new FilterComp());

                varChanges = statementCompare(filters1, filters2, varChanges, trees);

            }
        }

        List<HashMap<String, String>> varChangeSet = new ArrayList<HashMap<String, String>>();

        for (HashMap<String, String> s : varChanges) {
            if (!varChangeSet.contains(s)) {
                varChangeSet.add(s);
            }

        }

        
        ValueMapVisitor valMapVis = new ValueMapVisitor();
        tuple1.visit(valMapVis);
        Map<String, Value> valMap = valMapVis.getValueMap();
        

        for (HashMap<String, String> s : varChangeSet) {
            //System.out.println(s);
            tuple = tuple2.clone();
            replaceTupleVariables(s, tuple, valMap);
            tupleList.add(tuple);
        }

        return tupleList;

    }

    /**
     * Produces a list of all possible substitutions stored in HashMaps that are
     * consistent with the two lists of statement patterns
     * 
     * @param qArray
     *            list of Statement nodes from query tuple
     * @param iArray
     *            list of Statement nodes from index tuple
     * @param hMaps
     *            HashMap containing variable substitutions
     * @param trees
     *            TreeMaps used for statement pattern node ordering
     * @return
     */
    private static List<HashMap<String, String>> statementCompare(List<QueryModelNode> qArray,
            List<QueryModelNode> iArray, List<HashMap<String, String>> hMaps, TreeMap<String, Integer>[] trees) {

        if (hMaps.size() == 0) {
            HashMap<HashMap<String, String>, Boolean> mapConsistent = new HashMap<HashMap<String, String>, Boolean>();
            HashMap<String, String> hMap = new HashMap<String, String>();
            mapConsistent.put(hMap, false);
            evaluateMap(qArray, iArray, hMap, hMaps, mapConsistent, trees);

            return hMaps;
        }

        else {

            ArrayList<HashMap<String, String>> tempMaps = Lists.newArrayList(hMaps);
            HashMap<HashMap<String, String>, Boolean> mapConsistent = new HashMap<HashMap<String, String>, Boolean>();
            for (HashMap<String, String> s : hMaps) {
                mapConsistent.put(s, false);
            }
            for (HashMap<String, String> s : hMaps) {
                evaluateMap(qArray, iArray, s, tempMaps, mapConsistent, trees);
            }

            return tempMaps;

        }
    }

   
    /**
     * Adds or removes HashMap substitution schemes to the list of substitutions
     * schemes depending on whether or not they are consistent with the two
     * lists of statement patterns
     * 
     * @param qArray
     *            List of StatementPatterns associated with query array
     * @param iArray
     *            List of StatementPatterns associated with index array
     * @param hMap
     *            HashMap of substitutions to be analyzed for consistent and
     *            added or removed
     * @param hMaps
     *            List of HashMaps containing substitution schemes
     * @param trees
     *            Array of TreeMaps used for comparison of StatementPattern
     *            nodes
     */
    private static void evaluateMap(List<QueryModelNode> qArray, List<QueryModelNode> iArray,
            HashMap<String, String> hMap, List<HashMap<String, String>> hMaps,
            HashMap<HashMap<String, String>, Boolean> mapConsistent, TreeMap<String, Integer>[] trees) throws IllegalArgumentException {

        // if all nodes in indexArray have been exhausted, add map of substitutions to
        // list of possible substitution schemes.
        if (iArray.size() == 0) {
            if (!hMaps.contains(hMap)) {
                hMaps.add(hMap);
            }
            mapConsistent.put(hMap, true);
            return;
        }

        // for a given constant key, iterate through possible combinations of statement pattern nodes contained in associated query list and
        // index list to generate all possible substitution schemes.
        for (int i = 0; i < iArray.size(); i++) {
            for (int j = 0; j < qArray.size(); j++) {
                //System.out.println("Query list is " + qArray+ " and index list is " + iArray);
                
                QueryModelNode node1 = qArray.get(j);
                QueryModelNode node2 = iArray.get(i);
                // if lists contain statement patterns, check to see if two given statement patterns have same structure
                // independent of variables names (same constants in same place, non constant Vars in same place)
                if ((node1 instanceof StatementPattern) && (node2 instanceof StatementPattern)) {
                    if (genConstantCompare((StatementPattern) node1, (StatementPattern) node2)) {

                        List<Var> variables1 = ((StatementPattern)node1).getVarList();
                        List<Var> variables2 =  ((StatementPattern)node2).getVarList();
                        
                        List<List<String>> vars = genGetCommonVars(variables1, variables2);
                        List<String> vars1 = vars.get(0);
                        List<String> vars2 = vars.get(1);

                        if (listConsistent(vars1, vars2, hMap)) {

                            HashMap<String, String> hashMap = Maps.newHashMap(hMap);
                            putVars(vars1, vars2, hashMap);

                            List<QueryModelNode> queryArray = Lists.newArrayList(qArray);
                            List<QueryModelNode> indexArray = Lists.newArrayList(iArray);

                            indexArray.remove(i);
                            queryArray.remove(j);

                            evaluateMap(queryArray, indexArray, hashMap, hMaps, mapConsistent, trees);
                        }
                    }
                } // if lists contain filters, see if filters have same structure independent of variables names
                //(check that conditions are same independent of variable names).
                else if ((node1 instanceof Filter) && (node2 instanceof Filter)) {
                    try {
                        if (filterCompare((Filter) node1, (Filter) node2)) {

                            List<QueryModelNode> variables1 = FilterVarValueCollector.process(((Filter) node1).getCondition());
                            List<QueryModelNode> variables2 = FilterVarValueCollector.process(((Filter) node2).getCondition());
                            
                            List<List<String>> vars = filterCommonVars(variables1, variables2);
                            List<String> vars1 = vars.get(0);
                            List<String> vars2 = vars.get(1);
                            
                            if (listConsistent(vars1, vars2, hMap)) {

                                HashMap<String, String> hashMap = Maps.newHashMap(hMap);
                                putVars(vars1, vars2, hashMap);

                                List<QueryModelNode> queryArray = Lists.newArrayList(qArray);
                                List<QueryModelNode> indexArray = Lists.newArrayList(iArray);

                                indexArray.remove(i);
                                queryArray.remove(j);

                                evaluateMap(queryArray, indexArray, hashMap, hMaps, mapConsistent, trees);
                            }

                        }
                    } catch (Exception e) {
                        System.out.println("Invalid Filter! " + e);
                    }

                } else {
                    throw new IllegalArgumentException("Invalid query tree.");
                }

            }
        }
        if (mapConsistent.containsKey(hMap))
            if (mapConsistent.get(hMap) == false) {
                hMaps.remove(hMap);
            }
        return;

    }


    
    
    private static List<List<String>> genGetCommonVars(List<Var> vars1, List<Var> vars2) {
        
        
        List<List<String>> varList = Lists.newArrayList();
        List<String> varList1 = Lists.newArrayList();
        List<String> varList2 = Lists.newArrayList();
        

        
        for (int i = 0; i < vars1.size(); i++) {

            if (!vars1.get(i).isConstant() && !vars2.get(i).isConstant()) {

                varList1.add(vars1.get(i).getName());
                varList2.add(vars2.get(i).getName());

            } else if(vars1.get(i).isConstant() && !vars2.get(i).isConstant()) {
                varList1.add(vars1.get(i).getName());
                varList2.add(vars2.get(i).getName());  
            }

        }
        
        varList.add(varList1);
        varList.add(varList2);

        return varList;
    }
    
    
 private static List<List<String>> filterCommonVars(List<QueryModelNode> vars1, List<QueryModelNode> vars2) {
        
        
        List<List<String>> varList = Lists.newArrayList();
        List<String> varList1 = Lists.newArrayList();
        List<String> varList2 = Lists.newArrayList();
        

        
        for (int i = 0; i < vars1.size(); i++) {

            if ((vars1.get(i) instanceof ValueConstant) && (vars2.get(i) instanceof Var)) {
                
                ValueConstant vc = (ValueConstant) vars1.get(i);
                String s = vc.getValue().toString();
                if(vc.getValue() instanceof Literal) {
                    s = s.substring(1, s.length() - 1);
                } 
                s = "-const-" + s;
                varList1.add(s);
                varList2.add(((Var)vars2.get(i)).getName());
            } else if(!(vars1.get(i) instanceof ValueConstant)){
                if (!((Var) vars1.get(i)).isConstant() && (vars2.get(i) instanceof Var)
                        && !((Var) vars2.get(i)).isConstant()) {
                    varList1.add(((Var) vars1.get(i)).getName());
                    varList2.add(((Var) vars2.get(i)).getName());
                } else if (((Var) vars1.get(i)).isConstant() && (vars2.get(i) instanceof Var)
                        && !((Var) vars2.get(i)).isConstant()) {
                    varList1.add(((Var) vars1.get(i)).getName());
                    varList2.add(((Var) vars2.get(i)).getName());
                }
            }

        }
        
        varList.add(varList1);
        varList.add(varList2);

        return varList;
    }
    
    
    
    private static boolean genConstantCompare(StatementPattern queryNode, StatementPattern indexNode) {
        
        

        ArrayList<Var> vars1 = (ArrayList<Var>) queryNode.getVarList();
        ArrayList<Var> vars2 = (ArrayList<Var>) indexNode.getVarList();

        
        for (int i = 0; i < vars1.size(); i++) {

            if (vars1.get(i).isConstant() && vars2.get(i).isConstant()) {

                if (!vars1.get(i).equals(vars2.get(i))) {
                    return false;

                }

            } else if(!vars1.get(i).isConstant() && vars2.get(i).isConstant() ) {
                    return false;
            }

        }

        return true;

    }
        
    
    
    
    /**
     * Method checks that substituting val for key is consistent with
     * substitutions in hMap
     * 
     * @param val
     *            substituting variable
     * @param key
     *            variable to be substituted for
     * @param hMap
     *            HashMap containing the substitutions to be made
     * @return true if the proposed substitution is consistent with hMap, and
     *         false otherwise
     */
    private static boolean checkVariables(String val, String key, HashMap<String, String> hMap) {

        if (!hMap.containsKey(key) && !hMap.containsValue(val)) {

            return true;
        } else if (!hMap.containsKey(key) && hMap.containsValue(val) || hMap.containsKey(key)
                && !hMap.containsValue(val)) {

            return false;
        } else {

            if (hMap.get(key).equals(val)) {
                return true;
            } else
                return false;

        }

    }

    
    
   
    
    
    // given two lists of variables and a HashMap, checks to see if substituting variable names in varList1
    // for variable names in varList2 is consistent with map.
    private static boolean listConsistent(List<String> varList1, List<String> varList2, HashMap<String, String> hMap) {

        for (int k = 0; k < varList1.size(); k++) {

            String s1 = varList1.get(k);
            String s2 = varList2.get(k);
            if (!checkVariables(s1, s2, hMap)) {
                return false;
            }
        }
        return true;

    }

    
    // given two lists of variables and a HashMap, substitutes variable names in varList1
    // for variable names in varList2 by updating map.
    private static void putVars(List<String> varList1, List<String> varList2, HashMap<String, String> hashMap) {

        for (int k = 0; k < varList1.size(); k++) {
            String s1 = varList1.get(k);
            String s2 = varList2.get(k);
            if (!hashMap.containsKey(s2)) {

                hashMap.put(s2, s1);
            }
        }

    }

   
    /**
     * @param filter1
     * @param filter2
     * @return true if filter2 is equal to filter1 once variables in filter2 are replaced with variables and constants
     * occurring in same position in filter1 (allows filter1 to contain constants where filter2 contains variables)
     * @throws Exception
     */
    private static boolean filterCompare(Filter filter1, Filter filter2) throws Exception {

        NodeCollector nc1 = new NodeCollector();
        NodeCollector nc2 = new NodeCollector();

        filter1.getCondition().visit(nc1);
        filter2.getCondition().visit(nc2);

        List<QueryModelNode> nodeList1 = nc1.getNodes();
        List<QueryModelNode> nodeList2 = nc2.getNodes();

        if (nodeList1.size() != nodeList2.size()) {
            return false;
        }

        for (int i = 0; i < nodeList1.size(); i++) {
            if ((nodeList1.get(i) instanceof ValueConstant) && (nodeList2.get(i) instanceof Var)) {
                continue;
            } else {
                if (nodeList1.get(i).getClass() != nodeList2.get(i).getClass()) {
                    return false;
                }
            }
        }

        return true;

    }

    /**
     * Given a HashMap containing variable substitutions and a tuple, this
     * method uses a visitor to iterate through the tuple and make the necessary
     * substitutions
     * 
     * @param varChanges
     * @param tuple
     * @throws Exception
     */
    private static void replaceTupleVariables(HashMap<String, String> varChanges, TupleExpr tuple, Map<String,Value> valMap) throws Exception {

        TupleVarRenamer visitor = new TupleVarRenamer(varChanges, valMap);
        tuple.visit(visitor);
    }

    /**
     * Given a list of StatementPattern nodes and a TreeMap containing the
     * variables in the tuple, this method counts the number of occurrences of
     * each variable in the given list
     * 
     * @param list
     *            List of StatementPattern nodes
     * @param cnt
     *            TreeMap whose keys are tuple variables and whose value is 0
     * @return TreeMap whose keys are tuple variables and whose value is the
     *         number of times variable appears in list
     */
    private static TreeMap<String, Integer> getListVarCnt(List<QueryModelNode> list, TreeMap<String, Integer> cnt) {

        int count = 0;

        for (QueryModelNode qNode : list) {
            List<String> vars = VarCollector.process(qNode);
            for (String s : vars) {
                count = cnt.get(s);
                count++;
                cnt.put(s, count);
            }

        }

        return cnt;

    }

    /**
     * Given a StatementPattern and two TreeMaps containing the variable counts
     * associated with an associated list and tuple, this method assigns a
     * number to the StatementPattern node which is determined by the number of
     * times its variables (non-constant Vars) appear in the list and throughout
     * the tuple
     * 
     * @param sp
     *            StatementPattern node
     * @param listCount
     *            TreeMap with variable count info associated with list
     * @param tupCount
     *            TreeMap with variable count info associated with tuple
     * @return count info associated with StatementPattern node
     */
    private static int getSpCount(QueryModelNode sp, TreeMap<String, Integer> listCount,
            TreeMap<String, Integer> tupCount) {

        int spCount = 0;

        List<String> vars = VarCollector.process(sp);
        for (String var : vars) {
            spCount = spCount + listCount.get(var) + tupCount.get(var);
        }
        return spCount;

    }

    /**
     * @return NormalizedQueryVisitor
     */
    public static NormalizeQueryVisitor getVisitor(boolean isIndex) {
        return new NormalizeQueryVisitor(isIndex);

    }

   
    // ********************Definition of Comparators****************
    // *************************************************************
    public static class CountComp implements Comparator<QueryModelNode> {

        private TreeMap<String, Integer> lCount, tupleCount;

        public CountComp(TreeMap<String, Integer> lCount, TreeMap<String, Integer> tupleCount) {

            this.lCount = lCount;
            this.tupleCount = tupleCount;
        }

        // compares StatementPattern nodes based on frequency at which their
        // variables appear in other StatementPattern nodes in associated
        // tuple and list

        public int compare(QueryModelNode sp1, QueryModelNode sp2) {

            return -(getSpCount(sp1, lCount, tupleCount) - getSpCount(sp2, lCount, tupleCount));
        }

    }

    // comparator to sort constant key list according to size of associated
    // StatementPattern array
    public static class ConstantKeyComp implements Comparator<String> {

        private TreeMap<String, List<QueryModelNode>> indexMap, queryMap;

        public ConstantKeyComp(TreeMap<String, List<QueryModelNode>> indexMap,
                TreeMap<String, List<QueryModelNode>> queryMap) {

            this.indexMap = indexMap;
            this.queryMap = queryMap;

        }

        // Compare method to sort keys of HashMap<String,
        // ArrayList<StatementPattern>
        // for index based on whether key also appears in query Map--if key does
        // not appear
        // in query map, key is given value 0 so it is moved to front when key
        // list is sorted.
        // If key appears in query map, key is assigned value that is the sum of
        // the size of the associated
        // lists in index map and query map.

        public int compare(String key1, String key2) {

            int len1 = 0;
            int len2 = 0;

            if (queryMap.containsKey(key1) && indexMap.containsKey(key1))
                len1 = indexMap.get(key1).size() + queryMap.get(key1).size();
            if (queryMap.containsKey(key2) && indexMap.containsKey(key2))
                len2 = indexMap.get(key2).size() + queryMap.get(key2).size();

            return (len1 - len2);

        }

    }

    public static class FilterComp implements Comparator<QueryModelNode> {

        public int compare(QueryModelNode q1, QueryModelNode q2) {

            int size1 = VarCollector.process(q1).size();
            int size2 = VarCollector.process(q2).size();

            return size1 - size2;

        }

    }

    // ******************** Definition of Visitors*****************
    // ************************************************************

    

    public static class ValueMapVisitor extends QueryModelVisitorBase<Exception> {

        
        private Map<String, Value> valMap = Maps.newHashMap();

       
        
        public Map<String, Value> getValueMap() {
            return valMap;
        }

        public void meet(Var var) {
            if (var.isConstant()) {
                valMap.put(var.getName(),var.getValue());
            }
            
            
        }

        public void meet(ValueConstant val) {

            String s = val.getValue().toString();
            
            if (val.getValue() instanceof Literal) {
                s = s.substring(1, s.length() - 1);
            }
            
            s = "-const-" + s;
            valMap.put(s, val.getValue());
        }

    }
    
    
    
    
    
    
    
    public static class NodeCollector extends QueryModelVisitorBase<Exception> {

        
        private List<QueryModelNode> nodes = Lists.newArrayList();

        public List<QueryModelNode> getNodes() {
            return nodes;
        }

        @Override
        public void meetNode(QueryModelNode node) throws Exception {
            nodes.add(node);
            super.meetNode(node);
        }

    }

    public static class SpVarReNamer extends QueryModelVisitorBase<RuntimeException> {

        private final HashMap<String, String> hMap;
        private Map<String, Value> valMap;
        private final ValueFactoryImpl vf = new ValueFactoryImpl();

        public SpVarReNamer(HashMap<String, String> hMap, Map<String, Value> valMap) {
            this.valMap = valMap;
            this.hMap = hMap;
        }

        public void meet(Var var) {
            if (!var.isConstant() && hMap.containsKey(var.getName())) {
                String val = hMap.get(var.getName());
                if (val.startsWith("-const-")) {
                   var.setName(val);
                   var.setValue(valMap.get(val));
                   var.setAnonymous(true); //TODO this might be a hack -- when are Vars not anonymous?
                } else {
                    var.setName(val);
                }
            }
        }

    }
    
    
    
    
    public static class FilterVarReNamer extends QueryModelVisitorBase<RuntimeException> {

        private final HashMap<String, String> hMap;
        private Map<String, Value> valMap;
        private final ValueFactoryImpl vf = new ValueFactoryImpl();

        public FilterVarReNamer(HashMap<String, String> hMap, Map<String, Value> valMap) {
            this.valMap = valMap;
            this.hMap = hMap;
        }

        @Override
        public void meet(Var var) {
            
            if (!(var.getParentNode() instanceof NAryValueOperator)) {
                if (!var.isConstant() && hMap.containsKey(var.getName())) {
                    String val = hMap.get(var.getName());
                    if (val.startsWith("-const-")) {
                        var.replaceWith(new ValueConstant(valMap.get(val)));
                    } else {
                        var.setName(val);
                    }
                }
            }
        }
        
        
        
        @Override
        public void meetNAryValueOperator(NAryValueOperator node) {

            List<ValueExpr> oldValues = node.getArguments();
            List<ValueExpr> newValues = Lists.newArrayList();

            for (ValueExpr v : oldValues) {
                if (v instanceof Var) {
                    Var var = (Var) v;
                    if (!(var.isConstant() && hMap.containsKey(var.getName()))) {
                        String val = hMap.get(var.getName());
                        if (val.startsWith("-const-")) {
                            newValues.add(new ValueConstant(valMap.get(val)));
                        } else {
                            var.setName(val);
                            newValues.add(var);
                        }
                    }
                } else {
                    newValues.add(v);
                }
            }
            
            node.setArguments(newValues);

        }
        

    }
    
    
    

    public static class TupleVarRenamer extends QueryModelVisitorBase<RuntimeException> {

        private final HashMap<String, String> varChanges;
        private Map<String, Value> valMap;

        public TupleVarRenamer(HashMap<String, String> varChanges, Map<String, Value> valMap) {
            this.varChanges = varChanges;
            this.valMap = valMap;
        }

        @Override
        public void meet(ProjectionElemList node) {
            List<ProjectionElem> proj = node.getElements();
            for (ProjectionElem s : proj) {
                if (varChanges.containsKey(s.getSourceName())) {
                    String name = s.getSourceName();
                    s.setSourceName(varChanges.get(name));
                    s.setTargetName(varChanges.get(name));
                   
                }
            }

        }
        
        
        @Override
       public void meet(StatementPattern node) {
           SpVarReNamer spv = new SpVarReNamer(varChanges, valMap);
           node.visit(spv);
       }
       
       
       @Override
       public void meet(Filter node) {
           FilterVarReNamer fvr = new FilterVarReNamer(varChanges, valMap);
           node.getCondition().visit(fvr);
           node.getArg().visit(this);
           
       }
        
        

    }

    public static class VarCollector extends QueryModelVisitorBase<RuntimeException> {

        public static List<String> process(QueryModelNode node) {
            VarCollector collector = new VarCollector();
            node.visit(collector);
            return collector.getVarNames();
        }
        
        public static List<Var> processVar(QueryModelNode node) {
            VarCollector collector = new VarCollector();
            node.visit(collector);
            return collector.getVars();
        }

        private List<String> varNames = new ArrayList<String>();
        private List<Var> vars = Lists.newArrayList();

        public List<String> getVarNames() {
            return varNames;
        }
        
        public List<Var> getVars() {
            return vars;
        }

        @Override
        public void meet(Var var) {
            if (!var.hasValue()) {
                varNames.add(var.getName());
            }
            vars.add(var);
        }
    }
    
    public static class FilterVarValueCollector extends QueryModelVisitorBase<RuntimeException> {

        public static List<QueryModelNode> process(QueryModelNode node) {
            FilterVarValueCollector collector = new FilterVarValueCollector();
            node.visit(collector);
            return collector.getVars();
        }
        
      
       
        private List<QueryModelNode> vars = Lists.newArrayList();

        
        public List<QueryModelNode> getVars() {
            return vars;
        }

        @Override
        public void meet(Var node) {
            vars.add(node);
        }
        
        @Override
        public void meet(ValueConstant node) {
            vars.add(node);
        }
        
        
        
    }
    
    
    

    public static class NormalizeQueryVisitor extends QueryModelVisitorBase<Exception> {

        private TreeMap<String, List<QueryModelNode>> map = new TreeMap<String, List<QueryModelNode>>();
        private TreeMap<String, Integer> varMap = new TreeMap<String, Integer>();
        private TreeMap<String, Integer> emptyVarMap = new TreeMap<String, Integer>();
        private List<StatementPattern> statementList = new ArrayList<StatementPattern>();
        private List<QueryModelNode> filters = new ArrayList<QueryModelNode>();
        private boolean isIndex;
        
        
        
        public NormalizeQueryVisitor(boolean isIndex) {
            this.isIndex = isIndex;
        }
        
        
        
        private TreeMap<String, List<QueryModelNode>> getMap() {

            return map;

        }
        

        private TreeMap<String, Integer> getKeyMap() {

            return varMap;
        }

        private TreeMap<String, Integer> getVariableMap() {
            return emptyVarMap;
        }

        public List<StatementPattern> getStatementPatterns() {
            return statementList;
        }
        

        private List<QueryModelNode> getFilters() {

            return filters;
        }

        @Override
        public void meet(StatementPattern node) throws Exception {

            statementList.add(node);

            String s = "";
            String t = "";

            Var node1 = node.getSubjectVar();
            Var node2 = node.getObjectVar();
            Var node3 = node.getPredicateVar();
            Var node4 = node.getContextVar();
            
            String s1 = "";
            String s2 = "";
            String s3 = "";
            String s4 = "";
            
            
            if (node1.isConstant())
                s1 = node1.getName().substring(7);

            if (node2.isConstant())
                s2 = node2.getName().substring(7);

            if (node3.isConstant())
                s3 = node3.getName().substring(7);

            if (node4 != null) {
                if (node4.isConstant())
                    s4 = node4.getName().substring(7);
            }

            if ((s1+s2+s3).length() == 0) {
                s = "Nonconstant nodes have no variables.";
            }
              
            List<QueryModelNode> nodes;
            
            
            if (s.length() > 0) {
       
                if (map.containsKey(s)) {
                    nodes = map.get(s);
                    nodes.add(node);
                } else {
                    nodes = new ArrayList<QueryModelNode>();
                    nodes.add(node);
                }
                
                map.put(s, nodes);
                
            } else {

                if (isIndex) {

                    t = s1 + s2 + s3 + s4;

                    if (map.containsKey(t)) {
                        nodes = map.get(t);
                        nodes.add(node);
                    } else {
                        nodes = new ArrayList<QueryModelNode>();
                        nodes.add(node);
                    }

                    map.put(t, nodes);

                } else {

                    String[] comps = new String[4];
                    comps[0] = s1;
                    comps[1] = s2;
                    comps[2] = s3;
                    comps[3] = s4;

                    for (int i = 0; i < 3; i++) {
                        if (comps[i].length() != 0) {
                            if (map.containsKey(comps[i] + comps[3])) {
                                nodes = map.get(comps[i] + comps[3]);
                                nodes.add(node);
                            } else {
                                nodes = new ArrayList<QueryModelNode>();
                                nodes.add(node);
                            }

                            map.put(comps[i] + comps[3], nodes);

                            for (int j = i + 1; j < 3; j++) {
                                if (comps[j].length() != 0) {
                                    if (map.containsKey(comps[i] + comps[j] + comps[3])) {
                                        nodes = map.get(comps[i] + comps[j] + comps[3]);
                                        nodes.add(node);
                                    } else {
                                        nodes = new ArrayList<QueryModelNode>();
                                        nodes.add(node);
                                    }
                                    map.put(comps[i] + comps[j] + comps[3], nodes);
                                }

                            }
                        }
                    }

                    if (s1.length() != 0 && s2.length() != 0 && s3.length() != 0) {
                        if (map.containsKey(s1 + s2 + s3 + s4)) {
                            nodes = map.get(s1 + s2 + s3 + s4);
                            nodes.add(node);
                        } else {
                            nodes = new ArrayList<QueryModelNode>();
                            nodes.add(node);
                        }
                        map.put(s1 + s2 + s3 + s4, nodes);
                    }
                }
            }
           
            super.meet(node);

        }

        @Override
        public void meet(Var node) throws Exception {

            int count = 1;

            if (!node.isConstant()) {
                if (varMap.containsKey(node.getName())) {
                    count = varMap.get(node.getName());
                    count++;
                    varMap.put(node.getName(), count);
                } else
                    varMap.put(node.getName(), 1);

                if (!emptyVarMap.containsKey(node.getName()))
                    emptyVarMap.put(node.getName(), 0);

            }
            super.meet(node);
        }

        public void meet(Filter filter) throws Exception {
            filters.add(filter);
            super.meet(filter);
        }

    }

}
