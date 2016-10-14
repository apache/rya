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


import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.rya.accumulo.documentIndex.TextColumn;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.api.resolver.RyaContext;
import org.apache.rya.api.resolver.RyaTypeResolverException;
import org.apache.rya.joinselect.AccumuloSelectivityEvalDAO;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.hadoop.io.Text;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Var;

import com.beust.jcommander.internal.Maps;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.primitives.Bytes;

public class StarQuery {

    private List<StatementPattern> nodes;
    private TextColumn[] nodeColumnCond;
    private String commonVarName;
    private Var commonVar;
    private Var context;
    private String contextURI ="";
    private Map<String,Integer> varPos = Maps.newHashMap();
    private boolean isCommonVarURI = false;


    public StarQuery(List<StatementPattern> nodes) {
        this.nodes = nodes;
        if(nodes.size() == 0) {
            throw new IllegalArgumentException("Nodes cannot be empty!");
        }
        nodeColumnCond = new TextColumn[nodes.size()];
        Var tempContext = nodes.get(0).getContextVar();
        if(tempContext != null) {
            context = tempContext.clone();
        } else {
            context = new Var();
        }
        try {
            this.init();
        } catch (RyaTypeResolverException e) {
            e.printStackTrace();
        }
    }


    public StarQuery(Set<StatementPattern> nodes) {
        this(Lists.newArrayList(nodes));
    }

    public int size() {
        return nodes.size();
    }

    public StarQuery(StarQuery other) {
       this(other.nodes);
    }


    public List<StatementPattern> getNodes() {
        return nodes;
    }


    public TextColumn[] getColumnCond() {
        return nodeColumnCond;
    }


    public boolean isCommonVarURI() {
        return isCommonVarURI;
    }

    public String getCommonVarName() {
        return commonVarName;
    }

    public Var getCommonVar() {
        return commonVar;
    }

    public boolean commonVarHasValue() {
        return commonVar.getValue() != null;
    }

    public boolean commonVarConstant() {
        return commonVar.isConstant();
    }

    public String getCommonVarValue() {
        if(commonVarHasValue()) {
            return commonVar.getValue().stringValue();
        } else {
            return null;
        }
    }


    public Set<String> getUnCommonVars() {
        return varPos.keySet();
    }


    public Map<String,Integer> getVarPos() {
        return varPos;
    }

    public boolean hasContext() {
        return context.getValue() != null;
    }

    public String getContextURI() {
        return contextURI;
    }




    public Set<String> getBindingNames() {

        Set<String> bindingNames = Sets.newHashSet();

        for(StatementPattern sp: nodes) {

            if(bindingNames.size() == 0) {
                bindingNames = sp.getBindingNames();
            } else {
                bindingNames = Sets.union(bindingNames, sp.getBindingNames());
            }

        }

        return bindingNames;

    }




    public Set<String> getAssuredBindingNames() {

        Set<String> bindingNames = Sets.newHashSet();

        for(StatementPattern sp: nodes) {

            if(bindingNames.size() == 0) {
                bindingNames = sp.getAssuredBindingNames();
            } else {
                bindingNames = Sets.union(bindingNames, sp.getAssuredBindingNames());
            }

        }

        return bindingNames;

    }







    public CardinalityStatementPattern getMinCardSp(AccumuloSelectivityEvalDAO ase) {

        StatementPattern minSp = null;
        double cardinality = Double.MAX_VALUE;
        double tempCard = -1;

        for (StatementPattern sp : nodes) {

            try {
                tempCard = ase.getCardinality(ase.getConf(), sp);

                if (tempCard < cardinality) {
                    cardinality = tempCard;
                    minSp = sp;
                }
            } catch (TableNotFoundException e) {
                e.printStackTrace();
            }


        }

        return new CardinalityStatementPattern(minSp, cardinality) ;
    }



    public class CardinalityStatementPattern {

        private StatementPattern sp;
        private double cardinality;

        public CardinalityStatementPattern(StatementPattern sp, double cardinality) {
            this.sp = sp;
            this.cardinality = cardinality;
        }

        public StatementPattern getSp() {
            return sp;
        }

        public double getCardinality() {
            return cardinality;
        }

    }


   public double getCardinality( AccumuloSelectivityEvalDAO ase) {

        double cardinality = Double.MAX_VALUE;
        double tempCard = -1;

        ase.setDenormalized(true);

        try {

            for (int i = 0; i < nodes.size(); i++) {
                for (int j = i + 1; j < nodes.size(); j++) {

                    tempCard = ase.getJoinSelect(ase.getConf(), nodes.get(i), nodes.get(j));

                    if (tempCard < cardinality) {
                        cardinality = tempCard;
                    }

                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        ase.setDenormalized(false);

        return cardinality/(nodes.size() + 1);

    }



    public static Set<String> getCommonVars(StarQuery query, BindingSet bs) {

        Set<String> starQueryVarNames = Sets.newHashSet();

        if(bs == null || bs.size() == 0) {
            return Sets.newHashSet();
        }

        Set<String> bindingNames = bs.getBindingNames();
        starQueryVarNames.addAll(query.getUnCommonVars());
        if(!query.commonVarConstant()) {
            starQueryVarNames.add(query.getCommonVarName());
        }

        return Sets.intersection(bindingNames, starQueryVarNames);


    }






    public static StarQuery getConstrainedStarQuery(StarQuery query, BindingSet bs) {

        if(bs.size() == 0) {
            return query;
        }

        Set<String> bindingNames = bs.getBindingNames();
        Set<String> unCommonVarNames = query.getUnCommonVars();
        Set<String> intersectVar = Sets.intersection(bindingNames, unCommonVarNames);


        if (!query.commonVarConstant()) {

            Value v = bs.getValue(query.getCommonVarName());

            if (v != null) {
                query.commonVar.setValue(v);
            }
        }

        for(String s: intersectVar) {
            try {
                query.nodeColumnCond[query.varPos.get(s)] = query.setValue(query.nodeColumnCond[query.varPos.get(s)], bs.getValue(s));
            } catch (RyaTypeResolverException e) {
                e.printStackTrace();
            }
        }

        return query;
    }


    private TextColumn setValue(TextColumn tc, Value v) throws RyaTypeResolverException {

        String cq = tc.getColumnQualifier().toString();
        String[] cqArray = cq.split("\u0000");

        if (cqArray[0].equals("subject")) {
            // RyaURI subjURI = (RyaURI) RdfToRyaConversions.convertValue(v);
            tc.setColumnQualifier(new Text("subject" + "\u0000" + v.stringValue()));
            tc.setIsPrefix(false);
        } else if (cqArray[0].equals("object")) {
            RyaType objType = RdfToRyaConversions.convertValue(v);
            byte[][] b1 = RyaContext.getInstance().serializeType(objType);
            byte[] b2 = Bytes.concat("object".getBytes(),
                    "\u0000".getBytes(), b1[0], b1[1]);
            tc.setColumnQualifier(new Text(b2));
            tc.setIsPrefix(false);
        } else {
            throw new IllegalStateException("Invalid direction!");
        }

        return tc;

    }



    //assumes nodes forms valid star query with only one common variable
    //assumes nodes and commonVar has been set
    private TextColumn nodeToTextColumn(StatementPattern node, int i) throws RyaTypeResolverException {

        RyaContext rc = RyaContext.getInstance();

        Var subjVar = node.getSubjectVar();
        Var predVar = node.getPredicateVar();
        Var objVar = node.getObjectVar();

        RyaURI predURI = (RyaURI) RdfToRyaConversions.convertValue(node.getPredicateVar().getValue());


        //assumes StatementPattern contains at least on variable
        if (subjVar.isConstant()) {
            if (commonVarConstant()) {
                varPos.put(objVar.getName(), i);
                return new TextColumn(new Text(predURI.getData()), new Text("object"));
            } else {
                return new TextColumn(new Text(predURI.getData()), new Text("subject" + "\u0000"
                        + subjVar.getValue().stringValue()));
            }

        } else if (objVar.isConstant()) {

            if (commonVarConstant()) {
                varPos.put(subjVar.getName(), i);
                return new TextColumn(new Text(predURI.getData()), new Text("subject"));
            } else {

                isCommonVarURI = true;
                RyaType objType = RdfToRyaConversions.convertValue(objVar.getValue());
                byte[][] b1 = rc.serializeType(objType);

                byte[] b2 = Bytes.concat("object".getBytes(), "\u0000".getBytes(), b1[0], b1[1]);
                return new TextColumn(new Text(predURI.getData()), new Text(b2));
            }

        } else {
            if (subjVar.getName().equals(commonVarName)) {

                isCommonVarURI = true;
                varPos.put(objVar.getName(), i);

                TextColumn tc = new TextColumn(new Text(predURI.getData()), new Text("object"));
                tc.setIsPrefix(true);
                return tc;

            } else {

                varPos.put(subjVar.getName(), i);

                TextColumn tc = new TextColumn(new Text(predURI.getData()), new Text("subject"));
                tc.setIsPrefix(true);
                return tc;

            }


        }


    }




    //called in constructor after nodes set
    //assumes nodes and nodeColumnCond are same size
    private void init() throws RyaTypeResolverException {


        commonVar = this.getCommonVar(nodes);
        if(!commonVar.isConstant()) {
            commonVarName = commonVar.getName();
        } else {
            commonVarName = commonVar.getName().substring(7);
        }

        if(hasContext()) {
            RyaURI ctxtURI = (RyaURI) RdfToRyaConversions.convertValue(context.getValue());
            contextURI = ctxtURI.getData();
        }

        for(int i = 0; i < nodes.size(); i++){
            nodeColumnCond[i] = nodeToTextColumn(nodes.get(i), i);
        }

    }








    // called after nodes set
    // assumes nodes forms valid query with single, common variable
    private Var getCommonVar(List<StatementPattern> nodes) {

        Set<Var> vars = null;
        List<Var> tempVar;
        Set<Var> tempSet;

        int i = 0;
        for (StatementPattern sp : nodes) {

            if (vars == null) {
                vars = Sets.newHashSet();
                vars.add(sp.getSubjectVar());
                vars.add(sp.getObjectVar());
            } else {
                tempSet = Sets.newHashSet();
                tempSet.add(sp.getSubjectVar());
                tempSet.add(sp.getObjectVar());
                vars = Sets.intersection(vars, tempSet);
            }

        }

        if (vars.size() == 1) {
            return vars.iterator().next();
        } else if (vars.size() > 1) {
            Var first = null;

            i = 0;

            for (Var v : vars) {
                i++;

                if (i == 1) {
                    first = v;
                } else {
                    if (v.isConstant()) {
                        return v;
                    }
                }
            }

            return first;

        } else {
            throw new IllegalStateException("No common Var!");
        }

    }


    //assumes bindings is not of size 0
    private static boolean isBindingsetValid(Set<String> bindings) {

        int varCount = 0;

        if (bindings.size() == 1) {
            return true;
        } else {


            for (String s : bindings) {
                if (!s.startsWith("-const-")) {
                    varCount++;
                }
                if (varCount > 1) {
                    return false;
                }
            }

            return true;

        }

    }





    public static boolean isValidStarQuery(Collection<StatementPattern> nodes) {

        Set<String> bindings = null;
        boolean contextSet = false;
        Var context = null;

        if(nodes.size() < 2) {
            return false;
        }

        for(StatementPattern sp: nodes) {

            Var tempContext = sp.getContextVar();
            Var predVar = sp.getPredicateVar();

            //does not support variable context
            if(tempContext != null && !tempContext.isConstant()) {
               return false;
            }
            if(!contextSet) {
                context = tempContext;
                contextSet = true;
            } else {

                if(context == null && tempContext != null) {
                    return false;
                } else if (context != null && !context.equals(tempContext)) {
                    return false;
                }
            }

            if(!predVar.isConstant()) {
                return false;
            }

            if(bindings == null ) {
                bindings = sp.getBindingNames();
                if(bindings.size() == 0) {
                    return false;
                }
            } else {
                bindings = Sets.intersection(bindings, sp.getBindingNames());
                if(bindings.size() == 0) {
                    return false;
                }
            }

        }


        return isBindingsetValid(bindings);
    }





//    private static Set<String> getSpVariables(StatementPattern sp) {
//
//        Set<String> variables = Sets.newHashSet();
//        List<Var> varList = sp.getVarList();
//
//        for(Var v: varList) {
//            if(!v.isConstant()) {
//                variables.add(v.getName());
//            }
//        }
//
//        return variables;
//
//    }
//





    @Override
	public String toString() {

        String s = "Term conditions: " + "\n";

        for (TextColumn element : this.nodeColumnCond) {
            s = s + element.toString() + "\n";
        }

        s = s + "Common Var: " + this.commonVar.toString() + "\n";
        s = s + "Context: " + this.contextURI;

        return s;

    }






}
