package org.apache.rya.accumulo.pig;

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



import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.algebra.*;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * Date: 4/12/12
 * Time: 10:17 AM
 * To change this template use File | Settings | File Templates.
 */
public class SparqlToPigTransformVisitor extends QueryModelVisitorBase<RuntimeException> {
    private StringBuilder pigScriptBuilder = new StringBuilder();
    private String tablePrefix;
    private String instance, zk, user, password; //TODO: use a Configuration object to get these

    private Map<String, String> varToSet = new HashMap<String, String>();
    private Map<TupleExpr, List<String>> exprToNames = new HashMap<TupleExpr, List<String>>();
    private Map<TupleExpr, String> exprToVar = new HashMap<TupleExpr, String>();

    private char i = 'A'; //TODO: do better, hack

    public SparqlToPigTransformVisitor() {
        pigScriptBuilder.append("set pig.splitCombination false;\n")
                .append("set default_parallel 32;\n") //TODO: set parallel properly
                .append("set mapred.map.tasks.speculative.execution false;\n")
                .append("set mapred.reduce.tasks.speculative.execution false;\n")
                .append("set io.sort.mb 256;\n")
                .append("set mapred.child.java.opts -Xmx2048m;\n")
                .append("set mapred.compress.map.output true;\n")
                .append("set mapred.map.output.compression.codec org.apache.hadoop.io.compress.GzipCodec;\n")
                .append("set io.file.buffer.size 65536;\n")
                .append("set io.sort.factor 25;\n");
    }

    @Override
    public void meet(StatementPattern node) throws RuntimeException {
        super.meet(node);
        String subjValue = getVarValue(node.getSubjectVar());
        String predValue = getVarValue(node.getPredicateVar());
        String objValue = getVarValue(node.getObjectVar());

        String subj = i + "_s";
        String pred = i + "_p";
        String obj = i + "_o";
        String var = i + "";
        if (node.getSubjectVar().getValue() == null) {                  //TODO: look nicer
            subj = node.getSubjectVar().getName();
            varToSet.put(subj, var);

            addToExprToNames(node, subj);
        }
        if (node.getPredicateVar().getValue() == null) {                  //TODO: look nicer
            pred = node.getPredicateVar().getName();
            varToSet.put(pred, var);

            addToExprToNames(node, pred);
        }
        if (node.getObjectVar().getValue() == null) {                  //TODO: look nicer
            obj = node.getObjectVar().getName();
            varToSet.put(obj, var);

            addToExprToNames(node, obj);
        }
        if (node.getContextVar() != null && node.getContextVar().getValue() == null) {
            String cntxtName = node.getContextVar().getName();
            varToSet.put(cntxtName, var);

            addToExprToNames(node, cntxtName);
        }
        //load 'l_' using org.apache.rya.cloudbase.pig.dep.StatementPatternStorage('<http://www.Department0.University0.edu>', '', '',
        // 'stratus', 'stratus13:2181', 'root', 'password') AS (dept:chararray, p:chararray, univ:chararray);
//        pigScriptBuilder.append(i).append(" = load '").append(tablePrefix).append("' using org.apache.rya.cloudbase.pig.dep.StatementPatternStorage('")
//                .append(subjValue).append("','").append(predValue).append("','").append(objValue).append("','").append(instance).append("','")
//                .append(zk).append("','").append(user).append("','").append(password).append("') AS (").append(subj).append(":chararray, ")
//                .append(pred).append(":chararray, ").append(obj).append(":chararray);\n");

        //load 'cloudbase://tablePrefix?instance=myinstance&user=root&password=secret&zookeepers=127.0.0.1:2181&auths=PRIVATE,PUBLIC&subject=a&predicate=b&object=c'
        //using org.apache.rya.accumulo.pig.StatementPatternStorage() AS (dept:chararray, p:chararray, univ:chararray);
        pigScriptBuilder.append(i).append(" = load 'accumulo://").append(tablePrefix).append("?instance=").append(instance).append("&user=").append(user)
                .append("&password=").append(password).append("&zookeepers=").append(zk);
        if (subjValue != null && subjValue.length() > 0) {
            pigScriptBuilder.append("&subject=").append(subjValue);
        }
        if (predValue != null && predValue.length() > 0) {
            pigScriptBuilder.append("&predicate=").append(predValue);
        }
        if (objValue != null && objValue.length() > 0) {
            pigScriptBuilder.append("&object=").append(objValue);
        }
        if (node.getContextVar() != null && node.getContextVar().getValue() != null) {
            pigScriptBuilder.append("&context=").append(getVarValue(node.getContextVar()));
        }

        pigScriptBuilder.append("' using ").append(StatementPatternStorage.class.getName()).append("() AS (").append(subj).append(":chararray, ")
                .append(pred).append(":chararray, ").append(obj).append(":chararray");
        if (node.getContextVar() != null) {
            Value cntxtValue = node.getContextVar().getValue();
            String cntxtName = null;
            if (cntxtValue == null) {
                //use name
                cntxtName = node.getContextVar().getName();
            } else {
                cntxtName = i + "_c";
            }
            pigScriptBuilder.append(", ").append(cntxtName).append(":chararray");
        }
        pigScriptBuilder.append(");\n");
        //TODO: add auths

        exprToVar.put(node, var);
        i++;
    }

    private void addToExprToNames(TupleExpr node, String name) {
        List<String> names = exprToNames.get(node);
        if (names == null) {
            names = new ArrayList<String>();
            exprToNames.put(node, names);
        }
        names.add(name);
    }

    @Override
    public void meet(Union node) throws RuntimeException {
        super.meet(node);

        TupleExpr leftArg = node.getLeftArg();
        TupleExpr rightArg = node.getRightArg();
        String left_var = exprToVar.get(leftArg);
        String right_var = exprToVar.get(rightArg);
        //Q = UNION ONSCHEMA B, P;
        pigScriptBuilder.append(i).append(" = UNION ONSCHEMA ").append(left_var).append(", ").append(right_var).append(";\n");

        String unionVar = i + "";
        List<String> left_names = exprToNames.get(leftArg);
        List<String> right_names = exprToNames.get(rightArg);
        for (String name : left_names) {
            varToSet.put(name, unionVar);
            addToExprToNames(node, name);
        }
        for (String name : right_names) {
            varToSet.put(name, unionVar);
            addToExprToNames(node, name);
        }
        exprToVar.put(node, unionVar);
        i++;
    }

    @Override
    public void meet(Join node) throws RuntimeException {
        super.meet(node);

        TupleExpr leftArg = node.getLeftArg();
        TupleExpr rightArg = node.getRightArg();
        List<String> left_names = exprToNames.get(leftArg);
        List<String> right_names = exprToNames.get(rightArg);

        Set<String> joinNames = new HashSet<String>(left_names);
        joinNames.retainAll(right_names); //intersection, this is what I join on
        //SEC = join FIR by (MEMB_OF::ugrad, SUBORG_J::univ), UGRADDEG by (ugrad, univ);
        StringBuilder joinStr = new StringBuilder();
        joinStr.append("(");
        boolean first = true;
        for (String name : joinNames) { //TODO: Make this a utility method
            if (!first) {
                joinStr.append(",");
            }
            first = false;
            joinStr.append(name);
        }
        joinStr.append(")");

        String left_var = exprToVar.get(leftArg);
        String right_var = exprToVar.get(rightArg);
        if (joinStr.length() <= 2) {
            //no join params, need to cross
            pigScriptBuilder.append(i).append(" = cross ").append(left_var).append(", ").append(right_var).append(";\n");
        } else {
            //join
            pigScriptBuilder.append(i).append(" = join ").append(left_var);
            pigScriptBuilder.append(" by ").append(joinStr);
            pigScriptBuilder.append(", ").append(right_var);
            pigScriptBuilder.append(" by ").append(joinStr);
            pigScriptBuilder.append(";\n");

        }

        String joinVarStr = i + "";
        i++;
        // D = foreach C GENERATE A::subj AS subj:chararray, A::A_p AS p:chararray;
        String forEachVarStr = i + "";
        pigScriptBuilder.append(i).append(" = foreach ").append(joinVarStr).append(" GENERATE ");
        Map<String, String> nameToJoinName = new HashMap<String, String>();
        for (String name : left_names) {
            varToSet.put(name, forEachVarStr);
            addToExprToNames(node, name);
            nameToJoinName.put(name, left_var + "::" + name);
        }
        for (String name : right_names) {
            varToSet.put(name, forEachVarStr);
            addToExprToNames(node, name);
            nameToJoinName.put(name, right_var + "::" + name);
        }

        first = true;
        for (Map.Entry entry : nameToJoinName.entrySet()) {
            if (!first) {
                pigScriptBuilder.append(",");
            }
            first = false;
            pigScriptBuilder.append(entry.getValue()).append(" AS ").append(entry.getKey()).append(":chararray ");
        }
        pigScriptBuilder.append(";\n");

        exprToVar.put(node, forEachVarStr);
        i++;
    }

    @Override
    public void meet(Projection node) throws RuntimeException {
        super.meet(node);
        ProjectionElemList list = node.getProjectionElemList();
        String set = null;
        StringBuilder projList = new StringBuilder();
        boolean first = true;
        //TODO: we do not support projections from multiple pig statements yet
        for (String name : list.getTargetNames()) {
            set = varToSet.get(name);  //TODO: overwrite
            if (set == null) {
                throw new IllegalArgumentException("Have not found any pig logic for name[" + name + "]");
            }
            if (!first) {
                projList.append(",");
            }
            first = false;
            projList.append(name);
        }
        if (set == null)
            throw new IllegalArgumentException(""); //TODO: Fill this
        //SUBORG = FOREACH SUBORG_L GENERATE dept, univ;
        pigScriptBuilder.append("PROJ = FOREACH ").append(set).append(" GENERATE ").append(projList.toString()).append(";\n");
    }

    @Override
    public void meet(Slice node) throws RuntimeException {
        super.meet(node);
        long limit = node.getLimit();
        //PROJ = LIMIT PROJ 10;
        pigScriptBuilder.append("PROJ = LIMIT PROJ ").append(limit).append(";\n");
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getZk() {
        return zk;
    }

    public void setZk(String zk) {
        this.zk = zk;
    }

    public String getInstance() {
        return instance;
    }

    public void setInstance(String instance) {
        this.instance = instance;
    }

    public String getTablePrefix() {
        return tablePrefix;
    }

    public void setTablePrefix(String tablePrefix) {
        this.tablePrefix = tablePrefix;
    }

    public String getPigScript() {
        return pigScriptBuilder.toString();
    }

    protected String getVarValue(Var var) {
        if (var == null) {
            return "";
        } else {
            Value value = var.getValue();
            if (value == null) {
                return "";
            }
            if (value instanceof URI) {
                return "<" + value.stringValue() + ">";
            }
            if (value instanceof Literal) {
                Literal lit = (Literal) value;
                if (lit.getDatatype() == null) {
                    //string
                    return "\\'" + value.stringValue() + "\\'";
                }
            }
            return value.stringValue();
        }

    }
}
