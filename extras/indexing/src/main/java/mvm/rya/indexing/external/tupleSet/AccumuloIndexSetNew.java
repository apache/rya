package mvm.rya.indexing.external.tupleSet;

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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;

import mvm.rya.accumulo.precompQuery.AccumuloPcjQuery;
import mvm.rya.indexing.PcjQuery;
import mvm.rya.rdftriplestore.evaluation.ExternalBatchingIterator;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.parser.ParsedTupleQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;
import org.openrdf.sail.SailException;

import com.google.common.base.Joiner;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class AccumuloIndexSetNew extends ExternalTupleSet implements ExternalBatchingIterator {

    private static final int WRITER_MAX_WRITE_THREADS = 30;
    private static final long WRITER_MAX_LATNECY = Long.MAX_VALUE;
    private static final long WRITER_MAX_MEMORY = 500L * 1024L * 1024L;
    private List<String> bindingslist;
    private final Connector accCon;
    private final String tablename;
    private long tableSize = 0;
    private List<String> varOrder = null;

    @Override
    public Map<String, Set<String>> getSupportedVariableOrders() {
        return this.getSupportedVariableOrderMap();
    }

    @Override
    public boolean supportsBindingSet(Set<String> bindingNames) {

        final Map<String, Set<String>> varOrderMap = this.getSupportedVariableOrders();
        final Collection<Set<String>> values = varOrderMap.values();
        final Set<String> bNames = Sets.newHashSet();

        for (final String s : this.getTupleExpr().getAssuredBindingNames()) {
            if (bindingNames.contains(s)) {
                bNames.add(s);
            }
        }
        return values.contains(bNames);
    }

    @Override
    public void setProjectionExpr(Projection tupleExpr) {
        super.setProjectionExpr(tupleExpr);
        this.bindingslist = Lists.newArrayList(tupleExpr.getAssuredBindingNames());
    }

    @Override
    public String getSignature() {
        return "AccumuloIndexSet(" + tablename + ") : " + Joiner.on(", ").join(bindingslist);
    }

    public AccumuloIndexSetNew(String sparql, Connector accCon, String tablename) throws MalformedQueryException, SailException, QueryEvaluationException,
            MutationsRejectedException, TableNotFoundException {
        super(null);
        this.tablename = tablename;
        this.accCon = accCon;
        final SPARQLParser sp = new SPARQLParser();
        final ParsedTupleQuery pq = (ParsedTupleQuery) sp.parseQuery(sparql, null);
        setProjectionExpr((Projection) pq.getTupleExpr());
        this.bindingslist = Lists.newArrayList(pq.getTupleExpr().getAssuredBindingNames());

        final Scanner s = accCon.createScanner(tablename, new Authorizations());
        s.setRange(Range.exact(new Text("~SPARQL")));
        final Iterator<Entry<Key,Value>> i = s.iterator();

        String[] tempVarOrders = null;

        if (i.hasNext()) {
            final Entry<Key, Value> entry = i.next();
            final Text ts = entry.getKey().getColumnFamily();
            tempVarOrders = entry.getKey().getColumnQualifier().toString().split("\u0000");
            tableSize = Long.parseLong(ts.toString());

        } else {
            throw new IllegalStateException("Index table contains no metadata!");
        }
        varOrder = Lists.newArrayList();
        for(String t: tempVarOrders) {
            t = t.replace(";","\u0000");
            varOrder.add(t);
        }
        setLocalityGroups(tablename, accCon, varOrder);
        this.setSupportedVariableOrderMap(createSupportedVarOrderMap(varOrder));
    }

    @Override
    public double cardinality() {
        return tableSize;
    }

    private void setLocalityGroups(String tableName, Connector conn, List<String> groups) {

    	final HashMap<String, Set<Text>> localityGroups = new HashMap<String, Set<Text>>();
        for (int i = 0; i < groups.size(); i++) {
            final HashSet<Text> tempColumn = new HashSet<Text>();
            tempColumn.add(new Text(groups.get(i)));
            final String groupName = groups.get(i).replace("\u0000","");
            localityGroups.put(groupName, tempColumn);
        }

        try {
            conn.tableOperations().setLocalityGroups(tableName, localityGroups);
        } catch (final AccumuloException e) {
            e.printStackTrace();
        } catch (final AccumuloSecurityException e) {
            e.printStackTrace();
        } catch (final TableNotFoundException e) {
            e.printStackTrace();
        }
    }

    private Map<String, Set<String>> createSupportedVarOrderMap(List<String> orders) {
        final Map<String, Set<String>> supportedVars = Maps.newHashMap();

        for (final String t : orders) {
            final String[] tempOrder = t.split("\u0000");
            final Set<String> varSet = Sets.newHashSet();
            String u = "";
            for (final String s : tempOrder) {
                if(u.length() == 0) {
                    u = s;
                } else{
                    u = u+ "\u0000" + s;
                }
                varSet.add(s);
                supportedVars.put(u, new HashSet<String>(varSet));
            }
        }
        return supportedVars;
    }

    @Override
    public CloseableIteration<BindingSet,QueryEvaluationException> evaluate(BindingSet bindingset) throws QueryEvaluationException {
        return this.evaluate(Collections.singleton(bindingset));
    }

    @Override
    public CloseableIteration<BindingSet,QueryEvaluationException> evaluate(final Collection<BindingSet> bindingset) throws QueryEvaluationException {
        String localityGroup = "";
        final Set<String> commonVars = Sets.newHashSet();
        //to build range prefix, find common vars of bindingset and PCJ bindings
        if (!bindingset.isEmpty()) {
            final BindingSet bs = bindingset.iterator().next();
            for (final String b : bindingslist) {
                final Binding v = bs.getBinding(b);
                if (v != null) {
                    commonVars.add(b);
                }
            }
        }
        //add any constant constraints to common vars to be used in range prefix
        commonVars.addAll(getConstantConstraints());
        PcjQuery apq = null;
        List<String> fullVarOrder =  null;
        try {
            if (commonVars.size() > 0) {
                final String commonVarOrder = getVarOrder(commonVars);
                if(commonVarOrder == null) {
                    throw new IllegalStateException("Index does not support binding set!");
                }
                fullVarOrder = Lists.newArrayList(prefixToOrder(commonVarOrder).split("\u0000"));
                //use varOrder and tableVarMap to set correct scan column
                localityGroup = orderToLocGroup(fullVarOrder);
            } else {
                fullVarOrder = bindingslist;
                localityGroup = orderToLocGroup(fullVarOrder);
            }
            apq = new AccumuloPcjQuery(accCon, tablename);
            final ValueMapVisitor vmv = new ValueMapVisitor();
            this.getTupleExpr().visit(vmv);

            return apq.queryPrecompJoin(new ArrayList<String>(commonVars), localityGroup, vmv.getValMap(), bindingset);
        } catch(final TableNotFoundException e) {
            throw new QueryEvaluationException(e);
        }
    }

    //converts var order in terms of query vars into order in terms of PCJ vars
    private String orderToLocGroup(List<String> order) {
        String localityGroup = "";
        for (final String s : order) {
            if (localityGroup.length() == 0) {
                localityGroup = this.getTableVarMap().get(s);
            } else {
                localityGroup = localityGroup + "\u0000" + this.getTableVarMap().get(s);
            }
        }
        return localityGroup;
    }

    //given partial order of query vars, convert to PCJ vars and determine
    //if converted partial order is a substring of a full var order of PCJ variables.
    //if converted partial order is a prefix, convert corresponding full PCJ var order to query vars
    private String prefixToOrder(String order) {
        final Map<String, String> invMap = HashBiMap.create(this.getTableVarMap()).inverse();
        String[] temp = order.split("\u0000");
        //get order in terms of PCJ variables
        for (int i = 0; i < temp.length; i++) {
            temp[i] = this.getTableVarMap().get(temp[i]);
        }
        order = Joiner.on("\u0000").join(temp);
        for (final String s : varOrder) {
        	//verify that partial order is prefix of a PCJ varOrder
            if (s.startsWith(order)) {
                temp = s.split("\u0000");
                //convert full PCJ varOrder back to query varOrder
                for (int i = 0; i < temp.length; i++) {
                    temp[i] = invMap.get(temp[i]);
                }
                return Joiner.on("\u0000").join(temp);
            }
        }
        throw new NoSuchElementException("Order is not a prefix of any locality group value!");
    }

    private String getVarOrder(Set<String> variables) {
        final Map<String, Set<String>> varOrderMap = this.getSupportedVariableOrders();
        final Set<Map.Entry<String, Set<String>>> entries = varOrderMap.entrySet();
        for (final Map.Entry<String, Set<String>> e : entries) {

            if (e.getValue().equals(variables)) {
                return e.getKey();
            }
        }
        return null;
    }

    private Set<String> getConstantConstraints() {
        final Map<String, String> tableMap = this.getTableVarMap();
        final Set<String> keys = tableMap.keySet();
        final Set<String> constants = Sets.newHashSet();
        for (final String s : keys) {
            if (s.startsWith("-const-")) {
                constants.add(s);
            }
        }
        return constants;
    }

    public class ValueMapVisitor extends QueryModelVisitorBase<RuntimeException> {
        Map<String, org.openrdf.model.Value> valMap = Maps.newHashMap();
        public Map<String, org.openrdf.model.Value> getValMap() {
            return valMap;
        }
        @Override
        public void meet(Var node) {
            if (node.getName().startsWith("-const-")) {
                valMap.put(node.getName(), node.getValue());
            }
        }
    }

}


