/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.indexing.geotemporal;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.rya.indexing.IndexingExpr;
import org.apache.rya.indexing.IndexingFunctionRegistry;
import org.apache.rya.indexing.IndexingFunctionRegistry.FUNCTION_TYPE;
import org.apache.rya.indexing.accumulo.geo.GeoTupleSet;
import org.apache.rya.indexing.external.matching.ExternalSetProvider;
import org.apache.rya.indexing.external.matching.QuerySegment;
import org.apache.rya.indexing.geotemporal.model.EventQueryNode;
import org.apache.rya.indexing.geotemporal.storage.EventStorage;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.algebra.FunctionCall;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.ValueConstant;
import org.openrdf.query.algebra.ValueExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * Provides {@link GeoTupleSet}s.
 */
public class GeoTemporalIndexSetProvider implements ExternalSetProvider<EventQueryNode> {
    //organzied by object var.  Each object is a filter, or set of filters
    private Multimap<Var, IndexingExpr> filterMap;

    //organzied by subject var.  Each subject is a GeoTemporalTupleSet
    private Multimap<Var, StatementPattern> patternMap;

    //filters that have not been constrained by statement patterns into indexing expressions yet.
    private Multimap<Var, FunctionCall> unmatchedFilters;
    //filters that have been used, to be used by the matcher later.
    private Multimap<Var, FunctionCall> matchedFilters;

    //organzied by object var.  Used to find matches between unmatch filters and patterns
    private Map<Var, StatementPattern> objectPatterns;


    private static URI filterURI;

    private final EventStorage eventStorage;

    public GeoTemporalIndexSetProvider(final EventStorage eventStorage) {
        this.eventStorage = requireNonNull(eventStorage);
    }

    @Override
    public List<EventQueryNode> getExternalSets(final QuerySegment<EventQueryNode> node) {
        filterMap = HashMultimap.create();
        patternMap = HashMultimap.create();
        unmatchedFilters = HashMultimap.create();
        matchedFilters = HashMultimap.create();

        objectPatterns = new HashMap<>();
        //discover entities
        buildMaps(node);
        final List<EventQueryNode> nodes = createNodes();

        return nodes;
    }

    private List<EventQueryNode> createNodes() {
        final List<EventQueryNode> nodes = new ArrayList<>();
        for(final Var subj : patternMap.keySet()) {
            final EventQueryNode node = getGeoTemporalNode(subj);
            if(node != null) {
                nodes.add(node);
            }
        }
        return nodes;
    }

    private EventQueryNode getGeoTemporalNode(final Var subj) {
        final Collection<StatementPattern> patterns = patternMap.get(subj);
        final Collection<FunctionCall> usedFilters = new ArrayList<>();
        Optional<StatementPattern> geoPattern = Optional.empty();
        Optional<StatementPattern> temporalPattern = Optional.empty();
        Optional<Collection<IndexingExpr>> geoFilters = Optional.empty();
        Optional<Collection<IndexingExpr>> temporalFilters = Optional.empty();

        //should only be 2 patterns.
        for(final StatementPattern sp : patterns) {
            final Var obj = sp.getObjectVar();

            ///filter map does not have -const-


            if(filterMap.containsKey(obj)) {
                final Collection<IndexingExpr> filters = filterMap.get(obj);
                final IndexingFunctionRegistry.FUNCTION_TYPE type = ensureSameType(filters);
                if(type != null && type == FUNCTION_TYPE.GEO) {
                    geoPattern = Optional.of(sp);
                    geoFilters = Optional.of(filters);
                    usedFilters.addAll(matchedFilters.get(obj));
                } else if(type != null && type == FUNCTION_TYPE.TEMPORAL) {
                    temporalPattern = Optional.of(sp);
                    temporalFilters = Optional.of(filters);
                    usedFilters.addAll(matchedFilters.get(obj));
                } else {
                    return null;
                }
            } else {
                return null;
            }
        }

        if(geoFilters.isPresent() && temporalFilters.isPresent() && geoPattern.isPresent() && temporalPattern.isPresent()) {
            return new EventQueryNode(eventStorage, geoPattern.get(), temporalPattern.get(), geoFilters.get(), temporalFilters.get(), usedFilters);
        } else {
            return null;
        }
    }

    private FUNCTION_TYPE ensureSameType(final Collection<IndexingExpr> filters) {
        FUNCTION_TYPE type = null;
        for(final IndexingExpr filter : filters) {
            if(type == null) {
                type = IndexingFunctionRegistry.getFunctionType(filter.getFunction());
            } else {
                if(IndexingFunctionRegistry.getFunctionType(filter.getFunction()) != type) {
                    return null;
                }
            }
        }
        return type;
    }

    private void buildMaps(final QuerySegment<EventQueryNode> node) {
        final List<QueryModelNode> unused = new ArrayList<>();
        for (final QueryModelNode pattern : node.getOrderedNodes()) {
            if(pattern instanceof FunctionCall) {
                discoverFilter((FunctionCall) pattern, unused);
            }
            if(pattern instanceof StatementPattern) {
                discoverPatterns((StatementPattern) pattern, unused);
            }
        }
    }

    private void discoverFilter(final FunctionCall filter, final List<QueryModelNode> unmatched) {
        try {
            filter.visit(new FilterVisitor());
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }

    private void discoverPatterns(final StatementPattern pattern, final List<QueryModelNode> unmatched) {
        final Var subj = pattern.getSubjectVar();
        final Var objVar = pattern.getObjectVar();

        patternMap.put(subj, pattern);
        objectPatterns.put(objVar, pattern);
        //check for existing filters.
        if(unmatchedFilters.containsKey(objVar)) {
            final Collection<FunctionCall> calls = unmatchedFilters.removeAll(objVar);
            for(final FunctionCall call : calls) {
                addFilter(call);
                matchedFilters.put(objVar, call);
            }
        }
    }

    @Override
    public Iterator<List<EventQueryNode>> getExternalSetCombos(final QuerySegment<EventQueryNode> segment) {
        final List<List<EventQueryNode>> comboList = new ArrayList<>();
        comboList.add(getExternalSets(segment));
        return comboList.iterator();
    }

    private void addFilter(final FunctionCall call) {
        filterURI = new URIImpl(call.getURI());
        final Var objVar = IndexingFunctionRegistry.getResultVarFromFunctionCall(filterURI, call.getArgs());
        filterMap.put(objVar, new IndexingExpr(filterURI, objectPatterns.get(objVar), extractArguments(objVar.getName(), call)));
    }

    private Value[] extractArguments(final String matchName, final FunctionCall call) {
        final Value args[] = new Value[call.getArgs().size() - 1];
        int argI = 0;
        for (int i = 0; i != call.getArgs().size(); ++i) {
            final ValueExpr arg = call.getArgs().get(i);
            if (argI == i && arg instanceof Var && matchName.equals(((Var)arg).getName())) {
                continue;
            }
            if (arg instanceof ValueConstant) {
                args[argI] = ((ValueConstant)arg).getValue();
            } else if (arg instanceof Var && ((Var)arg).hasValue()) {
                args[argI] = ((Var)arg).getValue();
            } else {
                throw new IllegalArgumentException("Query error: Found " + arg + ", expected a Literal, BNode or URI");
            }
            ++argI;
        }
        return args;
    }

    /**
     * Finds the object/function in a Filter.  If the associated statement pattern
     * has been found, creates the {@link IndexingExpr} and adds it to the map.
     */
    private class FilterVisitor extends QueryModelVisitorBase<Exception> {
        @Override
        public void meet(final FunctionCall call) throws Exception {

            filterURI = new URIImpl(call.getURI());
            final FUNCTION_TYPE type = IndexingFunctionRegistry.getFunctionType(filterURI);
            if(type == FUNCTION_TYPE.GEO || type == FUNCTION_TYPE.TEMPORAL) {
                final Var objVar = IndexingFunctionRegistry.getResultVarFromFunctionCall(filterURI, call.getArgs());
                if(objectPatterns.containsKey(objVar)) {
                    filterMap.put(objVar, new IndexingExpr(filterURI, objectPatterns.get(objVar), extractArguments(objVar.getName(), call)));
                    matchedFilters.put(objVar, call);
                } else {
                    unmatchedFilters.put(objVar, call);
                }
            }
        }
    }
}
