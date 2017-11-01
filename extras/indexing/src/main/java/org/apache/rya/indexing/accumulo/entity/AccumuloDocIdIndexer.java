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
package org.apache.rya.indexing.accumulo.entity;

import static org.apache.rya.api.RdfCloudTripleStoreConstants.DELIM_BYTE;
import static org.apache.rya.api.RdfCloudTripleStoreConstants.TYPE_DELIM_BYTE;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.documentIndex.DocIndexIteratorUtil;
import org.apache.rya.accumulo.documentIndex.DocumentIndexIntersectingIterator;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.resolver.RyaContext;
import org.apache.rya.api.resolver.RyaToRdfConversions;
import org.apache.rya.api.resolver.RyaTypeResolverException;
import org.apache.rya.indexing.DocIdIndexer;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.eclipse.rdf4j.query.algebra.helpers.StatementPatternCollector;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Sets;
import com.google.common.primitives.Bytes;

public class AccumuloDocIdIndexer implements DocIdIndexer {



    private BatchScanner bs;
    private final AccumuloRdfConfiguration conf;

    public AccumuloDocIdIndexer(final RdfCloudTripleStoreConfiguration conf) throws AccumuloException, AccumuloSecurityException {
        Preconditions.checkArgument(conf instanceof RdfCloudTripleStoreConfiguration, "conf must be isntance of RdfCloudTripleStoreConfiguration");
        this.conf = (AccumuloRdfConfiguration) conf;
        //Connector conn = ConfigUtils.getConnector(conf);
    }




    public CloseableIteration<BindingSet, QueryEvaluationException> queryDocIndex(final String sparqlQuery,
            final Collection<BindingSet> constraints) throws TableNotFoundException, QueryEvaluationException {

        final SPARQLParser parser = new SPARQLParser();
        ParsedQuery pq1 = null;
        try {
            pq1 = parser.parseQuery(sparqlQuery, null);
        } catch (final MalformedQueryException e) {
            e.printStackTrace();
        }

        final TupleExpr te1 = pq1.getTupleExpr();
        final List<StatementPattern> spList1 = StatementPatternCollector.process(te1);

        if(StarQuery.isValidStarQuery(spList1)) {
            final StarQuery sq1 = new StarQuery(spList1);
            return queryDocIndex(sq1, constraints);
        } else {
            throw new IllegalArgumentException("Invalid star query!");
        }

    }




    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> queryDocIndex(final StarQuery query,
            final Collection<BindingSet> constraints) throws TableNotFoundException, QueryEvaluationException {

        final StarQuery starQ = query;
        final Iterator<BindingSet> bs = constraints.iterator();
        final Iterator<BindingSet> bs2 = constraints.iterator();
        final Set<String> unCommonVarNames;
        final Set<String> commonVarNames;
        if (bs2.hasNext()) {
            final BindingSet currBs = bs2.next();
            commonVarNames = StarQuery.getCommonVars(query, currBs);
            unCommonVarNames = Sets.difference(currBs.getBindingNames(), commonVarNames);
        } else {
            commonVarNames = Sets.newHashSet();
            unCommonVarNames = Sets.newHashSet();
        }

        if( commonVarNames.size() == 1 && !query.commonVarConstant() && commonVarNames.contains(query.getCommonVarName())) {

            final HashMultimap<String, BindingSet> map = HashMultimap.create();
            final String commonVar = starQ.getCommonVarName();
            final Iterator<Entry<Key, Value>> intersections;
            final BatchScanner scan;
            final Set<Range> ranges = Sets.newHashSet();

            while(bs.hasNext()) {

                final BindingSet currentBs = bs.next();

                if(currentBs.getBinding(commonVar) == null) {
                    continue;
                }

                final String row = currentBs.getBinding(commonVar).getValue().stringValue();
                ranges.add(new Range(row));
                map.put(row, currentBs);

            }
            scan = runQuery(starQ, ranges);
            intersections = scan.iterator();


            return new CloseableIteration<BindingSet, QueryEvaluationException>() {


                private QueryBindingSet currentSolutionBs = null;
                private boolean hasNextCalled = false;
                private boolean isEmpty = false;
                private Iterator<BindingSet> inputSet = new ArrayList<BindingSet>().iterator();
                private BindingSet currentBs;
                private Key key;



                @Override
                public boolean hasNext() throws QueryEvaluationException {
                    if (!hasNextCalled && !isEmpty) {
                        while (inputSet.hasNext() || intersections.hasNext()) {
                            if (!inputSet.hasNext()) {
                                key = intersections.next().getKey();
                                inputSet = map.get(key.getRow().toString()).iterator();
                            }
                            currentBs = inputSet.next();
                            currentSolutionBs = deserializeKey(key, starQ, currentBs, unCommonVarNames);

                            if (currentSolutionBs.size() == unCommonVarNames.size() + starQ.getUnCommonVars().size() +1) {
                                hasNextCalled = true;
                                return true;
                            }

                        }

                        isEmpty = true;
                        return false;

                    } else
                        return !isEmpty;

                }


                @Override
                public BindingSet next() throws QueryEvaluationException {

                    if (hasNextCalled) {
                        hasNextCalled = false;
                    } else if (isEmpty) {
                        throw new NoSuchElementException();
                    } else {
                        if (this.hasNext()) {
                            hasNextCalled = false;
                        } else {
                            throw new NoSuchElementException();
                        }
                    }

                    return currentSolutionBs;
                }

                @Override
                public void remove() throws QueryEvaluationException {
                    throw new UnsupportedOperationException();
                }

                @Override
                public void close() throws QueryEvaluationException {
                    scan.close();
                }

            };


        } else {

            return new CloseableIteration<BindingSet, QueryEvaluationException>() {

                @Override
                public void remove() throws QueryEvaluationException {
                    throw new UnsupportedOperationException();
                }

                private Iterator<Entry<Key, Value>> intersections = null;
                private QueryBindingSet currentSolutionBs = null;
                private boolean hasNextCalled = false;
                private boolean isEmpty = false;
                private boolean init = false;
                private BindingSet currentBs;
                private StarQuery sq = new StarQuery(starQ);
                private final Set<Range> emptyRangeSet = Sets.newHashSet();
                private BatchScanner scan;

                @Override
                public BindingSet next() throws QueryEvaluationException {
                    if (hasNextCalled) {
                        hasNextCalled = false;
                    } else if (isEmpty) {
                        throw new NoSuchElementException();
                    } else {
                        if (this.hasNext()) {
                            hasNextCalled = false;
                        } else {
                            throw new NoSuchElementException();
                        }
                    }
                    return currentSolutionBs;
                }

                @Override
                public boolean hasNext() throws QueryEvaluationException {

                    if (!init) {
                        if (intersections == null && bs.hasNext()) {
                            currentBs = bs.next();
                            sq = StarQuery.getConstrainedStarQuery(sq, currentBs);
                            scan = runQuery(sq,emptyRangeSet);
                            intersections = scan.iterator();
                            // binding set empty
                        } else if (intersections == null && !bs.hasNext()) {
                            currentBs = new QueryBindingSet();
                            scan = runQuery(starQ,emptyRangeSet);
                            intersections = scan.iterator();
                        }

                        init = true;
                    }

                    if (!hasNextCalled && !isEmpty) {
                        while (intersections.hasNext() || bs.hasNext()) {
                            if (!intersections.hasNext()) {
                                scan.close();
                                currentBs = bs.next();
                                sq = StarQuery.getConstrainedStarQuery(sq, currentBs);
                                scan = runQuery(sq,emptyRangeSet);
                                intersections = scan.iterator();
                            }
                            if (intersections.hasNext()) {
                                currentSolutionBs = deserializeKey(intersections.next().getKey(), sq, currentBs,
                                        unCommonVarNames);
                            } else {
                                continue;
                            }

                            if (sq.commonVarConstant() && currentSolutionBs.size() == unCommonVarNames.size() + sq.getUnCommonVars().size()) {
                                hasNextCalled = true;
                                return true;
                            } else if(currentSolutionBs.size() == unCommonVarNames.size() + sq.getUnCommonVars().size() + 1) {
                                hasNextCalled = true;
                                return true;
                            }
                        }

                        isEmpty = true;
                        return false;

                    } else
                        return !isEmpty;
                }

                @Override
                public void close() throws QueryEvaluationException {
                    scan.close();
                }
            };
        }
    }

    private QueryBindingSet deserializeKey(final Key key, final StarQuery sq, final BindingSet currentBs, final Set<String> unCommonVar) {


        final QueryBindingSet currentSolutionBs = new QueryBindingSet();

        final Text row = key.getRow();
        final Text cq = key.getColumnQualifier();


        final String[] cqArray = cq.toString().split(DocIndexIteratorUtil.DOC_ID_INDEX_DELIM);

        boolean commonVarSet = false;

        //if common Var is constant there is no common variable to assign a value to
        if(sq.commonVarConstant()) {
            commonVarSet = true;
        }

        if (!commonVarSet && sq.isCommonVarURI()) {
            final RyaURI rURI = new RyaURI(row.toString());
            currentSolutionBs.addBinding(sq.getCommonVarName(),
                    RyaToRdfConversions.convertValue(rURI));
            commonVarSet = true;
        }

        for (final String s : sq.getUnCommonVars()) {

            final byte[] cqBytes = cqArray[sq.getVarPos().get(s)].getBytes(StandardCharsets.UTF_8);
            final int firstIndex = Bytes.indexOf(cqBytes, DELIM_BYTE);
            final int secondIndex = Bytes.lastIndexOf(cqBytes, DELIM_BYTE);
            final int typeIndex = Bytes.indexOf(cqBytes, TYPE_DELIM_BYTE);
            final String tripleComponent = new String(Arrays.copyOfRange(cqBytes, firstIndex + 1, secondIndex), StandardCharsets.UTF_8);
            final byte[] cqContent = Arrays.copyOfRange(cqBytes, secondIndex + 1, typeIndex);
            final byte[] objType = Arrays.copyOfRange(cqBytes, typeIndex, cqBytes.length);

            if (tripleComponent.equals("object")) {
                final byte[] object = Bytes.concat(cqContent, objType);
                org.eclipse.rdf4j.model.Value v = null;
                try {
                    v = RyaToRdfConversions.convertValue(RyaContext.getInstance().deserialize(
                            object));
                } catch (final RyaTypeResolverException e) {
                    e.printStackTrace();
                }
                currentSolutionBs.addBinding(s, v);

            } else if (tripleComponent.equals("subject")) {
                if (!commonVarSet) {
                    final byte[] object = Bytes.concat(row.getBytes(), objType);
                    org.eclipse.rdf4j.model.Value v = null;
                    try {
                        v = RyaToRdfConversions.convertValue(RyaContext.getInstance().deserialize(
                                object));
                    } catch (final RyaTypeResolverException e) {
                        e.printStackTrace();
                    }
                    currentSolutionBs.addBinding(sq.getCommonVarName(), v);
                    commonVarSet = true;
                }
                final RyaURI rURI = new RyaURI(new String(cqContent, StandardCharsets.UTF_8));
                currentSolutionBs.addBinding(s, RyaToRdfConversions.convertValue(rURI));
            } else {
                throw new IllegalArgumentException("Invalid row.");
            }
        }
        for (final String s : unCommonVar) {
            currentSolutionBs.addBinding(s, currentBs.getValue(s));
        }
        return currentSolutionBs;
    }

    private BatchScanner runQuery(final StarQuery query, Collection<Range> ranges) throws QueryEvaluationException {

        try {
            if (ranges.size() == 0) {
                final String rangeText = query.getCommonVarValue();
                Range r;
                if (rangeText != null) {
                    r = new Range(new Text(query.getCommonVarValue()));
                } else {
                    r = new Range();
                }
                ranges = Collections.singleton(r);
            }

            final Connector accCon = ConfigUtils.getConnector(conf);
            final IteratorSetting is = new IteratorSetting(30, "fii", DocumentIndexIntersectingIterator.class);

            DocumentIndexIntersectingIterator.setColumnFamilies(is, query.getColumnCond());

            if (query.hasContext()) {
                DocumentIndexIntersectingIterator.setContext(is, query.getContextURI());
            }

            final Authorizations auths;
            final String authsStr = conf.get(ConfigUtils.CLOUDBASE_AUTHS);
            if(authsStr == null || authsStr.isEmpty()) {
                auths = new Authorizations();
            } else {
                auths = new Authorizations(authsStr);
            }

            bs = accCon.createBatchScanner(EntityCentricIndex.getTableName(conf), auths, 15);
            bs.addScanIterator(is);
            bs.setRanges(ranges);

            return bs;

        } catch (TableNotFoundException | AccumuloException | AccumuloSecurityException e) {
            throw new QueryEvaluationException(e);
        }
    }

    @Override
    public void close() throws IOException {
        //TODO generate an exception when BS passed in -- scanner closed
//        if (bs != null) {
//            bs.close();
//        }
    }

}
