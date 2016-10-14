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

//package org.apache.rya.accumulo;

//
//import com.google.common.collect.Iterators;
//import com.google.common.io.ByteArrayDataInput;
//import com.google.common.io.ByteStreams;
//import info.aduna.iteration.CloseableIteration;
//import org.apache.rya.api.RdfCloudTripleStoreConstants;
//import org.apache.rya.api.RdfCloudTripleStoreUtils;
//import org.apache.rya.api.persist.RdfDAOException;
//import org.apache.rya.api.utils.NullableStatementImpl;
//import org.apache.accumulo.core.client.*;
//import org.apache.accumulo.core.data.Key;
//import org.apache.accumulo.core.data.Range;
//import org.apache.accumulo.core.iterators.user.AgeOffFilter;
//import org.apache.accumulo.core.iterators.user.TimestampFilter;
//import org.apache.accumulo.core.security.Authorizations;
//import org.apache.hadoop.io.Text;
//import org.openrdf.model.Resource;
//import org.openrdf.model.Statement;
//import org.openrdf.model.URI;
//import org.openrdf.model.Value;
//import org.openrdf.query.BindingSet;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.IOException;
//import java.util.Collection;
//import java.util.Collections;
//import java.util.HashSet;
//import java.util.Iterator;
//import java.util.Map.Entry;
//
//import static org.apache.rya.accumulo.AccumuloRdfConstants.ALL_AUTHORIZATIONS;
//import static org.apache.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;
//import static org.apache.rya.api.RdfCloudTripleStoreUtils.writeValue;
//
//public class AccumuloRdfQueryIterator implements
//        CloseableIteration<Entry<Statement, BindingSet>, RdfDAOException> {
//
//    protected final Logger logger = LoggerFactory.getLogger(getClass());
//
//    private boolean open = false;
//    private Iterator result;
//    private Resource[] contexts;
//    private Collection<Entry<Statement, BindingSet>> statements;
//    private int numOfThreads = 20;
//
//    private RangeBindingSetEntries rangeMap = new RangeBindingSetEntries();
//    private ScannerBase scanner;
//    private boolean isBatchScanner = true;
//    private Statement statement;
//    Iterator<BindingSet> iter_bss = null;
//
//    private boolean hasNext = true;
//    private AccumuloRdfConfiguration conf;
//    private TABLE_LAYOUT tableLayout;
//    private Text context_txt;
//
//    private DefineTripleQueryRangeFactory queryRangeFactory = new DefineTripleQueryRangeFactory();
//
//    public AccumuloRdfQueryIterator(Collection<Entry<Statement, BindingSet>> statements, Connector connector, Resource... contexts)
//            throws RdfDAOException {
//        this(statements, connector, null, contexts);
//    }
//
//    public AccumuloRdfQueryIterator(Collection<Entry<Statement, BindingSet>> statements, Connector connector,
//                                    AccumuloRdfConfiguration conf, Resource... contexts)
//            throws RdfDAOException {
//        this.statements = statements;
//        this.contexts = contexts;
//        this.conf = conf;
//        initialize(connector);
//        open = true;
//    }
//
//    public AccumuloRdfQueryIterator(Resource subject, URI predicate, Value object, Connector connector,
//                                    AccumuloRdfConfiguration conf, Resource[] contexts) throws RdfDAOException {
//        this(Collections.<Entry<Statement, BindingSet>>singleton(new RdfCloudTripleStoreUtils.CustomEntry<Statement, BindingSet>(
//                new NullableStatementImpl(subject, predicate, object, contexts),
//                null)), connector, conf, contexts);
//    }
//
//    protected void initialize(Connector connector)
//            throws RdfDAOException {
//        try {
//            //TODO: We cannot span multiple tables here
//            Collection<Range> ranges = new HashSet<Range>();
//
//            result = Iterators.emptyIterator();
//            Long startTime = conf.getStartTime();
//            Long ttl = conf.getTtl();
//
//            Resource context = null;
//            for (Entry<Statement, BindingSet> stmtbs : statements) {
//                Statement stmt = stmtbs.getKey();
//                Resource subject = stmt.getSubject();
//                URI predicate = stmt.getPredicate();
//                Value object = stmt.getObject();
//                context = stmt.getContext(); //TODO: assumes the same context for all statements
//                logger.debug("Batch Scan, lookup subject[" + subject + "] predicate[" + predicate + "] object[" + object + "] combination");
//
//                Entry<TABLE_LAYOUT, Range> entry = queryRangeFactory.defineRange(subject, predicate, object, conf);
//                tableLayout = entry.getKey();
////                isTimeRange = isTimeRange || queryRangeFactory.isTimeRange();
//                Range range = entry.getValue();
//                ranges.add(range);
//                rangeMap.ranges.add(new RdfCloudTripleStoreUtils.CustomEntry<Range, BindingSet>(range, stmtbs.getValue()));
//            }
//
//            Authorizations authorizations = AccumuloRdfConstants.ALL_AUTHORIZATIONS;
//            String auth = conf.getAuth();
//            if (auth != null) {
//                authorizations = new Authorizations(auth.split(","));
//            }
//            String table = RdfCloudTripleStoreUtils.layoutToTable(tableLayout, conf);
//            result = createScanner(connector, authorizations, table, context, startTime, ttl, ranges);
////            if (isBatchScanner) {
////                ((BatchScanner) scanner).setRanges(ranges);
////            } else {
////                for (Range range : ranges) {
////                    ((Scanner) scanner).setRange(range); //TODO: Not good way of doing this
////                }
////            }
////
////            if (isBatchScanner) {
////                result = ((BatchScanner) scanner).iterator();
////            } else {
////                result = ((Scanner) scanner).iterator();
////            }
//        } catch (Exception e) {
//            throw new RdfDAOException(e);
//        }
//    }
//
//    protected Iterator<Entry<Key, org.apache.accumulo.core.data.Value>> createScanner(Connector connector, Authorizations authorizations, String table, Resource context, Long startTime, Long ttl, Collection<Range> ranges) throws TableNotFoundException, IOException {
////        ShardedConnector shardedConnector = new ShardedConnector(connector, 4, ta)
//        if (rangeMap.ranges.size() > (numOfThreads / 2)) { //TODO: Arbitrary number, make configurable
//            BatchScanner scannerBase = connector.createBatchScanner(table, authorizations, numOfThreads);
//            scannerBase.setRanges(ranges);
//            populateScanner(context, startTime, ttl, scannerBase);
//            return scannerBase.iterator();
//        } else {
//            isBatchScanner = false;
//            Iterator<Entry<Key, org.apache.accumulo.core.data.Value>>[] iters = new Iterator[ranges.size()];
//            int i = 0;
//            for (Range range : ranges) {
//                Scanner scannerBase = connector.createScanner(table, authorizations);
//                populateScanner(context, startTime, ttl, scannerBase);
//                scannerBase.setRange(range);
//                iters[i] = scannerBase.iterator();
//                i++;
//                scanner = scannerBase; //TODO: Always overridden, but doesn't matter since Scanner doesn't need to be closed
//            }
//            return Iterators.concat(iters);
//        }
//
//    }
//
//    protected void populateScanner(Resource context, Long startTime, Long ttl, ScannerBase scannerBase) throws IOException {
//        if (context != null) { //default graph
//            context_txt = new Text(writeValue(context));
//            scannerBase.fetchColumnFamily(context_txt);
//        }
//
////        if (!isQueryTimeBased(conf)) {
//        if (startTime != null && ttl != null) {
////            scannerBase.setScanIterators(1, FilteringIterator.class.getName(), "filteringIterator");
////            scannerBase.setScanIteratorOption("filteringIterator", "0", TimeRangeFilter.class.getName());
////            scannerBase.setScanIteratorOption("filteringIterator", "0." + TimeRangeFilter.TIME_RANGE_PROP, ttl);
////            scannerBase.setScanIteratorOption("filteringIterator", "0." + TimeRangeFilter.START_TIME_PROP, startTime);
//            IteratorSetting setting = new IteratorSetting(1, "fi", TimestampFilter.class.getName());
//            TimestampFilter.setStart(setting, startTime, true);
//            TimestampFilter.setEnd(setting, startTime + ttl, true);
//            scannerBase.addScanIterator(setting);
//        } else if (ttl != null) {
////                scannerBase.setScanIterators(1, FilteringIterator.class.getName(), "filteringIterator");
////                scannerBase.setScanIteratorOption("filteringIterator", "0", AgeOffFilter.class.getName());
////                scannerBase.setScanIteratorOption("filteringIterator", "0.ttl", ttl);
//            IteratorSetting setting = new IteratorSetting(1, "fi", AgeOffFilter.class.getName());
//            AgeOffFilter.setTTL(setting, ttl);
//            scannerBase.addScanIterator(setting);
//        }
////        }
//    }
//
//    @Override
//    public void close() throws RdfDAOException {
//        if (!open)
//            return;
//        verifyIsOpen();
//        open = false;
//        if (scanner != null && isBatchScanner) {
//            ((BatchScanner) scanner).close();
//        }
//    }
//
//    public void verifyIsOpen() throws RdfDAOException {
//        if (!open) {
//            throw new RdfDAOException("Iterator not open");
//        }
//    }
//
//    @Override
//    public boolean hasNext() throws RdfDAOException {
//        try {
//            if (!open)
//                return false;
//            verifyIsOpen();
//            /**
//             * For some reason, the result.hasNext returns false
//             * once at the end of the iterator, and then true
//             * for every subsequent call.
//             */
//            hasNext = (hasNext && result.hasNext());
//            return hasNext || ((iter_bss != null) && iter_bss.hasNext());
//        } catch (Exception e) {
//            throw new RdfDAOException(e);
//        }
//    }
//
//    @Override
//    public Entry<Statement, BindingSet> next() throws RdfDAOException {
//        try {
//            if (!this.hasNext())
//                return null;
//
//            return getStatement(result, contexts);
//        } catch (Exception e) {
//            throw new RdfDAOException(e);
//        }
//    }
//
//    public Entry<Statement, BindingSet> getStatement(
//            Iterator<Entry<Key, org.apache.accumulo.core.data.Value>> rowResults,
//            Resource... filterContexts) throws IOException {
//        try {
//            while (true) {
//                if (iter_bss != null && iter_bss.hasNext()) {
//                    return new RdfCloudTripleStoreUtils.CustomEntry<Statement, BindingSet>(statement, iter_bss.next());
//                }
//
//                if (rowResults.hasNext()) {
//                    Entry<Key, org.apache.accumulo.core.data.Value> entry = rowResults.next();
//                    Key key = entry.getKey();
//                    ByteArrayDataInput input = ByteStreams.newDataInput(key.getRow().getBytes());
//                    statement = RdfCloudTripleStoreUtils.translateStatementFromRow(input, key.getColumnFamily(), tableLayout, RdfCloudTripleStoreConstants.VALUE_FACTORY);
//                    iter_bss = rangeMap.containsKey(key).iterator();
//                } else
//                    break;
//            }
//        } catch (Exception e) {
//            throw new IOException(e);
//        }
//        return null;
//    }
//
//    @Override
//    public void remove() throws RdfDAOException {
//        next();
//    }
//
//    public int getNumOfThreads() {
//        return numOfThreads;
//    }
//
//    public void setNumOfThreads(int numOfThreads) {
//        this.numOfThreads = numOfThreads;
//    }
//
//    public DefineTripleQueryRangeFactory getQueryRangeFactory() {
//        return queryRangeFactory;
//    }
//
//    public void setQueryRangeFactory(DefineTripleQueryRangeFactory queryRangeFactory) {
//        this.queryRangeFactory = queryRangeFactory;
//    }
//}
