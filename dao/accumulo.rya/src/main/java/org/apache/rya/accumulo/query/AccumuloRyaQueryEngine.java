package org.apache.rya.accumulo.query;

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



import static org.apache.rya.api.RdfCloudTripleStoreUtils.layoutToTable;
import info.aduna.iteration.CloseableIteration;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.RdfCloudTripleStoreConstants;
import org.apache.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;
import org.apache.rya.api.RdfCloudTripleStoreUtils;
import org.apache.rya.api.domain.RyaRange;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.layout.TableLayoutStrategy;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.api.persist.query.BatchRyaQuery;
import org.apache.rya.api.persist.query.RyaQuery;
import org.apache.rya.api.persist.query.RyaQueryEngine;
import org.apache.rya.api.query.strategy.ByteRange;
import org.apache.rya.api.query.strategy.TriplePatternStrategy;
import org.apache.rya.api.resolver.RyaContext;
import org.apache.rya.api.resolver.RyaTripleContext;
import org.apache.rya.api.resolver.triple.TripleRowRegex;
import org.apache.rya.api.utils.CloseableIterableIteration;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.iterators.user.TimestampFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.collect.CloseableIterables;
import org.calrissian.mango.collect.FluentCloseableIterable;
import org.openrdf.query.BindingSet;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterators;

/**
 * Date: 7/17/12
 * Time: 9:28 AM
 */
public class AccumuloRyaQueryEngine implements RyaQueryEngine<AccumuloRdfConfiguration> {

    private AccumuloRdfConfiguration configuration;
    private Connector connector;
    private RyaTripleContext ryaContext;
    private final Map<TABLE_LAYOUT, KeyValueToRyaStatementFunction> keyValueToRyaStatementFunctionMap = new HashMap<TABLE_LAYOUT, KeyValueToRyaStatementFunction>();

    public AccumuloRyaQueryEngine(Connector connector) {
        this(connector, new AccumuloRdfConfiguration());
    }

    public AccumuloRyaQueryEngine(Connector connector, AccumuloRdfConfiguration conf) {
        this.connector = connector;
        this.configuration = conf;
        ryaContext = RyaTripleContext.getInstance(conf);
        keyValueToRyaStatementFunctionMap.put(TABLE_LAYOUT.SPO, new KeyValueToRyaStatementFunction(TABLE_LAYOUT.SPO, ryaContext));
        keyValueToRyaStatementFunctionMap.put(TABLE_LAYOUT.PO, new KeyValueToRyaStatementFunction(TABLE_LAYOUT.PO, ryaContext));
        keyValueToRyaStatementFunctionMap.put(TABLE_LAYOUT.OSP, new KeyValueToRyaStatementFunction(TABLE_LAYOUT.OSP, ryaContext));
    }

    @Override
    public CloseableIteration<RyaStatement, RyaDAOException> query(RyaStatement stmt, AccumuloRdfConfiguration conf) throws RyaDAOException {
        if (conf == null) {
            conf = configuration;
        }

        RyaQuery ryaQuery = RyaQuery.builder(stmt).load(conf).build();
        CloseableIterable<RyaStatement> results = query(ryaQuery);

        return new CloseableIterableIteration<RyaStatement, RyaDAOException>(results);
    }

    protected String getData(RyaType ryaType) {
        return (ryaType != null) ? (ryaType.getData()) : (null);
    }

    @Override
    public CloseableIteration<? extends Map.Entry<RyaStatement, BindingSet>, RyaDAOException> queryWithBindingSet(Collection<Map.Entry<RyaStatement, BindingSet>> stmts, AccumuloRdfConfiguration conf) throws RyaDAOException {
        if (conf == null) {
            conf = configuration;
        }
        //query configuration
        Authorizations authorizations = conf.getAuthorizations();
        Long ttl = conf.getTtl();
        Long maxResults = conf.getLimit();
        Integer maxRanges = conf.getMaxRangesForScanner();
        Integer numThreads = conf.getNumThreads();

        //TODO: cannot span multiple tables here
        try {
            Collection<Range> ranges = new HashSet<Range>();
            RangeBindingSetEntries rangeMap = new RangeBindingSetEntries();
            TABLE_LAYOUT layout = null;
            RyaURI context = null;
            TriplePatternStrategy strategy = null;
            for (Map.Entry<RyaStatement, BindingSet> stmtbs : stmts) {
                RyaStatement stmt = stmtbs.getKey();
                context = stmt.getContext(); //TODO: This will be overwritten
                BindingSet bs = stmtbs.getValue();
                strategy = ryaContext.retrieveStrategy(stmt);
                if (strategy == null) {
                    throw new IllegalArgumentException("TriplePattern[" + stmt + "] not supported");
                }

                Map.Entry<RdfCloudTripleStoreConstants.TABLE_LAYOUT, ByteRange> entry =
                        strategy.defineRange(stmt.getSubject(), stmt.getPredicate(), stmt.getObject(), stmt.getContext(), conf);

                //use range to set scanner
                //populate scanner based on authorizations, ttl
                layout = entry.getKey();
                ByteRange byteRange = entry.getValue();
                Range range = new Range(new Text(byteRange.getStart()), new Text(byteRange.getEnd()));
                ranges.add(range);
                rangeMap.ranges.add(new RdfCloudTripleStoreUtils.CustomEntry<Range, BindingSet>(range, bs));
            }
            //no ranges
            if (layout == null) return null;
            String regexSubject = conf.getRegexSubject();
            String regexPredicate = conf.getRegexPredicate();
            String regexObject = conf.getRegexObject();
            TripleRowRegex tripleRowRegex = strategy.buildRegex(regexSubject, regexPredicate, regexObject, null, null);

            String table = layoutToTable(layout, conf);
            boolean useBatchScanner = ranges.size() > maxRanges;
            RyaStatementBindingSetKeyValueIterator iterator = null;
            if (useBatchScanner) {
                ScannerBase scanner = connector.createBatchScanner(table, authorizations, numThreads);
                ((BatchScanner) scanner).setRanges(ranges);
                fillScanner(scanner, context, null, ttl, null, tripleRowRegex, conf);
                iterator = new RyaStatementBindingSetKeyValueIterator(layout, ryaContext, scanner, rangeMap);
            } else {
                Scanner scannerBase = null;
                Iterator<Map.Entry<Key, Value>>[] iters = new Iterator[ranges.size()];
                int i = 0;
                for (Range range : ranges) {
                    scannerBase = connector.createScanner(table, authorizations);
                    scannerBase.setRange(range);
                    fillScanner(scannerBase, context, null, ttl, null, tripleRowRegex, conf);
                    iters[i] = scannerBase.iterator();
                    i++;
                }
                iterator = new RyaStatementBindingSetKeyValueIterator(layout, Iterators.concat(iters), rangeMap, ryaContext);
            }
            if (maxResults != null) {
                iterator.setMaxResults(maxResults);
            }
            return iterator;
        } catch (Exception e) {
            throw new RyaDAOException(e);
        }

    }

    @Override
    public CloseableIteration<RyaStatement, RyaDAOException> batchQuery(Collection<RyaStatement> stmts, AccumuloRdfConfiguration conf)
            throws RyaDAOException {
        if (conf == null) {
            conf = configuration;
        }

        BatchRyaQuery batchRyaQuery = BatchRyaQuery.builder(stmts).load(conf).build();
        CloseableIterable<RyaStatement> results = query(batchRyaQuery);

        return new CloseableIterableIteration<RyaStatement, RyaDAOException>(results);
    }

    @Override
    public CloseableIterable<RyaStatement> query(RyaQuery ryaQuery) throws RyaDAOException {
        Preconditions.checkNotNull(ryaQuery);
        RyaStatement stmt = ryaQuery.getQuery();
        Preconditions.checkNotNull(stmt);

        //query configuration
        String[] auths = ryaQuery.getAuths();
        Authorizations authorizations = auths != null ? new Authorizations(auths) : configuration.getAuthorizations();
        Long ttl = ryaQuery.getTtl();
        Long currentTime = ryaQuery.getCurrentTime();
        Long maxResults = ryaQuery.getMaxResults();
        Integer batchSize = ryaQuery.getBatchSize();
        String regexSubject = ryaQuery.getRegexSubject();
        String regexPredicate = ryaQuery.getRegexPredicate();
        String regexObject = ryaQuery.getRegexObject();
        TableLayoutStrategy tableLayoutStrategy = configuration.getTableLayoutStrategy();

        try {
            //find triple pattern range
            TriplePatternStrategy strategy = ryaContext.retrieveStrategy(stmt);
            TABLE_LAYOUT layout;
            Range range;
            RyaURI subject = stmt.getSubject();
            RyaURI predicate = stmt.getPredicate();
            RyaType object = stmt.getObject();
            RyaURI context = stmt.getContext();
            String qualifier = stmt.getQualifer();
            TripleRowRegex tripleRowRegex = null;
            if (strategy != null) {
                //otherwise, full table scan is supported
                Map.Entry<RdfCloudTripleStoreConstants.TABLE_LAYOUT, ByteRange> entry =
                        strategy.defineRange(subject, predicate, object, context, null);
                layout = entry.getKey();
                ByteRange byteRange = entry.getValue();
                range = new Range(new Text(byteRange.getStart()), new Text(byteRange.getEnd()));

            } else {
                range = new Range();
                layout = TABLE_LAYOUT.SPO;
                strategy = ryaContext.retrieveStrategy(layout);
            }

            byte[] objectTypeInfo = null;
            if (object != null) {
                //TODO: Not good to serialize this twice
                if (object instanceof RyaRange) {
                    objectTypeInfo = RyaContext.getInstance().serializeType(((RyaRange) object).getStart())[1];
                } else {
                    objectTypeInfo = RyaContext.getInstance().serializeType(object)[1];
                }
            }

            tripleRowRegex = strategy.buildRegex(regexSubject, regexPredicate, regexObject, null, objectTypeInfo);

            //use range to set scanner
            //populate scanner based on authorizations, ttl
            String table = layoutToTable(layout, tableLayoutStrategy);
            Scanner scanner = connector.createScanner(table, authorizations);
            scanner.setRange(range);
            if (batchSize != null) {
                scanner.setBatchSize(batchSize);
            }
            fillScanner(scanner, context, qualifier, ttl, currentTime, tripleRowRegex, ryaQuery.getConf());

            FluentCloseableIterable<RyaStatement> results = FluentCloseableIterable.from(new ScannerBaseCloseableIterable(scanner))
                    .transform(keyValueToRyaStatementFunctionMap.get(layout));
            if (maxResults != null) {
                results = results.limit(maxResults.intValue());
            }

            return results;
        } catch (Exception e) {
            throw new RyaDAOException(e);
        }
    }

    @Override
    public CloseableIterable<RyaStatement> query(BatchRyaQuery ryaQuery) throws RyaDAOException {
        Preconditions.checkNotNull(ryaQuery);
        Iterable<RyaStatement> stmts = ryaQuery.getQueries();
        Preconditions.checkNotNull(stmts);

        //query configuration
        String[] auths = ryaQuery.getAuths();
        final Authorizations authorizations = auths != null ? new Authorizations(auths) : configuration.getAuthorizations();
        final Long ttl = ryaQuery.getTtl();
        Long currentTime = ryaQuery.getCurrentTime();
        Long maxResults = ryaQuery.getMaxResults();
        Integer batchSize = ryaQuery.getBatchSize();
        Integer numQueryThreads = ryaQuery.getNumQueryThreads();
        String regexSubject = ryaQuery.getRegexSubject();
        String regexPredicate = ryaQuery.getRegexPredicate();
        String regexObject = ryaQuery.getRegexObject();
        TableLayoutStrategy tableLayoutStrategy = configuration.getTableLayoutStrategy();
        int maxRanges = ryaQuery.getMaxRanges();

        //TODO: cannot span multiple tables here
        try {
            Collection<Range> ranges = new HashSet<Range>();
            TABLE_LAYOUT layout = null;
            RyaURI context = null;
            TriplePatternStrategy strategy = null;
            for (RyaStatement stmt : stmts) {
                context = stmt.getContext(); //TODO: This will be overwritten
                strategy = ryaContext.retrieveStrategy(stmt);
                if (strategy == null) {
                    throw new IllegalArgumentException("TriplePattern[" + stmt + "] not supported");
                }

                Map.Entry<RdfCloudTripleStoreConstants.TABLE_LAYOUT, ByteRange> entry =
                        strategy.defineRange(stmt.getSubject(), stmt.getPredicate(), stmt.getObject(), stmt.getContext(), null);

                //use range to set scanner
                //populate scanner based on authorizations, ttl
                layout = entry.getKey();
                ByteRange byteRange = entry.getValue();
                Range range = new Range(new Text(byteRange.getStart()), new Text(byteRange.getEnd()));
                ranges.add(range);
            }
            //no ranges
            if (layout == null) throw new IllegalArgumentException("No table layout specified");

            final TripleRowRegex tripleRowRegex = strategy.buildRegex(regexSubject, regexPredicate, regexObject, null, null);

            final String table = layoutToTable(layout, tableLayoutStrategy);
            boolean useBatchScanner = ranges.size() > maxRanges;
            FluentCloseableIterable<RyaStatement> results = null;
            if (useBatchScanner) {
                BatchScanner scanner = connector.createBatchScanner(table, authorizations, numQueryThreads);
                scanner.setRanges(ranges);
                fillScanner(scanner, context, null, ttl, null, tripleRowRegex, ryaQuery.getConf());
                results = FluentCloseableIterable.from(new ScannerBaseCloseableIterable(scanner)).transform(keyValueToRyaStatementFunctionMap.get(layout));
            } else {
                final RyaURI fcontext = context;
                final RdfCloudTripleStoreConfiguration fconf = ryaQuery.getConf();
                FluentIterable<RyaStatement> fluent = FluentIterable.from(ranges).transformAndConcat(new Function<Range, Iterable<Map.Entry<Key, Value>>>() {
                    @Override
                    public Iterable<Map.Entry<Key, Value>> apply(Range range) {
                        try {
                            Scanner scanner = connector.createScanner(table, authorizations);
                            scanner.setRange(range);
                            fillScanner(scanner, fcontext, null, ttl, null, tripleRowRegex, fconf);
                            return scanner;
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                }).transform(keyValueToRyaStatementFunctionMap.get(layout));

                results = FluentCloseableIterable.from(CloseableIterables.wrap(fluent));
            }
            if (maxResults != null) {
                results = results.limit(maxResults.intValue());
            }
            return results;
        } catch (Exception e) {
            throw new RyaDAOException(e);
        }
    }

    protected void fillScanner(ScannerBase scanner, RyaURI context, String qualifier, Long ttl, Long currentTime, TripleRowRegex tripleRowRegex, RdfCloudTripleStoreConfiguration conf) throws IOException {
        if (context != null && qualifier != null) {
            scanner.fetchColumn(new Text(context.getData()), new Text(qualifier));
        } else if (context != null) {
            scanner.fetchColumnFamily(new Text(context.getData()));
        } else if (qualifier != null) {
            IteratorSetting setting = new IteratorSetting(8, "riq", RegExFilter.class.getName());
            RegExFilter.setRegexs(setting, null, null, qualifier, null, false);
            scanner.addScanIterator(setting);
        }
        if (ttl != null) {
            IteratorSetting setting = new IteratorSetting(9, "fi", TimestampFilter.class.getName());
            TimestampFilter.setStart(setting,  System.currentTimeMillis() - ttl, true);
            if(currentTime != null){
                TimestampFilter.setStart(setting, currentTime - ttl, true);
                TimestampFilter.setEnd(setting, currentTime, true);
            }
            scanner.addScanIterator(setting);
        }
        if (tripleRowRegex != null) {
            IteratorSetting setting = new IteratorSetting(11, "ri", RegExFilter.class.getName());
            String regex = tripleRowRegex.getRow();
            RegExFilter.setRegexs(setting, regex, null, null, null, false);
            scanner.addScanIterator(setting);
        }
        if (conf instanceof AccumuloRdfConfiguration) {
            //TODO should we take the iterator settings as is or should we adjust the priority based on the above?
            for (IteratorSetting itr : ((AccumuloRdfConfiguration)conf).getAdditionalIterators()) {
                scanner.addScanIterator(itr);
            }
        }
    }

    @Override
    public void setConf(AccumuloRdfConfiguration conf) {
        this.configuration = conf;
    }

    @Override
    public AccumuloRdfConfiguration getConf() {
        return configuration;
    }
}
