package mvm.rya.cloudbase.query;

import cloudbase.core.client.BatchScanner;
import cloudbase.core.client.Connector;
import cloudbase.core.client.Scanner;
import cloudbase.core.client.ScannerBase;
import cloudbase.core.data.Key;
import cloudbase.core.data.Range;
import cloudbase.core.data.Value;
import cloudbase.core.iterators.FilteringIterator;
import cloudbase.core.iterators.RegExIterator;
import cloudbase.core.iterators.filter.AgeOffFilter;
import cloudbase.core.iterators.filter.RegExFilter;
import cloudbase.core.security.Authorizations;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterators;
import mango.collect.CloseableIterable;
import mango.collect.CloseableIterables;
import mango.collect.FluentCloseableIterable;
import info.aduna.iteration.CloseableIteration;
import mvm.rya.api.RdfCloudTripleStoreConstants;
import mvm.rya.api.RdfCloudTripleStoreUtils;
import mvm.rya.api.domain.RyaRange;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.domain.RyaType;
import mvm.rya.api.domain.RyaURI;
import mvm.rya.api.layout.TableLayoutStrategy;
import mvm.rya.api.persist.RyaDAOException;
import mvm.rya.api.persist.query.BatchRyaQuery;
import mvm.rya.api.persist.query.RyaQuery;
import mvm.rya.api.persist.query.RyaQueryEngine;
import mvm.rya.api.query.strategy.ByteRange;
import mvm.rya.api.query.strategy.TriplePatternStrategy;
import mvm.rya.api.resolver.RyaContext;
import mvm.rya.api.resolver.triple.TripleRowRegex;
import mvm.rya.api.utils.CloseableIterableIteration;
import mvm.rya.cloudbase.CloudbaseRdfConfiguration;
import mvm.rya.iterators.LimitingAgeOffFilter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.openrdf.query.BindingSet;

import java.io.IOException;
import java.util.*;

import static mvm.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;
import static mvm.rya.api.RdfCloudTripleStoreUtils.layoutToTable;

/**
 * Date: 7/17/12
 * Time: 9:28 AM
 */
public class CloudbaseRyaQueryEngine implements RyaQueryEngine<CloudbaseRdfConfiguration> {

    private Log logger = LogFactory.getLog(CloudbaseRyaQueryEngine.class);
    private CloudbaseRdfConfiguration configuration;
    private RyaContext ryaContext = RyaContext.getInstance();
    private Connector connector;
    private Map<TABLE_LAYOUT, KeyValueToRyaStatementFunction> keyValueToRyaStatementFunctionMap = new HashMap<TABLE_LAYOUT, KeyValueToRyaStatementFunction>();

    public CloudbaseRyaQueryEngine(Connector connector) {
        this(connector, new CloudbaseRdfConfiguration());
    }

    public CloudbaseRyaQueryEngine(Connector connector, CloudbaseRdfConfiguration conf) {
        this.connector = connector;
        this.configuration = conf;

        keyValueToRyaStatementFunctionMap.put(TABLE_LAYOUT.SPO, new KeyValueToRyaStatementFunction(TABLE_LAYOUT.SPO));
        keyValueToRyaStatementFunctionMap.put(TABLE_LAYOUT.PO, new KeyValueToRyaStatementFunction(TABLE_LAYOUT.PO));
        keyValueToRyaStatementFunctionMap.put(TABLE_LAYOUT.OSP, new KeyValueToRyaStatementFunction(TABLE_LAYOUT.OSP));
    }

    @Override
    public CloseableIteration<RyaStatement, RyaDAOException> query(RyaStatement stmt, CloudbaseRdfConfiguration conf) throws RyaDAOException {
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
    public CloseableIteration<? extends Map.Entry<RyaStatement, BindingSet>, RyaDAOException> queryWithBindingSet(Collection<Map.Entry<RyaStatement, BindingSet>> stmts, CloudbaseRdfConfiguration conf) throws RyaDAOException {
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
                fillScanner(scanner, context, ttl, tripleRowRegex);
                iterator = new RyaStatementBindingSetKeyValueIterator(layout, scanner, rangeMap);
            } else {
                Scanner scannerBase = null;
                Iterator<Map.Entry<Key, Value>>[] iters = new Iterator[ranges.size()];
                int i = 0;
                for (Range range : ranges) {
                    scannerBase = connector.createScanner(table, authorizations);
                    scannerBase.setRange(range);
                    fillScanner(scannerBase, context, ttl, tripleRowRegex);
                    iters[i] = scannerBase.iterator();
                    i++;
                }
                iterator = new RyaStatementBindingSetKeyValueIterator(layout, Iterators.concat(iters), rangeMap);
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
    public CloseableIteration<RyaStatement, RyaDAOException> batchQuery(Collection<RyaStatement> stmts, CloudbaseRdfConfiguration conf)
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
            } else {
                range = new Range();
                layout = TABLE_LAYOUT.SPO;
            }

            //use range to set scanner
            //populate scanner based on authorizations, ttl
            String table = layoutToTable(layout, tableLayoutStrategy);
            Scanner scanner = connector.createScanner(table, authorizations);
            int itrLevel = 20;
            if (context != null && qualifier != null) {
                scanner.fetchColumn(new Text(context.getData()), new Text(qualifier));
            } else if (context != null) {
                scanner.fetchColumnFamily(new Text(context.getData()));
            } else if (qualifier != null) {
                scanner.setScanIterators(itrLevel++, RegExIterator.class.getName(), "riq");
                scanner.setScanIteratorOption("riq", RegExFilter.COLQ_REGEX, qualifier);
            }
            if (ttl != null) {
                scanner.setScanIterators(itrLevel++, FilteringIterator.class.getName(), "fi");
                scanner.setScanIteratorOption("fi", "0", LimitingAgeOffFilter.class.getName());
                scanner.setScanIteratorOption("fi", "0." + LimitingAgeOffFilter.TTL, ttl.toString());
                if (currentTime != null)
                    scanner.setScanIteratorOption("fi", "0." + LimitingAgeOffFilter.CURRENT_TIME, currentTime.toString());
            }
            scanner.setRange(range);
            if (batchSize != null) {
                scanner.setBatchSize(batchSize);
            }
            //TODO: Fill in context regex
            if (tripleRowRegex != null) {
                scanner.setScanIterators(itrLevel++, RegExIterator.class.getName(), "ri");
                scanner.setScanIteratorOption("ri", RegExFilter.ROW_REGEX, tripleRowRegex.getRow());
            }

            FluentCloseableIterable<RyaStatement> results = FluentCloseableIterable.from(new ScannerCloseableIterable(scanner))
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
                fillScanner(scanner, context, ttl, tripleRowRegex);
                results = FluentCloseableIterable.from(new BatchScannerCloseableIterable(scanner)).transform(keyValueToRyaStatementFunctionMap.get(layout));
            } else {
                final RyaURI fcontext = context;
                FluentIterable<RyaStatement> fluent = FluentIterable.from(ranges).transformAndConcat(new Function<Range, Iterable<Map.Entry<Key, Value>>>() {
                    @Override
                    public Iterable<Map.Entry<Key, Value>> apply(Range range) {
                        try {
                            Scanner scanner = connector.createScanner(table, authorizations);
                            scanner.setRange(range);
                            fillScanner(scanner, fcontext, ttl, tripleRowRegex);
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

    protected void fillScanner(ScannerBase scanner, RyaURI context, Long ttl, TripleRowRegex tripleRowRegex) throws IOException {
        if (context != null) {
            scanner.fetchColumnFamily(new Text(context.getData()));
        }
        if (ttl != null) {
            scanner.setScanIterators(9, FilteringIterator.class.getName(), "fi");
            scanner.setScanIteratorOption("fi", "0", AgeOffFilter.class.getName());
            scanner.setScanIteratorOption("fi", "0.ttl", ttl.toString());
        }
        if (tripleRowRegex != null) {
            scanner.setScanIterators(11, RegExIterator.class.getName(), "ri");
            scanner.setScanIteratorOption("ri", RegExFilter.ROW_REGEX, tripleRowRegex.getRow());
        }
    }

    @Override
    public void setConf(CloudbaseRdfConfiguration conf) {
        this.configuration = conf;
    }

    @Override
    public CloudbaseRdfConfiguration getConf() {
        return configuration;
    }
}
