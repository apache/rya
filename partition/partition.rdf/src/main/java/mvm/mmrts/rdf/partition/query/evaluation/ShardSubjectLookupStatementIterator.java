package mvm.mmrts.rdf.partition.query.evaluation;

import cloudbase.core.client.BatchScanner;
import cloudbase.core.client.Connector;
import cloudbase.core.client.TableNotFoundException;
import cloudbase.core.data.Key;
import cloudbase.core.data.Range;
import cloudbase.core.data.Value;
import cloudbase.core.security.Authorizations;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.EmptyIteration;
import mvm.mmrts.rdf.partition.PartitionSail;
import mvm.mmrts.rdf.partition.query.evaluation.select.FilterIterator;
import mvm.mmrts.rdf.partition.query.evaluation.select.SelectAllIterator;
import mvm.mmrts.rdf.partition.query.operators.ShardSubjectLookup;
import mvm.mmrts.rdf.partition.shard.DateHashModShardValueGenerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.openrdf.model.URI;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.Var;
import ss.cloudbase.core.iterators.CellLevelRecordIterator;
import ss.cloudbase.core.iterators.GMDenIntersectingIterator;
import ss.cloudbase.core.iterators.SortedRangeIterator;
import ss.cloudbase.core.iterators.filter.CBConverter;

import java.io.IOException;
import java.util.*;

import static mvm.mmrts.rdf.partition.PartitionConstants.*;
import static mvm.mmrts.rdf.partition.utils.RdfIO.writeValue;

/**
 * Class ShardSubjectLookupStatementIterator
 * Date: Jul 18, 2011
 * Time: 10:53:55 AM
 */
public class ShardSubjectLookupStatementIterator implements
        CloseableIteration<BindingSet, QueryEvaluationException> {

    private Connector connector;
    private String table;
    //MMRTS-148
    private String shardTable;
    private ShardSubjectLookup lookup;
    private DateHashModShardValueGenerator generator;
    private BatchScanner scanner;
    private BindingSet bindings;
    private CloseableIteration<BindingSet, QueryEvaluationException> iter;
    private Configuration configuration;
//    private TimeType timeType = TimeType.XMLDATETIME;
    private Authorizations authorizations = ALL_AUTHORIZATIONS;

    private int numThreads;

    public ShardSubjectLookupStatementIterator(PartitionSail psail, ShardSubjectLookup lookup, BindingSet bindings, Configuration configuration) throws QueryEvaluationException {
        this.connector = psail.getConnector();
        this.lookup = lookup;
        this.table = psail.getTable();
        this.shardTable = psail.getShardTable();
        this.bindings = bindings;
        this.configuration = configuration;

        //Time Type check
//        timeType = TimeType.valueOf(this.configuration.get(TIME_TYPE_PROP, TimeType.XMLDATETIME.name()));

        //authorizations
        String auths = this.configuration.get(AUTHORIZATION_PROP);
        if (auths != null) {
            authorizations = new Authorizations(auths.split(","));
        }

        //TODO: for now we need this
        this.generator = (DateHashModShardValueGenerator) psail.getGenerator();

        this.numThreads = this.configuration.getInt(NUMTHREADS_PROP, generator.getBaseMod());

        this.initialize();
    }

    public void initialize() throws QueryEvaluationException {
        try {
            /**
             * Here we will set up the BatchScanner based on the lookup
             */
            Var subject = lookup.getSubject();
            List<Map.Entry<Var, Var>> where = retrieveWhereClause();
            List<Map.Entry<Var, Var>> select = retrieveSelectClause();

            //global start-end time
            long start = configuration.getLong(START_BINDING, 0);
            long end = configuration.getLong(END_BINDING, System.currentTimeMillis());

            int whereSize = where.size() + select.size() + ((!isTimeRange(lookup, configuration)) ? 0 : 1);

            if (subject.hasValue()
                    && where.size() == 0  /* Not using whereSize, because we can set up the TimeRange in the scanner */
                    && select.size() == 0) {
                /**
                 * Case 1: Subject is set, but predicate, object are not.
                 * Return all for the subject
                 */
                this.scanner = scannerForSubject(subject.getValue());
                if (this.scanner == null) {
                    this.iter = new EmptyIteration();
                    return;
                }
                Map.Entry<Var, Var> predObj = lookup.getPredicateObjectPairs().get(0);
                this.iter = new SelectAllIterator(this.bindings, this.scanner.iterator(), predObj.getKey(), predObj.getValue());
            } else if (subject.hasValue()
                    && where.size() == 0 /* Not using whereSize, because we can set up the TimeRange in the scanner */) {
                /**
                 * Case 2: Subject is set, and a few predicates are set, but no objects
                 * Return all, and filter which predicates you are interested in
                 */
                this.scanner = scannerForSubject(subject.getValue());
                if (this.scanner == null) {
                    this.iter = new EmptyIteration();
                    return;
                }
                this.iter = new FilterIterator(this.bindings, this.scanner.iterator(), subject, select);
            } else if (subject.hasValue()
                    && where.size() >= 1 /* Not using whereSize, because we can set up the TimeRange in the scanner */) {
                /**
                 * Case 2a: Subject is set, and a few predicates are set, and one object
                 * TODO: For now we will ignore the predicate-object filter because we do not know how to query for this
                 */
                this.scanner = scannerForSubject(subject.getValue());
                if (this.scanner == null) {
                    this.iter = new EmptyIteration();
                    return;
                }
                this.iter = new FilterIterator(this.bindings, this.scanner.iterator(), subject, select);
            } else if (!subject.hasValue() && whereSize > 1) {
                /**
                 * Case 3: Subject is not set, more than one where clause
                 */
                this.scanner = scannerForPredicateObject(lookup, start, end, where, select);
                if (this.scanner == null) {
                    this.iter = new EmptyIteration();
                    return;
                }
                this.iter = new FilterIterator(this.bindings, this.scanner.iterator(), subject, select);
//                this.iter = new SubjectSelectIterator(this.bindings, this.scanner.iterator(), subject, select);
            } else if (!subject.hasValue() && whereSize == 1 && select.size() == 0) {
                /**
                 * Case 4: No subject, only one where clause
                 */
                Map.Entry<Var, Var> predObj = null;
                if (where.size() == 1) {
                    predObj = where.get(0);
                }
                this.scanner = scannerForPredicateObject(lookup, start, end, predObj);
                if (this.scanner == null) {
                    this.iter = new EmptyIteration();
                    return;
                }
                this.iter = new FilterIterator(this.bindings, this.scanner.iterator(), subject, select);
//                this.iter = new SubjectSelectIterator(this.bindings, this.scanner.iterator(), subject, select);
            } else if (!subject.hasValue() && select.size() > 1) {

                /**
                 * Case 5: No subject, no where (multiple select)
                 */
                this.scanner = scannerForPredicates(start, end, select);
                if (this.scanner == null) {
                    this.iter = new EmptyIteration();
                    return;
                }
                this.iter = new FilterIterator(this.bindings, this.scanner.iterator(), subject, select);
            } else if (!subject.hasValue() && select.size() == 1) {
                /**
                 * Case 5: No subject, no where (just 1 select)
                 */
                cloudbase.core.client.Scanner sc = scannerForPredicate(lookup, start, end, (URI) select.get(0).getKey().getValue());
                if (sc == null) {
                    this.iter = new EmptyIteration();
                    return;
                }                                             //TODO: Fix, put in concrete class
                final Iterator<Map.Entry<Key, Value>> scIter = sc.iterator();
                this.iter = new FilterIterator(this.bindings, scIter, subject, select);
            } else {
                throw new QueryEvaluationException("Case not supported as of yet");
            }

        } catch (Exception e) {
            throw new QueryEvaluationException(e);
        }
    }

    protected List<Map.Entry<Var, Var>> retrieveWhereClause() {
        List<Map.Entry<Var, Var>> where = new ArrayList<Map.Entry<Var, Var>>();
        for (Map.Entry<Var, Var> entry : lookup.getPredicateObjectPairs()) {
            Var pred = entry.getKey();
            Var object = entry.getValue();
            if (pred.hasValue() && object.hasValue()) {
                where.add(entry); //TODO: maybe we should clone this?
            }
        }
        return where;
    }

    protected List<Map.Entry<Var, Var>> retrieveSelectClause() {
        List<Map.Entry<Var, Var>> select = new ArrayList<Map.Entry<Var, Var>>();
        for (Map.Entry<Var, Var> entry : lookup.getPredicateObjectPairs()) {
            Var pred = entry.getKey();
            Var object = entry.getValue();
            if (pred.hasValue() && !object.hasValue()) {
                select.add(entry); //TODO: maybe we should clone this?
            }
        }
        return select;
    }

    @Override
    public void close() throws QueryEvaluationException {
        if (this.scanner != null) {
            this.scanner.close();
        }
    }

    @Override
    public boolean hasNext() throws QueryEvaluationException {
        return iter.hasNext();
    }

    @Override
    public BindingSet next() throws QueryEvaluationException {
        try {
            return iter.next();
        } catch (Exception e) {
            throw new QueryEvaluationException(e);
        }
    }

    @Override
    public void remove() throws QueryEvaluationException {
        iter.next();
    }

    /**
     * Utility methods to set up the scanner/batch scanner
     */

    protected List<Text> shardForSubject(org.openrdf.model.Value subject) throws TableNotFoundException, IOException {
        BatchScanner scanner = createBatchScanner(this.shardTable);
        try {
            scanner.setRanges(Collections.singleton(
                    new Range(new Text(writeValue(subject)))
            ));
            Iterator<Map.Entry<Key, Value>> shardIter = scanner.iterator();
            if (!shardIter.hasNext()) {
                return null;
            }

            List<Text> shards = new ArrayList<Text>();
            while (shardIter.hasNext()) {
                shards.add(shardIter.next().getKey().getColumnFamily());
            }
            //MMRTS-147 so that we can return subjects from multiple shards
            return shards;
        } finally {
            if (scanner != null)
                scanner.close();
        }
    }


    protected BatchScanner scannerForSubject(org.openrdf.model.Value subject) throws TableNotFoundException, IOException {
        List<Text> shards = shardForSubject(subject);

        if (shards == null)
            return null;

        BatchScanner scanner = createBatchScanner(this.table);

//        scanner.setScanIterators(21, CellLevelRecordIterator.class.getName(), "ci");
        Collection<Range> ranges = new ArrayList<Range>();
        for (Text shard : shards) {
            ranges.add(new Range(
                    new Key(
                            shard, DOC,
                            new Text(URI_MARKER_STR + subject + FAMILY_DELIM_STR + "\0")
                    ),
                    new Key(
                            shard, DOC,
                            new Text(URI_MARKER_STR + subject + FAMILY_DELIM_STR + "\uFFFD")
                    )
            ));
        }
        scanner.setRanges(ranges);
        return scanner;
    }

    protected BatchScanner scannerForPredicateObject(ShardSubjectLookup lookup, Long start, Long end, List<Map.Entry<Var, Var>> predObjs, List<Map.Entry<Var, Var>> select) throws IOException, TableNotFoundException {
        start = validateFillStartTime(start, lookup);
        end = validateFillEndTime(end, lookup);

        int extra = 0;

        if (isTimeRange(lookup, configuration)) {
            extra += 1;
        }

        Text[] queries = new Text[predObjs.size() + select.size() + extra];
        int qi = 0;
        for (Map.Entry<Var, Var> predObj : predObjs) {
            ByteArrayDataOutput output = ByteStreams.newDataOutput();
            writeValue(output, predObj.getKey().getValue());
            output.write(INDEX_DELIM);
            writeValue(output, predObj.getValue().getValue());
            queries[qi++] = new Text(output.toByteArray());
        }
        for (Map.Entry<Var, Var> predicate : select) {
            queries[qi++] = new Text(GMDenIntersectingIterator.getRangeTerm(INDEX.toString(),
                    URI_MARKER_STR + predicate.getKey().getValue() + INDEX_DELIM_STR + "\0"
                    , true,
                    URI_MARKER_STR + predicate.getKey().getValue() + INDEX_DELIM_STR + "\uFFFD",
                    true
            ));
        }

        if (isTimeRange(lookup, configuration)) {
            queries[queries.length - 1] = new Text(
                    GMDenIntersectingIterator.getRangeTerm(INDEX.toString(),
                            getStartTimeRange(lookup, configuration)
                            , true,
                            getEndTimeRange(lookup, configuration),
                            true
                    )
            );
        }

        BatchScanner bs = createBatchScanner(this.table);

        bs.setScanIterators(21, CellLevelRecordIterator.class.getName(), "ci");
        bs.setScanIteratorOption("ci", CBConverter.OPTION_VALUE_DELIMITER, VALUE_DELIMITER);

        bs.setScanIterators(20, GMDenIntersectingIterator.class.getName(), "ii");
        bs.setScanIteratorOption("ii", GMDenIntersectingIterator.docFamilyOptionName, DOC.toString());
        bs.setScanIteratorOption("ii", GMDenIntersectingIterator.indexFamilyOptionName, INDEX.toString());
        bs.setScanIteratorOption("ii", GMDenIntersectingIterator.columnFamiliesOptionName, GMDenIntersectingIterator.encodeColumns(queries));
        bs.setScanIteratorOption("ii", GMDenIntersectingIterator.OPTION_MULTI_DOC, "" + true);

        Range range = new Range(
                new Key(new Text(generator.generateShardValue(start, null) + "\0")),
                new Key(new Text(generator.generateShardValue(end, null) + "\uFFFD"))
        );
        bs.setRanges(Collections.singleton(
                range
        ));

        return bs;
    }

    protected BatchScanner scannerForPredicateObject(ShardSubjectLookup lookup, Long start, Long end, Map.Entry<Var, Var> predObj) throws IOException, TableNotFoundException {
        start = validateFillStartTime(start, lookup);
        end = validateFillEndTime(end, lookup);

        BatchScanner bs = createBatchScanner(this.table);

        bs.setScanIterators(21, CellLevelRecordIterator.class.getName(), "ci");
        bs.setScanIteratorOption("ci", CBConverter.OPTION_VALUE_DELIMITER, VALUE_DELIMITER);

        bs.setScanIterators(20, SortedRangeIterator.class.getName(), "ri");
        bs.setScanIteratorOption("ri", SortedRangeIterator.OPTION_DOC_COLF, DOC.toString());
        bs.setScanIteratorOption("ri", SortedRangeIterator.OPTION_COLF, INDEX.toString());
        bs.setScanIteratorOption("ri", SortedRangeIterator.OPTION_START_INCLUSIVE, "" + true);
        bs.setScanIteratorOption("ri", SortedRangeIterator.OPTION_END_INCLUSIVE, "" + true);
        bs.setScanIteratorOption("ri", SortedRangeIterator.OPTION_MULTI_DOC, "" + true);

        if (isTimeRange(lookup, configuration)) {
            String startRange = getStartTimeRange(lookup, configuration);
            bs.setScanIteratorOption("ri", SortedRangeIterator.OPTION_LOWER_BOUND,
                    startRange);
            String endRange = getEndTimeRange(lookup, configuration);
            bs.setScanIteratorOption("ri", SortedRangeIterator.OPTION_UPPER_BOUND,
                    endRange);
        } else {

            ByteArrayDataOutput output = ByteStreams.newDataOutput();
            writeValue(output, predObj.getKey().getValue());
            output.write(INDEX_DELIM);
            writeValue(output, predObj.getValue().getValue());

            String bound = new String(output.toByteArray());
            bs.setScanIteratorOption("ri", SortedRangeIterator.OPTION_LOWER_BOUND, bound);
            bs.setScanIteratorOption("ri", SortedRangeIterator.OPTION_UPPER_BOUND, bound + "\00");
        }

        //TODO: Do we add a time predicate to this?
//        bs.setScanIterators(19, FilteringIterator.class.getName(), "filteringIterator");
//        bs.setScanIteratorOption("filteringIterator", "0", TimeRangeFilter.class.getName());
//        bs.setScanIteratorOption("filteringIterator", "0." + TimeRangeFilter.TIME_RANGE_PROP, (end - start) + "");
//        bs.setScanIteratorOption("filteringIterator", "0." + TimeRangeFilter.START_TIME_PROP, end + "");

        Range range = new Range(
                new Key(new Text(generator.generateShardValue(start, null) + "\0")),
                new Key(new Text(generator.generateShardValue(end, null) + "\uFFFD"))
        );
        bs.setRanges(Collections.singleton(
                range
        ));

        return bs;
    }

    protected BatchScanner scannerForPredicates(Long start, Long end, List<Map.Entry<Var, Var>> predicates) throws IOException, TableNotFoundException {
        start = validateFillStartTime(start, lookup);
        end = validateFillEndTime(end, lookup);

        int extra = 0;

        if (isTimeRange(lookup, configuration)) {
            extra += 1;
        }

        Text[] queries = new Text[predicates.size() + extra];
        for (int i = 0; i < predicates.size(); i++) {
            Map.Entry<Var, Var> predicate = predicates.get(i);
            queries[i] = new Text(GMDenIntersectingIterator.getRangeTerm(INDEX.toString(),
                    URI_MARKER_STR + predicate.getKey().getValue() + INDEX_DELIM_STR + "\0"
                    , true,
                    URI_MARKER_STR + predicate.getKey().getValue() + INDEX_DELIM_STR + "\uFFFD",
                    true
            ));
        }

        if (isTimeRange(lookup, configuration)) {
            queries[queries.length - 1] = new Text(
                    GMDenIntersectingIterator.getRangeTerm(INDEX.toString(),
                            getStartTimeRange(lookup, configuration)
                            , true,
                            getEndTimeRange(lookup, configuration),
                            true
                    )
            );
        }

        BatchScanner bs = createBatchScanner(this.table);
        bs.setScanIterators(21, CellLevelRecordIterator.class.getName(), "ci");
        bs.setScanIteratorOption("ci", CBConverter.OPTION_VALUE_DELIMITER, VALUE_DELIMITER);

        bs.setScanIterators(20, GMDenIntersectingIterator.class.getName(), "ii");
        bs.setScanIteratorOption("ii", GMDenIntersectingIterator.docFamilyOptionName, DOC.toString());
        bs.setScanIteratorOption("ii", GMDenIntersectingIterator.indexFamilyOptionName, INDEX.toString());
        bs.setScanIteratorOption("ii", GMDenIntersectingIterator.columnFamiliesOptionName, GMDenIntersectingIterator.encodeColumns(queries));
        bs.setScanIteratorOption("ii", GMDenIntersectingIterator.OPTION_MULTI_DOC, "" + true);

        Range range = new Range(
                new Key(new Text(generator.generateShardValue(start, null) + "\0")),
                new Key(new Text(generator.generateShardValue(end, null) + "\uFFFD"))
        );
        bs.setRanges(Collections.singleton(
                range
        ));

        return bs;
    }

    protected cloudbase.core.client.Scanner scannerForPredicate(ShardSubjectLookup lookup, Long start, Long end, URI predicate) throws IOException, TableNotFoundException {
        start = validateFillStartTime(start, lookup);
        end = validateFillEndTime(end, lookup);

        cloudbase.core.client.Scanner sc = createScanner(this.table);

        Range range = new Range(
                new Key(new Text(generator.generateShardValue(start, null) + "\0")),
                new Key(new Text(generator.generateShardValue(end, null) + "\uFFFD"))
        );
        sc.setRange(range);
        sc.fetchColumnFamily(INDEX);
        sc.setColumnFamilyRegex(INDEX.toString());
        sc.setColumnQualifierRegex(URI_MARKER_STR + predicate + INDEX_DELIM_STR + "(.*)");

        return sc;
    }

    protected cloudbase.core.client.Scanner createScanner(String sTable) throws TableNotFoundException {
        return connector.createScanner(sTable, authorizations);
    }

    protected BatchScanner createBatchScanner(String sTable) throws TableNotFoundException {
        return createBatchScanner(sTable, numThreads);
    }

    protected BatchScanner createBatchScanner(String sTable, int numThreads) throws TableNotFoundException {
        return connector.createBatchScanner(sTable, authorizations, numThreads);
    }
}
