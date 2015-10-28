package mvm.mmrts.rdf.partition.mr.transform;

import cloudbase.core.CBConstants;
import cloudbase.core.client.TableNotFoundException;
import cloudbase.core.data.Key;
import cloudbase.core.data.Range;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import mvm.rya.cloudbase.utils.input.CloudbaseBatchScannerInputFormat;
import mvm.mmrts.rdf.partition.mr.iterators.SortedEncodedRangeIterator;
import mvm.mmrts.rdf.partition.query.operators.ShardSubjectLookup;
import mvm.mmrts.rdf.partition.shard.DateHashModShardValueGenerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.Var;
import ss.cloudbase.core.iterators.GMDenIntersectingIterator;
import ss.cloudbase.core.iterators.SortedRangeIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static mvm.mmrts.rdf.partition.PartitionConstants.*;
import static mvm.mmrts.rdf.partition.utils.RdfIO.writeValue;

import static mvm.mmrts.rdf.partition.mr.transform.SparqlCloudbaseIFTransformerConstants.*;

/**
 * Class SparqlCloudbaseIFTransformer
 * Date: Sep 1, 2011
 * Time: 11:28:48 AM
 */
public class SparqlCloudbaseIFTransformer {

    protected Job job;

    protected String userName;
    protected String pwd;
    protected String instance;
    protected String zk;

    protected ShardSubjectLookup lookup;
//    protected Configuration configuration;
    protected String table;

    protected DateHashModShardValueGenerator generator;

    public SparqlCloudbaseIFTransformer(ShardSubjectLookup lookup, Configuration configuration, Job job, String table,
                                        String userName, String pwd, String instance, String zk) throws QueryEvaluationException {
        this(lookup, configuration, job, table, userName, pwd, instance, zk, new DateHashModShardValueGenerator());
    }

    public SparqlCloudbaseIFTransformer(ShardSubjectLookup lookup, Configuration configuration, Job job, String table,
                                        String userName, String pwd, String instance, String zk, DateHashModShardValueGenerator generator) throws QueryEvaluationException {
        this.lookup = lookup;
//        this.configuration = configuration;
        this.table = table;
        this.job = job;
        this.userName = userName;
        this.pwd = pwd;
        this.instance = instance;
        this.zk = zk;
        this.generator = generator;

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
            long start = job.getConfiguration().getLong(START_BINDING, 0);
            long end = job.getConfiguration().getLong(END_BINDING, System.currentTimeMillis());

            int whereSize = where.size() + ((!isTimeRange(lookup, job.getConfiguration())) ? 0 : 1);

            if (subject.hasValue()
                    && where.size() == 0  /* Not using whereSize, because we can set up the TimeRange in the scanner */
                    && select.size() == 0) {
                /**
                 * Case 1: Subject is set, but predicate, object are not.
                 * Return all for the subject
                 */
//                this.scanner = scannerForSubject((URI) subject.getValue());
//                if (this.scanner == null) {
//                    this.iter = new EmptyIteration();
//                    return;
//                }
//                Map.Entry<Var, Var> predObj = lookup.getPredicateObjectPairs().get(0);
//                this.iter = new SelectAllIterator(this.bindings, this.scanner.iterator(), predObj.getKey(), predObj.getValue());
                throw new UnsupportedOperationException("Query Case not supported");
            } else if (subject.hasValue()
                    && where.size() == 0 /* Not using whereSize, because we can set up the TimeRange in the scanner */) {
                /**
                 * Case 2: Subject is set, and a few predicates are set, but no objects
                 * Return all, and filter which predicates you are interested in
                 */
//                this.scanner = scannerForSubject((URI) subject.getValue());
//                if (this.scanner == null) {
//                    this.iter = new EmptyIteration();
//                    return;
//                }
//                this.iter = new FilterIterator(this.bindings, this.scanner.iterator(), subject, select);
                throw new UnsupportedOperationException("Query Case not supported");
            } else if (subject.hasValue()
                    && where.size() >= 1 /* Not using whereSize, because we can set up the TimeRange in the scanner */) {
                /**
                 * Case 2a: Subject is set, and a few predicates are set, and one object
                 * TODO: For now we will ignore the predicate-object filter because we do not know how to query for this
                 */
//                this.scanner = scannerForSubject((URI) subject.getValue());
//                if (this.scanner == null) {
//                    this.iter = new EmptyIteration();
//                    return;
//                }
//                this.iter = new FilterIterator(this.bindings, this.scanner.iterator(), subject, select);
                throw new UnsupportedOperationException("Query Case not supported");
            } else if (!subject.hasValue() && whereSize > 1) {
                /**
                 * Case 3: Subject is not set, more than one where clause
                 */
                scannerForPredicateObject(lookup, start, end, where);
                setSelectFilter(subject, select);
            } else if (!subject.hasValue() && whereSize == 1) {
                /**
                 * Case 4: No subject, only one where clause
                 */
                Map.Entry<Var, Var> predObj = null;
                if (where.size() == 1) {
                    predObj = where.get(0);
                }
                scannerForPredicateObject(lookup, start, end, predObj);
                setSelectFilter(subject, select);
            } else if (!subject.hasValue() && whereSize == 0 && select.size() > 1) {
                /**
                 * Case 5: No subject, no where (just 1 select)
                 */
//                this.scanner = scannerForPredicates(start, end, select);
//                if (this.scanner == null) {
//                    this.iter = new EmptyIteration();
//                    return;
//                }
//                this.iter = new FilterIterator(this.bindings, this.scanner.iterator(), subject, select);
                throw new UnsupportedOperationException("Query Case not supported");
            } else if (!subject.hasValue() && whereSize == 0 && select.size() == 1) {
                /**
                 * Case 5: No subject, no where (just 1 select)
                 */
//                cloudbase.core.client.Scanner sc = scannerForPredicate(start, end, (URI) select.get(0).getKey().getValue());
//                if (sc == null) {
//                    this.iter = new EmptyIteration();
//                    return;
//                }
//                this.iter = new FilterIterator(this.bindings, sc.iterator(), subject, select);
                throw new UnsupportedOperationException("Query Case not supported");
            } else {
                throw new QueryEvaluationException("Case not supported as of yet");
            }

        } catch (Exception e) {
            throw new QueryEvaluationException(e);
        }
    }

    protected void setSelectFilter(Var subj, List<Map.Entry<Var, Var>> select) {
        List<String> selectStrs = new ArrayList<String>();
        for (Map.Entry<Var, Var> entry : select) {
            Var key = entry.getKey();
            Var obj = entry.getValue();
            if (key.hasValue()) {
                String pred_s = key.getValue().stringValue();
                selectStrs.add(pred_s);
                job.getConfiguration().set(pred_s, obj.getName());
            }
        }
        job.getConfiguration().setStrings(SELECT_FILTER, selectStrs.toArray(new String[selectStrs.size()]));
        job.getConfiguration().set(SUBJECT_NAME, subj.getName());
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

    protected void scannerForPredicateObject(ShardSubjectLookup lookup, Long start, Long end, List<Map.Entry<Var, Var>> predObjs) throws IOException, TableNotFoundException {
        start = validateFillStartTime(start, lookup);
        end = validateFillEndTime(end, lookup);

        int extra = 0;

        if (isTimeRange(lookup, job.getConfiguration())) {
            extra += 1;
        }

        Text[] queries = new Text[predObjs.size() + extra];
        for (int i = 0; i < predObjs.size(); i++) {
            Map.Entry<Var, Var> predObj = predObjs.get(i);
            ByteArrayDataOutput output = ByteStreams.newDataOutput();
            writeValue(output, predObj.getKey().getValue());
            output.write(INDEX_DELIM);
            writeValue(output, predObj.getValue().getValue());
            queries[i] = new Text(output.toByteArray());
        }

        if (isTimeRange(lookup, job.getConfiguration())) {
            queries[queries.length - 1] = new Text(
                    GMDenIntersectingIterator.getRangeTerm(INDEX.toString(),
                            getStartTimeRange(lookup, job.getConfiguration())
                            , true,
                            getEndTimeRange(lookup, job.getConfiguration()),
                            true
                    )
            );
        }

        createBatchScannerInputFormat();
        CloudbaseBatchScannerInputFormat.setIterator(job, 20, GMDenIntersectingIterator.class.getName(), "ii");
        CloudbaseBatchScannerInputFormat.setIteratorOption(job, "ii", GMDenIntersectingIterator.docFamilyOptionName, DOC.toString());
        CloudbaseBatchScannerInputFormat.setIteratorOption(job, "ii", GMDenIntersectingIterator.indexFamilyOptionName, INDEX.toString());
        CloudbaseBatchScannerInputFormat.setIteratorOption(job, "ii", GMDenIntersectingIterator.columnFamiliesOptionName, GMDenIntersectingIterator.encodeColumns(queries));
        CloudbaseBatchScannerInputFormat.setIteratorOption(job, "ii", GMDenIntersectingIterator.OPTION_MULTI_DOC, "" + true);

        Range range = new Range(
                new Key(new Text(generator.generateShardValue(start, null) + "\0")),
                new Key(new Text(generator.generateShardValue(end, null) + "\uFFFD"))
        );
        CloudbaseBatchScannerInputFormat.setRanges(job, Collections.singleton(
                range
        ));
    }

    protected void scannerForPredicateObject(ShardSubjectLookup lookup, Long start, Long end, Map.Entry<Var, Var> predObj) throws IOException, TableNotFoundException {
        start = validateFillStartTime(start, lookup);
        end = validateFillEndTime(end, lookup);

        /**
         * Need to use GMDen because SortedRange can't serialize non xml characters in range
         * @see https://issues.apache.org/jira/browse/MAPREDUCE-109
         */
        createBatchScannerInputFormat();
        CloudbaseBatchScannerInputFormat.setIterator(job, 20, SortedEncodedRangeIterator.class.getName(), "ri");
        CloudbaseBatchScannerInputFormat.setIteratorOption(job, "ri", SortedRangeIterator.OPTION_DOC_COLF, DOC.toString());
        CloudbaseBatchScannerInputFormat.setIteratorOption(job, "ri", SortedRangeIterator.OPTION_COLF, INDEX.toString());
        CloudbaseBatchScannerInputFormat.setIteratorOption(job, "ri", SortedRangeIterator.OPTION_START_INCLUSIVE, "" + true);
        CloudbaseBatchScannerInputFormat.setIteratorOption(job, "ri", SortedRangeIterator.OPTION_END_INCLUSIVE, "" + true);
        CloudbaseBatchScannerInputFormat.setIteratorOption(job, "ri", SortedRangeIterator.OPTION_MULTI_DOC, "" + true);

        String lower, upper = null;
        if (isTimeRange(lookup, job.getConfiguration())) {
            lower = getStartTimeRange(lookup, job.getConfiguration());
            upper = getEndTimeRange(lookup, job.getConfiguration());
        } else {

            ByteArrayDataOutput output = ByteStreams.newDataOutput();
            writeValue(output, predObj.getKey().getValue());
            output.write(INDEX_DELIM);
            writeValue(output, predObj.getValue().getValue());

            lower = new String(output.toByteArray());
            upper = lower + "\01";
        }
        CloudbaseBatchScannerInputFormat.setIteratorOption(job, "ri", SortedRangeIterator.OPTION_LOWER_BOUND, SortedEncodedRangeIterator.encode(lower));
        CloudbaseBatchScannerInputFormat.setIteratorOption(job, "ri", SortedRangeIterator.OPTION_UPPER_BOUND, SortedEncodedRangeIterator.encode(upper));

        //TODO: Do we add a time predicate to this?
//        bs.setScanIterators(19, FilteringIterator.class.getName(), "filteringIterator");
//        bs.setScanIteratorOption("filteringIterator", "0", TimeRangeFilter.class.getName());
//        bs.setScanIteratorOption("filteringIterator", "0." + TimeRangeFilter.TIME_RANGE_PROP, (end - start) + "");
//        bs.setScanIteratorOption("filteringIterator", "0." + TimeRangeFilter.START_TIME_PROP, end + "");

        Range range = new Range(
                new Key(new Text(generator.generateShardValue(start, null) + "\0")),
                new Key(new Text(generator.generateShardValue(end, null) + "\uFFFD"))
        );
        CloudbaseBatchScannerInputFormat.setRanges(job, Collections.singleton(
                range
        ));

    }

    protected void createBatchScannerInputFormat() {
        job.setInputFormatClass(CloudbaseBatchScannerInputFormat.class);
        CloudbaseBatchScannerInputFormat.setInputInfo(job, userName, pwd.getBytes(), table, CBConstants.NO_AUTHS); //may need to change these auths sometime soon
        CloudbaseBatchScannerInputFormat.setZooKeeperInstance(job, instance, zk);
        job.setMapperClass(KeyValueToMapWrMapper.class);
        job.setCombinerClass(AggregateTriplesBySubjectCombiner.class);
        job.setReducerClass(AggregateTriplesBySubjectReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(MapWritable.class);

        job.getConfiguration().set("io.sort.mb", "256");
        job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);
        job.getConfiguration().setBoolean("mapred.reduce.tasks.speculative.execution", false);
    }

}
