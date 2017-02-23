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
package org.apache.rya.indexing.accumulo.temporal;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.datatype.XMLGregorianCalendar;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.codec.binary.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.rya.accumulo.experimental.AbstractAccumuloIndexer;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.resolver.RyaToRdfConversions;
import org.apache.rya.indexing.KeyParts;
import org.apache.rya.indexing.StatementConstraints;
import org.apache.rya.indexing.StatementSerializer;
import org.apache.rya.indexing.TemporalIndexer;
import org.apache.rya.indexing.TemporalInstant;
import org.apache.rya.indexing.TemporalInstantRfc3339;
import org.apache.rya.indexing.TemporalInterval;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.joda.time.DateTime;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.query.QueryEvaluationException;

import info.aduna.iteration.CloseableIteration;

public class AccumuloTemporalIndexer extends AbstractAccumuloIndexer implements TemporalIndexer {

    private static final String TABLE_SUFFIX = "temporal";

    private static final Logger logger = Logger.getLogger(AccumuloTemporalIndexer.class);

    private static final String CF_INTERVAL = "interval";



    // Delimiter used in the interval stored in the triple's object literal.
    // So far, no ontology specifies a date range, just instants.
    // Set to the same delimiter used by the indexer, probably needs revisiting.
    //private static final String REGEX_intervalDelimiter = TemporalInterval.DELIMITER;

    private Configuration conf;

    private MultiTableBatchWriter mtbw;

    private BatchWriter temporalIndexBatchWriter;

    private Set<URI> validPredicates;
    private String temporalIndexTableName;

    private boolean isInit = false;



    private void initInternal() throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
            TableExistsException {
        temporalIndexTableName = getTableName();
        // Create one index table on first run.
        ConfigUtils.createTableIfNotExists(conf, temporalIndexTableName);

        mtbw = ConfigUtils.createMultitableBatchWriter(conf);

        temporalIndexBatchWriter = mtbw.getBatchWriter(temporalIndexTableName);

        validPredicates = ConfigUtils.getTemporalPredicates(conf);
    }

    //initialization occurs in setConf because index is created using reflection
    @Override
    public void setConf(final Configuration conf) {
        this.conf = conf;
        if (!isInit) {
            try {
                initInternal();
                isInit = true;
            } catch (final AccumuloException e) {
                logger.warn("Unable to initialize index.  Throwing Runtime Exception. ", e);
                throw new RuntimeException(e);
            } catch (final AccumuloSecurityException e) {
                logger.warn("Unable to initialize index.  Throwing Runtime Exception. ", e);
                throw new RuntimeException(e);
            } catch (final TableNotFoundException e) {
                logger.warn("Unable to initialize index.  Throwing Runtime Exception. ", e);
                throw new RuntimeException(e);
            } catch (final TableExistsException e) {
                logger.warn("Unable to initialize index.  Throwing Runtime Exception. ", e);
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public Configuration getConf() {
        return conf;
    }


    /**
     * Store a statement in the index if it meets the criterion: Object should be
     * a literal and one of the validPredicates from the configuration.
     * If it does not meet the criteria, it is silently ignored.
     * logs a warning if the object is not parse-able.
     * Attempts to parse with calendarValue = literalValue.calendarValue()
     * if that fails, tries: org.joda.time.DateTime.parse() .
     * T O D O parse an interval using multiple predicates for same subject -- ontology dependent.
     */
    private void storeStatement(final Statement statement) throws IOException, IllegalArgumentException {
        // if the predicate list is empty, accept all predicates.
        // Otherwise, make sure the predicate is on the "valid" list
        final boolean isValidPredicate = validPredicates.isEmpty() || validPredicates.contains(statement.getPredicate());
        if (!isValidPredicate || !(statement.getObject() instanceof Literal)) {
            return;
        }
        final DateTime[] indexDateTimes = new DateTime[2]; // 0 begin, 1 end of interval
        extractDateTime(statement, indexDateTimes);
        if (indexDateTimes[0]==null) {
            return;
        }

        // Add this as an instant, or interval.
        try {
            if (indexDateTimes[1] != null) {
                final TemporalInterval interval = new TemporalInterval(new TemporalInstantRfc3339(indexDateTimes[0]), new TemporalInstantRfc3339(indexDateTimes[1]));
                addInterval(temporalIndexBatchWriter, interval, statement);
            } else {
                final TemporalInstant instant = new TemporalInstantRfc3339(indexDateTimes[0]);
                addInstant(temporalIndexBatchWriter, instant, statement);
            }
        } catch (final MutationsRejectedException e) {
            throw new IOException("While adding interval/instant for statement =" + statement, e);
        }
    }


    @Override
    public void storeStatement(final RyaStatement statement) throws IllegalArgumentException, IOException {
        storeStatement(RyaToRdfConversions.convertStatement(statement));
    }



    /**
     * parse the literal dates from the object of a statement.
     *
     * @param statement
     * @param outputDateTimes
     */
    private void extractDateTime(final Statement statement, final DateTime[] outputDateTimes) {
        if (!(statement.getObject() instanceof Literal)) {
            throw new RuntimeException("Statement's object must be a literal: " + statement);
        }
        // throws IllegalArgumentException NumberFormatException if can't parse
        String logThis = null;         final Literal literalValue = (Literal) statement.getObject();
        // First attempt to parse a interval in the form "[date1,date2]"
        final Matcher matcher = Pattern.compile("\\[(.*)\\,(.*)\\].*").matcher(literalValue.stringValue());
        if (matcher.find()) {
            try {
                // Got a datetime pair, parse into an interval.
                outputDateTimes[0] = new DateTime(matcher.group(1));
                outputDateTimes[1] = new DateTime(matcher.group(2));
                return;
            } catch (final java.lang.IllegalArgumentException e) {
                logThis = e.getMessage() + " " + logThis;
                outputDateTimes[0]=null;
                outputDateTimes[1]=null;
            }
        }

        try {
            final XMLGregorianCalendar calendarValue = literalValue.calendarValue();
            outputDateTimes[0] = new DateTime(calendarValue.toGregorianCalendar());
            outputDateTimes[1] = null;
            return;
        } catch (final java.lang.IllegalArgumentException e) {
            logThis = e.getMessage();
        }
        // Try again using Joda Time DateTime.parse()
        try {
            outputDateTimes[0] = DateTime.parse(literalValue.stringValue());
            outputDateTimes[1] = null;
            //System.out.println(">>>>>>>Joda parsed: "+literalValue.stringValue());
            return;
        } catch (final java.lang.IllegalArgumentException e) {
            logThis = e.getMessage() + " " + logThis;
        }
        logger.warn("TemporalIndexer is unable to parse the date/time from statement="  + statement.toString() + " " +logThis);
        return;
    }

    /**
     * Remove an interval index
     * TODO: integrate into KeyParts (or eliminate)
     * @param writer
     * @param cv
     * @param interval
     * @throws MutationsRejectedException
     */
    public void removeInterval(final BatchWriter writer, final TemporalInterval interval, final Statement statement) throws MutationsRejectedException {
        final Text cf = new Text(StatementSerializer.writeContext(statement));
        final Text cqBegin = new Text(KeyParts.CQ_BEGIN);
        final Text cqEnd = new Text(KeyParts.CQ_END);

        // Start Begin index
        Text keyText = new Text(interval.getAsKeyBeginning());
        KeyParts.appendUniqueness(statement, keyText);
        Mutation m = new Mutation(keyText);
        m.putDelete(cf, cqBegin);
        writer.addMutation(m);

        // now the end index:
        keyText = new Text(interval.getAsKeyEnd());
        KeyParts.appendUniqueness(statement, keyText);
        m = new Mutation(keyText);
        m.putDelete(cf, cqEnd);
        writer.addMutation(m);
    }

    /**
     * Remove an interval instant
     *
     * @param writer
     * @param cv
     * @param instant
     * @throws MutationsRejectedException
     */
    public void removeInstant(final BatchWriter writer, final TemporalInstant instant, final Statement statement) throws MutationsRejectedException {
        final KeyParts keyParts = new KeyParts(statement, instant);
        for (final KeyParts  k: keyParts) {
            final Mutation m = new Mutation(k.getStoreKey());
            m.putDelete(k.cf, k.cq);
            writer.addMutation(m);
        }
    }

    /**
     * Index a new interval
     * TODO: integrate into KeyParts (or eliminate)
     * @param writer
     * @param cv
     * @param interval
     * @throws MutationsRejectedException
     */
    public void addInterval(final BatchWriter writer, final TemporalInterval interval, final Statement statement) throws MutationsRejectedException {

        final Value statementValue = new Value(StringUtils.getBytesUtf8(StatementSerializer.writeStatement(statement)));
        final Text cf = new Text(StatementSerializer.writeContext(statement));
        final Text cqBegin = new Text(KeyParts.CQ_BEGIN);
        final Text cqEnd = new Text(KeyParts.CQ_END);

        // Start Begin index
        Text keyText = new Text(interval.getAsKeyBeginning());
        KeyParts.appendUniqueness(statement, keyText);
        Mutation m = new Mutation(keyText);
        m.put(cf, cqBegin, statementValue);
        // System.out.println("mutations add begin row=" + m.getRow() + " value=" + value.toString());
        writer.addMutation(m);

        // now the end index:
        keyText = new Text(interval.getAsKeyEnd());
        KeyParts.appendUniqueness(statement, keyText);
        m = new Mutation(keyText);
        m.put(cf, cqEnd, new Value(statementValue));
        // System.out.println("mutations add end row=" + m.getRow() + " value=" + value.toString());
        writer.addMutation(m);
    }


    /**
     * Index a new instant
     * Make indexes that handle this expression:
     *     hash( s? p? ) ?o
     *         == o union hash(s)o union hash(p)o  union hash(sp)o
     *
     * @param writer
     * @param cv
     * @param instant
     * @throws MutationsRejectedException
     */
    public void addInstant(final BatchWriter writer, final TemporalInstant instant, final Statement statement) throws MutationsRejectedException {
        final KeyParts keyParts = new KeyParts(statement, instant);
        for (final KeyParts k : keyParts) {
            final Mutation m = new Mutation(k.getStoreKey());
            m.put(k.cf, k.cq,k.getValue());
            writer.addMutation(m);
        }
    }


    /**
     * creates a scanner and handles all the throwables and nulls.
     *
     * @param scanner
     * @return
     * @throws IOException
     */
    private Scanner getScanner() throws QueryEvaluationException {
        final String whileDoing = "While creating a scanner for a temporal query. table name=" + temporalIndexTableName;
        Scanner scanner = null;
        try {
            scanner = ConfigUtils.createScanner(temporalIndexTableName, conf);
        } catch (final AccumuloException e) {
            logger.error(whileDoing, e);
            throw new QueryEvaluationException(whileDoing, e);
        } catch (final AccumuloSecurityException e) {
            throw new QueryEvaluationException(whileDoing, e);
        } catch (final TableNotFoundException e) {
            logger.error(whileDoing, e);
            throw new QueryEvaluationException(whileDoing
                    + " The temporal index table should have been created by this constructor, if found missing.", e);
        }
        return scanner;
    }

    private BatchScanner getBatchScanner() throws QueryEvaluationException {
        final String whileDoing = "While creating a Batch scanner for a temporal query. table name=" + temporalIndexTableName;
        try {
            return ConfigUtils.createBatchScanner(temporalIndexTableName, conf);
        } catch (final AccumuloException e) {
            logger.error(whileDoing, e);
            throw new QueryEvaluationException(whileDoing, e);
        } catch (final AccumuloSecurityException e) {
            throw new QueryEvaluationException(whileDoing, e);
        } catch (final TableNotFoundException e) {
            logger.error(whileDoing, e);
            throw new QueryEvaluationException(whileDoing
                    + " The temporal index table should have been created by this constructor, if found missing. ", e);
        }
    }

    /**
     * statements where the datetime is exactly the same as the queryInstant.
     */
    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryInstantEqualsInstant(
            final TemporalInstant queryInstant, final StatementConstraints constraints)
            throws QueryEvaluationException {
            // get rows where the repository time is equal to the given time in queryInstant.
            final Query query = new Query() {
                @Override
                public Range getRange(final KeyParts keyParts) {
                    //System.out.println("Scanning queryInstantEqualsInstant: prefix:" + KeyParts.toHumanString(keyParts.getQueryKey()));
                    return Range.prefix(keyParts.getQueryKey()); // <-- specific logic
                }
            };
            final ScannerBase scanner = query.doQuery(queryInstant, constraints);
            // TODO currently context constraints are filtered on the client.
            return getContextIteratorWrapper(scanner, constraints.getContext());
    }

    /**
     * get statements where the db row ID is BEFORE the given queryInstant.
     */
    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryInstantBeforeInstant(
            final TemporalInstant queryInstant, final StatementConstraints constraints)
            throws QueryEvaluationException {
        // get rows where the repository time is before the given time.
        final Query query = new Query() {
            @Override
            public Range getRange(final KeyParts keyParts) {
                Text start= null;
                if (keyParts.constraintPrefix != null ) {
                    start = keyParts.constraintPrefix;   // <-- start specific logic
                } else {
                    start = new Text(KeyParts.HASH_PREFIX_FOLLOWING);
                }
                final Text endAt = keyParts.getQueryKey();                   // <-- end specific logic
                //System.out.println("Scanning queryInstantBeforeInstant: from:" + KeyParts.toHumanString(start) + " up to:" + KeyParts.toHumanString(endAt));
                return new Range(start, true, endAt, false);
            }
        };
        final ScannerBase scanner = query.doQuery(queryInstant, constraints);
        return getContextIteratorWrapper(scanner, constraints.getContext());
    }

    /**
     * get statements where the date object is after the given queryInstant.
     */
    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryInstantAfterInstant(
            final TemporalInstant queryInstant, final StatementConstraints constraints)
            throws QueryEvaluationException {
        final Query query = new Query() {
            @Override
            public Range getRange(final KeyParts keyParts) {
                final Text start = Range.followingPrefix(keyParts.getQueryKey());  // <-- specific logic
                Text endAt = null;  // no constraints                            // <-- specific logic
                if (keyParts.constraintPrefix != null ) {
                    endAt = Range.followingPrefix(keyParts.constraintPrefix);
                }
                //System.out.println("Scanning queryInstantAfterInstant from after:" + KeyParts.toHumanString(start) + " up to:" + KeyParts.toHumanString(endAt));
                return new Range(start, true, endAt, false);
            }
        };
        final ScannerBase scanner = query.doQuery(queryInstant, constraints);
        return getContextIteratorWrapper(scanner, constraints.getContext());
    }

    /**
     * Get instances before a given interval. Returns queryInstantBeforeInstant with the interval's beginning time.
     */
    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryInstantBeforeInterval(
            final TemporalInterval givenInterval, final StatementConstraints contraints)
            throws QueryEvaluationException {
        return queryInstantBeforeInstant(givenInterval.getHasBeginning(), contraints);
    }

    /**
     * Get instances after a given interval. Returns queryInstantAfterInstant with the interval's end time.
     */
    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryInstantAfterInterval(
            final TemporalInterval givenInterval, final StatementConstraints contraints) throws QueryEvaluationException {
        return queryInstantAfterInstant(givenInterval.getHasEnd(), contraints);
    }

    /**
     * Get instances inside a given interval.
     * Returns after interval's beginning time, and before ending time,
     * exclusive (don't match the beginning and ending).
     */
    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryInstantInsideInterval(
            final TemporalInterval queryInterval, final StatementConstraints constraints)
            throws QueryEvaluationException {
        // get rows where the time is after the given interval's beginning time and before the ending time.
        final TemporalInterval theQueryInterval = queryInterval;
        final Query query = new Query() {
            private final TemporalInterval queryInterval = theQueryInterval;
            @Override
            public Range getRange(final KeyParts keyParts) {
                final Text start = Range.followingPrefix(new Text(keyParts.getQueryKey(queryInterval.getHasBeginning())));
                final Text endAt = new Text(keyParts.getQueryKey(queryInterval.getHasEnd())); // <-- end specific logic
                //System.out.println("Scanning queryInstantInsideInterval: from excluding:" + KeyParts.toHumanString(start) + " up to:" + KeyParts.toHumanString(endAt));
                return new Range(start, false, endAt, false);
            }
        };
        final ScannerBase scanner = query.doQuery(queryInterval.getHasBeginning(), constraints);
        return getContextIteratorWrapper(scanner, constraints.getContext());
    }
    /**
     * Get instances matching the beginning of a given interval.
     */
    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryInstantHasBeginningInterval(
            final TemporalInterval queryInterval, final StatementConstraints contraints)
            throws QueryEvaluationException {
        return queryInstantEqualsInstant(queryInterval.getHasBeginning(), contraints);
    }

    /**
     * Get instances matching the ending of a given interval.
     */
    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryInstantHasEndInterval(
            final TemporalInterval queryInterval, final StatementConstraints contraints)
            throws QueryEvaluationException {
        return queryInstantEqualsInstant(queryInterval.getHasEnd(), contraints);
    }

    /**
     * Get intervals stored in the repository matching the given interval.
     * Indexing Intervals  will probably change or be removed.
     * Currently predicate and subject constraints are filtered on the client.
     */
    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryIntervalEquals(
            final TemporalInterval query, final StatementConstraints contraints)
            throws QueryEvaluationException {
        final Scanner scanner = getScanner();
        if (scanner != null) {
            // get rows where the start and end match.
            final Range range = Range.prefix(new Text(query.getAsKeyBeginning()));
            scanner.setRange(range);
            if (contraints.hasContext()) {
                scanner.fetchColumn(new Text(contraints.getContext().toString()), new Text(KeyParts.CQ_BEGIN));
            } else {
                scanner.fetchColumn(new Text(""), new Text(KeyParts.CQ_BEGIN));
            }
        }
        // Iterator<Entry<Key, Value>> iter = scanner.iterator();
        // while (iter.hasNext()) {
        // System.out.println("queryIntervalEquals results:"+iter.next());
        // }
        //return getConstrainedIteratorWrapper(scanner, contraints);
        return getIteratorWrapper(scanner);
    }

    /**
     * find intervals stored in the repository before the given Interval. Find interval endings that are
     * before the given beginning.
     * Indexing Intervals  will probably change or be removed.
     * Currently predicate and subject constraints are filtered on the client.
     */
    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryIntervalBefore(
            final TemporalInterval queryInterval, final StatementConstraints constraints) throws QueryEvaluationException
    {
        final Scanner scanner = getScanner();
        if (scanner != null) {
            // get rows where the end date is less than the queryInterval.getBefore()
            final Range range = new Range(null, false, new Key(new Text(queryInterval.getHasBeginning().getAsKeyBytes())), false);
            scanner.setRange(range);
             if (constraints.hasContext()) {
                scanner.fetchColumn(new Text(constraints.getContext().toString()), new Text(KeyParts.CQ_END));
            } else {
                scanner.fetchColumn(new Text(""), new Text(KeyParts.CQ_END));
            }
        }
        return getIteratorWrapper(scanner);
    }

    /**
     * Interval after given interval.  Find intervals that begin after the endings of the given interval.
     * Use the special following prefix mechanism to avoid matching the beginning date.
     * Indexing Intervals  will probably change or be removed.
     * Currently predicate and subject and context constraints are filtered on the client.
     */
    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryIntervalAfter(
            final TemporalInterval queryInterval, final StatementConstraints constraints)
            throws QueryEvaluationException {

        final Scanner scanner = getScanner();
        if (scanner != null) {
            // get rows where the start date is greater than the queryInterval.getEnd()
            final Range range = new Range(new Key(Range.followingPrefix(new Text(queryInterval.getHasEnd().getAsKeyBytes()))), false, null, true);
            scanner.setRange(range);

            if (constraints.hasContext()) {
                scanner.fetchColumn(new Text(constraints.getContext().toString()), new Text(KeyParts.CQ_BEGIN));
            } else {
                scanner.fetchColumn(new Text(""), new Text(KeyParts.CQ_BEGIN));
            }
        }
        // TODO currently predicate, subject and context constraints are filtered on the clients
        return getIteratorWrapper(scanner);
    }
    // --
    // -- END of Query functions.  Next up, general stuff used by the queries above.
    // --

    /**
     * Allows passing range specific logic into doQuery.
     * Each query function implements an anonymous instance of this and calls it's doQuery().
     */
    abstract class Query {
        abstract protected Range getRange(KeyParts keyParts);

        public ScannerBase doQuery(final TemporalInstant queryInstant, final StatementConstraints constraints) throws QueryEvaluationException {
            // key is contraintPrefix + time, or just time.
            // Any constraints handled here, if the constraints are empty, the
            // thisKeyParts.contraintPrefix will be null.
            final List<KeyParts> keyParts = KeyParts.keyPartsForQuery(queryInstant, constraints);
            ScannerBase scanner = null;
            if (keyParts.size() > 1) {
                scanner = getBatchScanner();
            } else {
                scanner = getScanner();
            }

            final Collection<Range> ranges = new HashSet<Range>();
            KeyParts lastKeyParts = null;
            Range range = null;
            for (final KeyParts thisKeyParts : keyParts) {
                range = getRange(thisKeyParts);
                ranges.add(range);
                lastKeyParts = thisKeyParts;
            }
            //System.out.println("Scanning columns, cf:" + lastKeyParts.cf + "CQ:" + lastKeyParts.cq);
            scanner.fetchColumn(new Text(lastKeyParts.cf), new Text(lastKeyParts.cq));
            if (scanner instanceof BatchScanner) {
                ((BatchScanner) scanner).setRanges(ranges);
            } else if (range != null) {
                ((Scanner) scanner).setRange(range);
            }
            return scanner;
        }
    }

    /**
     * An iteration wrapper for a loaded scanner that is returned for each query above.
     *
     * @param scanner
     *            the results to iterate, then close.
     * @return an anonymous object that will iterate the resulting statements from a given scanner.
     */
    private static CloseableIteration<Statement, QueryEvaluationException> getIteratorWrapper(final ScannerBase scanner) {

        final Iterator<Entry<Key, Value>> i = scanner.iterator();

        return new CloseableIteration<Statement, QueryEvaluationException>() {
            @Override
            public boolean hasNext() {
                return i.hasNext();
            }

            @Override
            public Statement next() throws QueryEvaluationException {
                final Entry<Key, Value> entry = i.next();
                final Value v = entry.getValue();
                try {
                    final String dataString = Text.decode(v.get(), 0, v.getSize());
                    final Statement s = StatementSerializer.readStatement(dataString);
                    return s;
                } catch (final CharacterCodingException e) {
                    logger.error("Error decoding value=" + Arrays.toString(v.get()), e);
                    throw new QueryEvaluationException(e);
                } catch (final IOException e) {
                    logger.error("Error de-serializing statement, string=" + v.get(), e);
                    throw new QueryEvaluationException(e);
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("Remove not implemented");
            }

            @Override
            public void close() throws QueryEvaluationException {
                scanner.close();
            }
        };
    }


    /**
     * An iteration wrapper for a loaded scanner that is returned for partially supported interval queries above.
     *
     * @param scanner  the results to iterate, then close.
     * @param constraints  limit statements returned by next() to those matching the constraints.
     * @return an anonymous object that will iterate the resulting statements from a given scanner.
     * @throws QueryEvaluationException
     */
    private static CloseableIteration<Statement, QueryEvaluationException> getConstrainedIteratorWrapper(final Scanner scanner, final StatementConstraints constraints) {
        if (!constraints.hasContext() && !constraints.hasSubject() && !constraints.hasPredicates()) {
            return getIteratorWrapper(scanner);
        }
        return new ConstrainedIteratorWrapper(scanner) {
            @Override
            public boolean allowedBy(final Statement statement) {
                return allowedByConstraints(statement, constraints);
            }
        };
    }
    /**
     * An iteration wrapper for a loaded scanner that is returned for queries above.
     * Currently, this temporal index supports contexts only on the client, using this filter.
     *
     * @param scanner  the results to iterate, then close.
     * @param constraints  limit statements returned by next() to those matching the constraints.
     * @return an anonymous object that will iterate the resulting statements from a given scanner.
     * @throws QueryEvaluationException
     */
    private static CloseableIteration<Statement, QueryEvaluationException> getContextIteratorWrapper(final ScannerBase scanner, final Resource context) {
        if (context==null) {
            return getIteratorWrapper(scanner);
        }
        return new ConstrainedIteratorWrapper(scanner) {
            @Override
            public boolean allowedBy(final Statement statement) {
                return allowedByContext(statement,  context);
            }
        };
    }
    /**
     * Wrap a scanner in a iterator that will filter statements based on a boolean allowedBy().
     * If the allowedBy function returns false for the next statement, it is skipped.
     * This is used for to do client side, what the index cannot (yet) do on the server side.
     */
    abstract static class ConstrainedIteratorWrapper implements CloseableIteration<Statement, QueryEvaluationException> {
            private Statement nextStatement=null;
            private boolean isInitialized = false;
            final private Iterator<Entry<Key, Value>> i;
            final private ScannerBase scanner;

            ConstrainedIteratorWrapper(final ScannerBase scanner) {
                this.scanner = scanner;
                i=scanner.iterator();
            }
            @Override
            public boolean hasNext() throws QueryEvaluationException {
                if (!isInitialized) {
                    internalGetNext();
                }
                return (nextStatement != null) ;
            }

            @Override
            public Statement next() throws QueryEvaluationException {
                if (nextStatement==null) {
                    if (!isInitialized) {
                        internalGetNext();
                    }
                    if (nextStatement==null) {
                        throw new NoSuchElementException();
                    }
                }
                // use this one, then get the next one loaded.
                final Statement thisStatement = nextStatement;
                internalGetNext();
                return thisStatement;
            }

            /**
             * Gets the next statement meeting constraints and stores in nextStatement.
             * Sets null when all done, or on exception.
             * @throws QueryEvaluationException
             */
            private void internalGetNext()
                    throws QueryEvaluationException {
                isInitialized=true;
                nextStatement = null;  // Default on done or error.
                Statement statement = null;
                while (i.hasNext()) {
                    final Entry<Key, Value> entry = i.next();
                    final Value v = entry.getValue();
                    try {
                        final String dataString = Text.decode(v.get(), 0, v.getSize());
                        statement = StatementSerializer.readStatement(dataString);
                    } catch (final CharacterCodingException e) {
                        logger.error("Error decoding value=" + Arrays.toString(v.get()), e);
                        throw new QueryEvaluationException(e);
                    } catch (final IOException e) {
                        logger.error("Error de-serializing statement, string=" + v.get(), e);
                        throw new QueryEvaluationException(e);
                    }
                    if (allowedBy(statement)) {
                        nextStatement = statement;
                        return;
                    }
                }
            }
            public abstract boolean allowedBy(Statement s);

            @Override
            public void remove() {
                throw new UnsupportedOperationException("Remove not implemented");
            }

            @Override
            public void close() throws QueryEvaluationException {
                scanner.close();
            }
        }

    /**
     * Does the statement meet the constraints? Match predicate, subject, and context.
     * @param statement  Candidate statement to be allowed or not.
     * @param contraints  fields that are non-null must match the statement's components, otherwise it is not allowed.
     * @return true if the parts of the statement match the statementConstraints' parts.
     */
    protected static boolean allowedByConstraints(final Statement statement, final StatementConstraints constraints) {

        if (constraints.hasSubject() && ! constraints.getSubject().toString().equals(statement.getSubject().toString()))
            {System.out.println("Constrain subject: "+constraints.getSubject()+" != " + statement.getSubject()); return false;}
            //return false;

        if (! allowedByContext(statement, constraints.getContext()))
         {
            return false;
            //{System.out.println("Constrain context: "+constraints.getContext()+" != " + statement.getContext()); return false;}
        }

        if (constraints.hasPredicates() && ! constraints.getPredicates().contains(statement.getPredicate()))
         {
            return false;
            //{System.out.println("Constrain predicate: "+constraints.getPredicates()+" != " + statement.getPredicate()); return false;}
        }

        System.out.println("allow statement: "+ statement.toString());
        return true;
    }

    /**
     * Allow only if the context matches the statement.  This is a client side filter.
     * @param statement
     * @param context
     * @return
     */
    protected static boolean allowedByContext(final Statement statement, final Resource context) {
        return context==null || context.equals( statement.getContext() );
    }

    @Override
    public Set<URI> getIndexablePredicates() {

        return validPredicates;
    }

    /**
     * Flush the data to the batchwriter.
     * Throws a IOException as required by the flushable interface,
     * wrapping MutationsRejectedException.
     */
    @Override
    public void flush() throws IOException {
        try {
            mtbw.flush();
        } catch (final MutationsRejectedException e) {
            final String msg = "Error while flushing the batch writer.";
            logger.error(msg, e);
            throw new IOException(msg, e);
        }
    }

    /**
     * Close batchwriter.
     * Throws a IOException as required by the flushable interface,
     * wrapping MutationsRejectedException.
     */
    @Override
    public void close() throws IOException {
        try {

            mtbw.close();

        } catch (final MutationsRejectedException e) {
            final String msg = "Error while closing the batch writer.";
            logger.error(msg, e);
            throw new IOException(msg, e);
        }
    }



    @Override
    public String getTableName() {
       return makeTableName( ConfigUtils.getTablePrefix(conf) );
    }

    /**
     * Make the Accumulo table name used by this indexer for a specific instance of Rya.
     *
     * @param ryaInstanceName -  The name of the Rya instance the table name is for. (not null)
     * @return The Accumulo table name used by this indexer for a specific instance of Rya.
     */
    public static String makeTableName(final String ryaInstanceName) {
        requireNonNull(ryaInstanceName);
        return ryaInstanceName + TABLE_SUFFIX;
    }

    private void deleteStatement(final Statement statement) throws IOException, IllegalArgumentException {
        // if the predicate list is empty, accept all predicates.
        // Otherwise, make sure the predicate is on the "valid" list
        final boolean isValidPredicate = validPredicates.isEmpty() || validPredicates.contains(statement.getPredicate());
        if (!isValidPredicate || !(statement.getObject() instanceof Literal)) {
            return;
        }
        final DateTime[] indexDateTimes = new DateTime[2]; // 0 begin, 1 end of interval
        extractDateTime(statement, indexDateTimes);
        if (indexDateTimes[0] == null) {
            return;
        }

        // Remove this as an instant, or interval.
        try {
            if (indexDateTimes[1] != null) {
                final TemporalInterval interval = new TemporalInterval(new TemporalInstantRfc3339(indexDateTimes[0]), new TemporalInstantRfc3339(indexDateTimes[1]));
                removeInterval(temporalIndexBatchWriter, interval, statement);
            } else {
                final TemporalInstant instant = new TemporalInstantRfc3339(indexDateTimes[0]);
                removeInstant(temporalIndexBatchWriter, instant, statement);
            }
        } catch (final MutationsRejectedException e) {
            throw new IOException("While adding interval/instant for statement =" + statement, e);
        }
    }

    @Override
    public void deleteStatement(final RyaStatement statement) throws IllegalArgumentException, IOException {
        deleteStatement(RyaToRdfConversions.convertStatement(statement));
    }

    @Override
    public void init() {
        // TODO Auto-generated method stub

    }

    @Override
    public void setConnector(final Connector connector) {
        // TODO Auto-generated method stub

    }

    @Override
    public void destroy() {
        // TODO Auto-generated method stub

    }

    @Override
    public void purge(final RdfCloudTripleStoreConfiguration configuration) {
        // TODO Auto-generated method stub

    }

    @Override
    public void dropAndDestroy() {
        // TODO Auto-generated method stub

    }
}
