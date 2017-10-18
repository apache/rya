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
package org.apache.rya.indexing.accumulo.freetext;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.*;
import java.util.Map.Entry;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.keyfunctor.ColumnFamilyFunctor;
import org.apache.accumulo.core.iterators.user.IntersectingIterator;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.rya.accumulo.experimental.AbstractAccumuloIndexer;
import org.apache.rya.api.RdfTripleStoreConfiguration;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.resolver.RyaToRdfConversions;
import org.apache.rya.indexing.FreeTextIndexer;
import org.apache.rya.indexing.Md5Hash;
import org.apache.rya.indexing.StatementConstraints;
import org.apache.rya.indexing.StatementSerializer;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.accumulo.freetext.iterators.BooleanTreeIterator;
import org.apache.rya.indexing.accumulo.freetext.query.*;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.query.QueryEvaluationException;

import static java.util.Objects.requireNonNull;
import static org.apache.rya.indexing.accumulo.freetext.query.ASTNodeUtils.getNodeIterator;

/**
 * The {@link AccumuloFreeTextIndexer} stores and queries "free text" data from statements into tables in Accumulo. Specifically, this class
 * stores data into two different Accumulo Tables. This is the <b>document table</b> (default name: triplestore_text) and the <b>terms
 * table</b> (default name: triplestore_terms).
 * <p>
 * The document table stores the document (i.e. a triple statement), document properties, and the terms within the document. This is the
 * main table used for processing a text search by using document partitioned indexing. See {@link IntersectingIterator}.
 * <p>
 * For each document, the document table will store the following information:
 * <P>
 *
 * <pre>
 * Row (partition) | Column Family  | Column Qualifier | Value
 * ================+================+==================+==========
 * shardID         | d\x00          | documentHash     | Document
 * shardID         | s\x00Subject   | documentHash     | (empty)
 * shardID         | p\x00Predicate | documentHash     | (empty)
 * shardID         | o\x00Object    | documentHash     | (empty)
 * shardID         | c\x00Context   | documentHash     | (empty)
 * shardID         | t\x00token     | documentHash     | (empty)
 * </pre>
 * <p>
 * Note: documentHash is a sha256 Hash of the Document's Content
 * <p>
 * The terms table is used for expanding wildcard search terms. For each token in the document table, the table will store the following
 * information:
 *
 * <pre>
 * Row (partition)   | CF/CQ/Value
 * ==================+=============
 * l\x00token        | (empty)
 * r\x00Reversetoken | (empty)
 * </pre>
 * <p>
 * There are two prefixes in the table, "token list" (keys with an "l" prefix) and "reverse token list" (keys with a "r" prefix). This table
 * is uses the "token list" to expand foo* into terms like food, foot, and football. This table uses the "reverse token list" to expand *ar
 * into car, bar, and far.
 * <p>
 * Example: Given these three statements as inputs:
 *
 * <pre>
 *     <uri:paul> rdfs:label "paul smith"@en <uri:graph1>
 *     <uri:steve> rdfs:label "steven anthony miller"@en <uri:graph1>
 *     <uri:steve> rdfs:label "steve miller"@en <uri:graph1>
 * </pre>
 * <p>
 * Here's what the tables would look like: (Note: the hashes aren't real, the rows are not sorted, and the partition ids will vary.)
 * <p>
 * Triplestore_text
 *
 * <pre>
 * Row (partition) | Column Family                   | Column Qualifier | Value
 * ================+=================================+==================+==========
 * 000000          | d\x00                           | 08b3d233a        | uri:graph1x00uri:paul\x00rdfs:label\x00"paul smith"@en
 * 000000          | s\x00uri:paul                   | 08b3d233a        | (empty)
 * 000000          | p\x00rdfs:label                 | 08b3d233a        | (empty)
 * 000000          | o\x00"paul smith"@en            | 08b3d233a        | (empty)
 * 000000          | c\x00uri:graph1                 | 08b3d233a        | (empty)
 * 000000          | t\x00paul                       | 08b3d233a        | (empty)
 * 000000          | t\x00smith                      | 08b3d233a        | (empty)
 *
 * 000000          | d\x00                           | 3a575534b        | uri:graph1x00uri:steve\x00rdfs:label\x00"steven anthony miller"@en
 * 000000          | s\x00uri:steve                  | 3a575534b        | (empty)
 * 000000          | p\x00rdfs:label                 | 3a575534b        | (empty)
 * 000000          | o\x00"steven anthony miller"@en | 3a575534b        | (empty)
 * 000000          | c\x00uri:graph1                 | 3a575534b        | (empty)
 * 000000          | t\x00steven                     | 3a575534b        | (empty)
 * 000000          | t\x00anthony                    | 3a575534b        | (empty)
 * 000000          | t\x00miller                     | 3a575534b        | (empty)
 *
 * 000001          | d\x00                           | 7bf670d06        | uri:graph1x00uri:steve\x00rdfs:label\x00"steve miller"@en
 * 000001          | s\x00uri:steve                  | 7bf670d06        | (empty)
 * 000001          | p\x00rdfs:label                 | 7bf670d06        | (empty)
 * 000001          | o\x00"steve miller"@en          | 7bf670d06        | (empty)
 * 000001          | c\x00uri:graph1                 | 7bf670d06        | (empty)
 * 000001          | t\x00steve                      | 7bf670d06        | (empty)
 * 000001          | t\x00miller                     | 7bf670d06        | (empty)
 * </pre>
 * <p>
 * triplestore_terms
 * <p>
 *
 * <pre>
 * Row (partition)   | CF/CQ/Value
 * ==================+=============
 * l\x00paul         | (empty)
 * l\x00smith        | (empty)
 * l\x00steven       | (empty)
 * l\x00anthony      | (empty)
 * l\x00miller       | (empty)
 * l\x00steve        | (empty)
 * r\x00luap         | (empty)
 * r\x00htims        | (empty)
 * r\x00nevets       | (empty)
 * r\x00ynohtna      | (empty)
 * r\x00rellim       | (empty)
 * r\x00evets        | (empty)
 *
 * <pre>
 */
public class AccumuloFreeTextIndexer extends AbstractAccumuloIndexer implements FreeTextIndexer  {
    private static final String TABLE_SUFFIX_TERM = "freetext_term";

    private static final String TABLE_SUFFFIX_DOC = "freetext";

    private static final Logger logger = Logger.getLogger(AccumuloFreeTextIndexer.class);

    private static final boolean IS_TERM_TABLE_TOKEN_DELETION_ENABLED = true;

    private static final byte[] EMPTY_BYTES = new byte[] {};
    private static final Text EMPTY_TEXT = new Text(EMPTY_BYTES);
    private static final Value EMPTY_VALUE = new Value(EMPTY_BYTES);

    private Tokenizer tokenizer;

    private BatchWriter docTableBw;
    private BatchWriter termTableBw;
    private MultiTableBatchWriter mtbw;

    private int queryTermLimit;

    private int docTableNumPartitions;

    private Set<IRI> validPredicates;

    private Configuration conf;

    private boolean isInit = false;

    /**
     * Called by setConf to initialize query only.  
     * Use this alone if usage does not require writing.
     * For a writeable (store and delete) version of this, 
     * call setconf() and then setMultiTableBatchWriter(), then call init()
     * that is what the DAO does.
     * @throws AccumuloException
     * @throws AccumuloSecurityException
     * @throws TableNotFoundException
     * @throws TableExistsException
     */
    private void initInternal() throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
            TableExistsException {
        final String doctable = getFreeTextDocTablename(conf);
        final String termtable = getFreeTextTermTablename(conf);

        docTableNumPartitions = ConfigUtils.getFreeTextDocNumPartitions(conf);
        final int termTableNumPartitions = ConfigUtils.getFreeTextTermNumPartitions(conf);

        final TableOperations tableOps = ConfigUtils.getConnector(conf).tableOperations();

        // Create term table partitions
        final boolean createdTermTable = ConfigUtils.createTableIfNotExists(conf, termtable);
        if (createdTermTable && !ConfigUtils.useMockInstance(conf) && termTableNumPartitions > 0) {
            final TreeSet<Text> splits = new TreeSet<Text>();

            // split on the "Term List" and "Reverse Term list" boundary
            splits.add(new Text(ColumnPrefixes.getRevTermListColFam("")));

            // Symmetrically split the "Term List" and "Reverse Term list"
            final int numSubpartitions = ((termTableNumPartitions - 1) / 2);
            if (numSubpartitions > 0) {
                final int step = (26 / numSubpartitions);
                for (int i = 0; i < numSubpartitions; i++) {
                    final String nextChar = String.valueOf((char) ('a' + (step * i)));
                    splits.add(new Text(ColumnPrefixes.getTermListColFam(nextChar)));
                    splits.add(new Text(ColumnPrefixes.getRevTermListColFam(nextChar)));
                }
            }
            tableOps.addSplits(termtable, splits);
        }

        // Create document (text) table partitions
        final boolean createdDocTable = ConfigUtils.createTableIfNotExists(conf, doctable);
        if (createdDocTable && !ConfigUtils.useMockInstance(conf)) {
            final TreeSet<Text> splits = new TreeSet<Text>();
            for (int i = 0; i < docTableNumPartitions; i++) {
                splits.add(genPartition(i, docTableNumPartitions));
            }
            tableOps.addSplits(doctable, splits);

            // Add a tablet level Bloom filter for the Column Family.
            // This will allow us to quickly determine if a term is contained in a tablet.
            tableOps.setProperty(doctable, "table.bloom.key.functor", ColumnFamilyFunctor.class.getCanonicalName());
            tableOps.setProperty(doctable, "table.bloom.enabled", Boolean.TRUE.toString());
        }

        // Set mtbw by calling setMultiTableBatchWriter().  The DAO does this and manages flushing.
        // If you create it here, tests work, but a real Accumulo may lose writes due to unmanaged flushing.
        if (mtbw != null) {
	        docTableBw = mtbw.getBatchWriter(doctable);
	        termTableBw = mtbw.getBatchWriter(termtable);
        }
        tokenizer = ConfigUtils.getFreeTextTokenizer(conf);
        validPredicates = ConfigUtils.getFreeTextPredicates(conf);

        queryTermLimit = ConfigUtils.getFreeTextTermLimit(conf);
    }

    /**
     * setConf sets the configuration and then initializes for query only.  
     * Use this alone if usage does not require writing.
     * For a writeable (store and delete) version of this, 
     * call this and then setMultiTableBatchWriter(), then call init()
     * that is what the DAO does.
     */
    @Override
    public void setConf(final Configuration conf) {
        this.conf = conf;
        if (!isInit) {
            try {
                initInternal();
                isInit = true;
            } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException | TableExistsException e) {
                logger.warn("Unable to initialize index.  Throwing Runtime Exception. ", e);
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public Configuration getConf() {
        return conf;
    }


    private void storeStatement(final Statement statement) throws IOException {
        Objects.requireNonNull(mtbw, "Freetext indexer attempting to store, but setMultiTableBatchWriter() was not set.");

        // if the predicate list is empty, accept all predicates.
        // Otherwise, make sure the predicate is on the "valid" list
        final boolean isValidPredicate = validPredicates.isEmpty() || validPredicates.contains(statement.getPredicate());

        if (isValidPredicate && (statement.getObject() instanceof Literal)) {

            // Get the tokens
            final String text = statement.getObject().stringValue().toLowerCase();
            final SortedSet<String> tokens = tokenizer.tokenize(text);

            if (!tokens.isEmpty()) {
                // Get Document Data
                final String docContent = StatementSerializer.writeStatement(statement);

                final String docId = Md5Hash.md5Base64(docContent);

                // Setup partition
                final Text partition = genPartition(docContent.hashCode(), docTableNumPartitions);

                final Mutation docTableMut = new Mutation(partition);
                final List<Mutation> termTableMutations = new ArrayList<Mutation>();

                final Text docIdText = new Text(docId);

                // Store the Document Data
                docTableMut.put(ColumnPrefixes.DOCS_CF_PREFIX, docIdText, new Value(docContent.getBytes(Charsets.UTF_8)));

                // index the statement parts
                docTableMut.put(ColumnPrefixes.getSubjColFam(statement), docIdText, EMPTY_VALUE);
                docTableMut.put(ColumnPrefixes.getPredColFam(statement), docIdText, EMPTY_VALUE);
                docTableMut.put(ColumnPrefixes.getObjColFam(statement), docIdText, EMPTY_VALUE);
                docTableMut.put(ColumnPrefixes.getContextColFam(statement), docIdText, EMPTY_VALUE);

                // index the statement terms
                for (final String token : tokens) {
                    // tie the token to the document
                    docTableMut.put(ColumnPrefixes.getTermColFam(token), docIdText, EMPTY_VALUE);

                    // store the term in the term table (useful for wildcard searches)
                    termTableMutations.add(createEmptyPutMutation(ColumnPrefixes.getTermListColFam(token)));
                    termTableMutations.add(createEmptyPutMutation(ColumnPrefixes.getRevTermListColFam(token)));
                }

                // write the mutations
                try {
                    docTableBw.addMutation(docTableMut);
                    termTableBw.addMutations(termTableMutations);
                } catch (final MutationsRejectedException e) {
                    logger.error("error adding mutation", e);
                    throw new IOException(e);
                }

            }

        }
    }

    @Override
    public void storeStatement(final RyaStatement statement) throws IOException {
        storeStatement(RyaToRdfConversions.convertStatement(statement));
    }

    private static Mutation createEmptyPutMutation(final Text row) {
        final Mutation m = new Mutation(row);
        m.put(EMPTY_TEXT, EMPTY_TEXT, EMPTY_VALUE);
        return m;
    }

    private static Mutation createEmptyPutDeleteMutation(final Text row) {
        final Mutation m = new Mutation(row);
        m.putDelete(EMPTY_TEXT, EMPTY_TEXT);
        return m;
    }

    private static Text genPartition(final int partition, final int numParitions) {
        final int length = Integer.toString(numParitions).length();
        return new Text(String.format("%0" + length + "d", Math.abs(partition % numParitions)));
    }

    @Override
    public Set<IRI> getIndexablePredicates() {
        return validPredicates;
    }

    /** {@inheritDoc} */
    @Override
    public void flush() throws IOException {
        try {
            mtbw.flush();
        } catch (final MutationsRejectedException e) {
            logger.error("error flushing the batch writer", e);
            throw new IOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
        try {
        	if (mtbw!=null)
        		mtbw.close();
        } catch (final MutationsRejectedException e) {
            logger.error("error closing the batch writer", e);
            throw new IOException(e);
        }
    }

    private Set<String> unrollWildcard(final String string, final boolean reverse) throws IOException {
        final Scanner termTableScan = getScanner(getFreeTextTermTablename(conf));

        final Set<String> unrolledTerms = new HashSet<String>();

        Text queryTerm;
        if (reverse) {
            final String t = StringUtils.removeStart(string, "*").toLowerCase();
            queryTerm = ColumnPrefixes.getRevTermListColFam(t);
        } else {
            final String t = StringUtils.removeEnd(string, "*").toLowerCase();
            queryTerm = ColumnPrefixes.getTermListColFam(t);
        }

        // perform query and read results
        termTableScan.setRange(Range.prefix(queryTerm));

        for (final Entry<Key, Value> e : termTableScan) {
            final String term = ColumnPrefixes.removePrefix(e.getKey().getRow()).toString();
            if (reverse) {
                unrolledTerms.add(StringUtils.reverse(term));
            } else {
                unrolledTerms.add(term);
            }
        }

        if (unrolledTerms.isEmpty()) {
            // put in a placeholder term that will never be in the index.
            unrolledTerms.add("\1\1\1");
        }

        return unrolledTerms;
    }

    private void unrollWildcards(final SimpleNode node) throws IOException {
        if (node instanceof ASTExpression || node instanceof ASTSimpleNode) {
            for (final SimpleNode n : getNodeIterator(node)) {
                unrollWildcards(n);
            }
        } else if (node instanceof ASTTerm) {
            final ASTTerm term = (ASTTerm) node;
            final boolean isWildTerm = term.getType().equals(ASTTerm.WILDTERM);
            final boolean isPreWildTerm = term.getType().equals(ASTTerm.PREFIXTERM);
            if (isWildTerm || isPreWildTerm) {
                final Set<String> unrolledTerms = unrollWildcard(term.getTerm(), isPreWildTerm);

                // create a new expression
                final ASTExpression newExpression = new ASTExpression(QueryParserTreeConstants.JJTEXPRESSION);
                newExpression.setType(ASTExpression.OR);
                newExpression.setNotFlag(term.isNotFlag());

                for (final String unrolledTerm : unrolledTerms) {
                    final ASTTerm t = new ASTTerm(QueryParserTreeConstants.JJTTERM);
                    t.setNotFlag(false);
                    t.setTerm(unrolledTerm);
                    t.setType(ASTTerm.TERM);
                    ASTNodeUtils.pushChild(newExpression, t);
                }

                // replace "term" node with "expression" node in "term" node parent
                final SimpleNode parent = (SimpleNode) term.jjtGetParent();
                final int index = ASTNodeUtils.getChildIndex(parent, term);

                Validate.isTrue(index >= 0, "child not found in parent");

                parent.jjtAddChild(newExpression, index);
            }

        } else {
            throw new IllegalArgumentException("Node is of unknown type: " + node.getClass().getName());
        }
    }

    private Scanner getScanner(final String tablename) throws IOException {
        try {
            return ConfigUtils.createScanner(tablename, conf);
        } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
            logger.error("Error connecting to " + tablename);
            throw new IOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryText(final String query, final StatementConstraints contraints)
            throws IOException {
        final Scanner docTableScan = getScanner(getFreeTextDocTablename(conf));

        // test the query to see if it's parses correctly.
        SimpleNode root = parseQuery(query);

        // unroll any wildcard nodes before it goes to the server
        unrollWildcards(root);

        final String unrolledQuery = ASTNodeUtils.serializeExpression(root);

        // Add S P O C constraints to query
        final StringBuilder constrainedQuery = new StringBuilder("(" + unrolledQuery + ")");

        if (contraints.hasSubject()) {
            constrainedQuery.append(" AND ");
            constrainedQuery.append(ColumnPrefixes.getSubjColFam(contraints.getSubject().toString()).toString());
        }
        if (contraints.hasContext()) {
            constrainedQuery.append(" AND ");
            constrainedQuery.append(ColumnPrefixes.getContextColFam(contraints.getContext().toString()).toString());
        }
        if (contraints.hasPredicates()) {
            constrainedQuery.append(" AND (");
            final List<String> predicates = new ArrayList<String>();
            for (final IRI u : contraints.getPredicates()) {
                predicates.add(ColumnPrefixes.getPredColFam(u.stringValue()).toString());
            }
            constrainedQuery.append(StringUtils.join(predicates, " OR "));
            constrainedQuery.append(")");
        }

        // Verify that the query is a reasonable size
        root = parseQuery(constrainedQuery.toString());
        final int termCount = ASTNodeUtils.termCount(root);

        if (termCount > queryTermLimit) {
            throw new IOException("Query contains too many terms.  Term limit: " + queryTermLimit + ".  Term Count: " + termCount);
        }

        // perform query
        docTableScan.clearScanIterators();
        docTableScan.clearColumns();

        final int iteratorPriority = 20;
        final String iteratorName = "booleanTree";
        final IteratorSetting ii = new IteratorSetting(iteratorPriority, iteratorName, BooleanTreeIterator.class);
        BooleanTreeIterator.setQuery(ii, constrainedQuery.toString());
        docTableScan.addScanIterator(ii);
        docTableScan.setRange(new Range());

        return getIteratorWrapper(docTableScan);
    }

    private static CloseableIteration<Statement, QueryEvaluationException> getIteratorWrapper(final Scanner s) {

        final Iterator<Entry<Key, Value>> i = s.iterator();

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
                    logger.error("Error decoding value", e);
                    throw new QueryEvaluationException(e);
                } catch (final IOException e) {
                    logger.error("Error deserializing statement", e);
                    throw new QueryEvaluationException(e);
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("Remove not implemented");
            }

            @Override
            public void close() throws QueryEvaluationException {
                if (s != null) {
                    s.close();
                }
            }
        };
    }

    /**
     * Simple adapter that parses the query using {@link QueryParser}. Note: any checked exceptions thrown by {@link QueryParser} are
     * re-thrown as {@link IOException}s.
     *
     * @param query
     * @return
     * @throws IOException
     */
    private static SimpleNode parseQuery(final String query) throws IOException {
        SimpleNode root = null;
        try {
            root = QueryParser.parse(query);
        } catch (final ParseException e) {
            logger.error("Parser Exception on Client Side. Query: " + query, e);
            throw new IOException(e);
        } catch (final TokenMgrError e) {
            logger.error("Token Manager Exception on Client Side. Query: " + query, e);
            throw new IOException(e);
        }
        return root;
    }

    /**
     * Get Free Text Document index table's name
     * Use the two table version of this below. This one is required by base class.
     */
    @Override
    public String getTableName() {
       return getFreeTextDocTablename(conf);
    }

    /**
     * Make the Accumulo table names used by this indexer for a specific instance of Rya.
     *
     * @param conf - The Rya configuration that specifies which instance of Rya
     *   the table names will be built for. (not null)
     * @return The Accumulo table names used by this indexer for a specific instance of Rya.
     */
    public static List<String> getTableNames(final Configuration conf) {
        requireNonNull(conf);
        return Collections.unmodifiableList(
                makeTableNames(ConfigUtils.getTablePrefix(conf) ) );
    }

    /**
     * Get the Document index's table name.
     *
     * @param conf - The Rya configuration that specifies which instance of Rya
     *   the table names will be built for. (not null)
     * @return The Free Text Document index's Accumulo table name for the Rya instance.
     */
    public static String getFreeTextDocTablename(final Configuration conf) {
        requireNonNull(conf);
        return makeFreeTextDocTablename( ConfigUtils.getTablePrefix(conf) );
    }

    @Override
	public void setMultiTableBatchWriter(MultiTableBatchWriter writer) throws IOException {
        mtbw = writer;
	}


	/**
     * Get the Term index's table name.
     *
     * @param conf - The Rya configuration that specifies which instance of Rya
     *   the table names will be built for. (not null)
     * @return The Free Text Term index's Accumulo table name for the Rya instance.
     */
    public static String getFreeTextTermTablename(final Configuration conf) {
        requireNonNull(conf);
        return makeFreeTextTermTablename( ConfigUtils.getTablePrefix(conf) );
    }

    /**
     * Make the Accumulo table names used by this indexer for a specific instance of Rya.
     *
     * @param ryaInstanceName - The name of the Rya instance the table names are for. (not null)
     * @return The Accumulo table names used by this indexer for a specific instance of Rya.
     */
    public static List<String> makeTableNames(final String ryaInstanceName) {
        requireNonNull(ryaInstanceName);
        return Lists.newArrayList(
                makeFreeTextDocTablename(ryaInstanceName),
                makeFreeTextTermTablename(ryaInstanceName));
    }

    /**
     * Make the Document index's table name.
     *
     * @param ryaInstanceName - The name of the Rya instance the table names are for. (not null)
     * @return The Free Text Document index's Accumulo table name for the Rya instance.
     */
    public static String makeFreeTextDocTablename(final String ryaInstanceName) {
        requireNonNull(ryaInstanceName);
        return ryaInstanceName + TABLE_SUFFFIX_DOC;
    }

    /**
     * Make the Term index's table name.
     *
     * @param ryaInstanceName - The name of the Rya instance the table names are for. (not null)
     * @return The Free Text Term index's Accumulo table name for the Rya instance.
     */
    public static String makeFreeTextTermTablename(final String ryaInstanceName) {
        requireNonNull(ryaInstanceName);
        return ryaInstanceName + TABLE_SUFFIX_TERM;
    }

    private void deleteStatement(final Statement statement) throws IOException {
        Objects.requireNonNull(mtbw, "Freetext indexer attempting to delete, but setMultiTableBatchWriter() was not set.");

        // if the predicate list is empty, accept all predicates.
        // Otherwise, make sure the predicate is on the "valid" list
        final boolean isValidPredicate = validPredicates.isEmpty() || validPredicates.contains(statement.getPredicate());

        if (isValidPredicate && (statement.getObject() instanceof Literal)) {

            // Get the tokens
            final String text = statement.getObject().stringValue().toLowerCase();
            final SortedSet<String> tokens = tokenizer.tokenize(text);

            if (!tokens.isEmpty()) {
                // Get Document Data
                final String docContent = StatementSerializer.writeStatement(statement);

                final String docId = Md5Hash.md5Base64(docContent);

                // Setup partition
                final Text partition = genPartition(docContent.hashCode(), docTableNumPartitions);

                final Mutation docTableMut = new Mutation(partition);
                final List<Mutation> termTableMutations = new ArrayList<Mutation>();

                final Text docIdText = new Text(docId);

                // Delete the Document Data
                docTableMut.putDelete(ColumnPrefixes.DOCS_CF_PREFIX, docIdText);

                // Delete the statement parts in index
                docTableMut.putDelete(ColumnPrefixes.getSubjColFam(statement), docIdText);
                docTableMut.putDelete(ColumnPrefixes.getPredColFam(statement), docIdText);
                docTableMut.putDelete(ColumnPrefixes.getObjColFam(statement), docIdText);
                docTableMut.putDelete(ColumnPrefixes.getContextColFam(statement), docIdText);


                // Delete the statement terms in index
                for (final String token : tokens) {
                    if (IS_TERM_TABLE_TOKEN_DELETION_ENABLED) {
                        final int rowId = Integer.parseInt(partition.toString());
                        final boolean doesTermExistInOtherDocs = doesTermExistInOtherDocs(token, rowId, docIdText);
                        // Only delete the term from the term table if it doesn't appear in other docs
                        if (!doesTermExistInOtherDocs) {
                            // Delete the term in the term table
                            termTableMutations.add(createEmptyPutDeleteMutation(ColumnPrefixes.getTermListColFam(token)));
                            termTableMutations.add(createEmptyPutDeleteMutation(ColumnPrefixes.getRevTermListColFam(token)));
                        }
                    }

                    // Un-tie the token to the document
                    docTableMut.putDelete(ColumnPrefixes.getTermColFam(token), docIdText);
                }

                // write the mutations
                try {
                    docTableBw.addMutation(docTableMut);
                    termTableBw.addMutations(termTableMutations);
                } catch (final MutationsRejectedException e) {
                    logger.error("error adding mutation", e);
                    throw new IOException(e);
                }

            }
        }
    }

    @Override
    public void deleteStatement(final RyaStatement statement) throws IOException {
        deleteStatement(RyaToRdfConversions.convertStatement(statement));
    }

    /**
     * Checks to see if the provided term appears in other documents.
     * @param term the term to search for.
     * @param currentDocId the current document ID that the search term exists in.
     * @return {@code true} if the term was found in other documents. {@code false} otherwise.
     */
    private boolean doesTermExistInOtherDocs(final String term, final int currentDocId, final Text docIdText) {
        try {
            final String freeTextDocTableName = getFreeTextDocTablename(conf);
            final Scanner scanner = getScanner(freeTextDocTableName);

            final String t = StringUtils.removeEnd(term, "*").toLowerCase();
            final Text queryTerm = ColumnPrefixes.getTermColFam(t);

            // perform query and read results
            scanner.fetchColumnFamily(queryTerm);

            for (final Entry<Key, Value> entry : scanner) {
                final Key key = entry.getKey();
                final Text row = key.getRow();
                final int rowId = Integer.parseInt(row.toString());
                // We only want to check other documents from the one we're deleting
                if (rowId != currentDocId) {
                    final Text columnFamily = key.getColumnFamily();
                    final String columnFamilyValue = columnFamily.toString();
                    // Check that the value has the term prefix
                    if (columnFamilyValue.startsWith(ColumnPrefixes.TERM_CF_PREFIX.toString())) {
                        final Text text = ColumnPrefixes.removePrefix(columnFamily);
                        final String value = text.toString();
                        if (value.equals(term)) {
                            return true;
                        }
                    }
                }
            }
        } catch (final IOException e) {
            logger.error("Error searching for the existance of the term in other documents", e);
        }
        return false;
    }


	/** 
	 * called by the DAO after setting the mtbw.
	 * The rest of the initilization is done by setConf()
	 */
    @Override
	public void init() {
        Objects.requireNonNull(mtbw, "Freetext indexer failed to initialize temporal index, setMultiTableBatchWriter() was not set.");
        Objects.requireNonNull(conf, "Freetext indexer failed to initialize temporal index, setConf() was not set.");
        try {
			docTableBw = mtbw.getBatchWriter(getFreeTextDocTablename(conf));
			termTableBw = mtbw.getBatchWriter(getFreeTextTermTablename(conf));
		} catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
			logger.error("Unable to initialize index.  Throwing Runtime Exception. ", e);
            throw new RuntimeException(e);		
        }
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
	public void purge(final RdfTripleStoreConfiguration configuration) {
		// TODO Auto-generated method stub

	}


	@Override
	public void dropAndDestroy() {
		// TODO Auto-generated method stub

	}
}
