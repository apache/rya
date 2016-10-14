package mvm.rya.indexing.accumulo.freetext;

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



import static mvm.rya.indexing.accumulo.freetext.query.ASTNodeUtils.getNodeIterator;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
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
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.query.QueryEvaluationException;

import com.google.common.base.Charsets;

import info.aduna.iteration.CloseableIteration;
import mvm.rya.accumulo.experimental.AbstractAccumuloIndexer;
import mvm.rya.api.RdfCloudTripleStoreConfiguration;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.resolver.RyaToRdfConversions;
import mvm.rya.indexing.FreeTextIndexer;
import mvm.rya.indexing.Md5Hash;
import mvm.rya.indexing.StatementConstraints;
import mvm.rya.indexing.StatementSerializer;
import mvm.rya.indexing.accumulo.ConfigUtils;
import mvm.rya.indexing.accumulo.freetext.iterators.BooleanTreeIterator;
import mvm.rya.indexing.accumulo.freetext.query.ASTExpression;
import mvm.rya.indexing.accumulo.freetext.query.ASTNodeUtils;
import mvm.rya.indexing.accumulo.freetext.query.ASTSimpleNode;
import mvm.rya.indexing.accumulo.freetext.query.ASTTerm;
import mvm.rya.indexing.accumulo.freetext.query.ParseException;
import mvm.rya.indexing.accumulo.freetext.query.QueryParser;
import mvm.rya.indexing.accumulo.freetext.query.QueryParserTreeConstants;
import mvm.rya.indexing.accumulo.freetext.query.SimpleNode;
import mvm.rya.indexing.accumulo.freetext.query.TokenMgrError;

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

    private Set<URI> validPredicates;

    private Configuration conf;

    private boolean isInit = false;


    private void initInternal() throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
            TableExistsException {
        String doctable = getFreeTextDocTablename(conf);
        String termtable = getFreeTextTermTablename(conf);

        docTableNumPartitions = ConfigUtils.getFreeTextDocNumPartitions(conf);
        int termTableNumPartitions = ConfigUtils.getFreeTextTermNumPartitions(conf);

        TableOperations tableOps = ConfigUtils.getConnector(conf).tableOperations();

        // Create term table partitions
        boolean createdTermTable = ConfigUtils.createTableIfNotExists(conf, termtable);
        if (createdTermTable && !ConfigUtils.useMockInstance(conf) && termTableNumPartitions > 0) {
            TreeSet<Text> splits = new TreeSet<Text>();

            // split on the "Term List" and "Reverse Term list" boundary
            splits.add(new Text(ColumnPrefixes.getRevTermListColFam("")));

            // Symmetrically split the "Term List" and "Reverse Term list"
            int numSubpartitions = ((termTableNumPartitions - 1) / 2);
            if (numSubpartitions > 0) {
                int step = (26 / numSubpartitions);
                for (int i = 0; i < numSubpartitions; i++) {
                    String nextChar = String.valueOf((char) ('a' + (step * i)));
                    splits.add(new Text(ColumnPrefixes.getTermListColFam(nextChar)));
                    splits.add(new Text(ColumnPrefixes.getRevTermListColFam(nextChar)));
                }
            }
            tableOps.addSplits(termtable, splits);
        }

        // Create document (text) table partitions
        boolean createdDocTable = ConfigUtils.createTableIfNotExists(conf, doctable);
        if (createdDocTable && !ConfigUtils.useMockInstance(conf)) {
            TreeSet<Text> splits = new TreeSet<Text>();
            for (int i = 0; i < docTableNumPartitions; i++) {
                splits.add(genPartition(i, docTableNumPartitions));
            }
            tableOps.addSplits(doctable, splits);

            // Add a tablet level Bloom filter for the Column Family.
            // This will allow us to quickly determine if a term is contained in a tablet.
            tableOps.setProperty(doctable, "table.bloom.key.functor", ColumnFamilyFunctor.class.getCanonicalName());
            tableOps.setProperty(doctable, "table.bloom.enabled", Boolean.TRUE.toString());
        }

        mtbw = ConfigUtils.createMultitableBatchWriter(conf);

        docTableBw = mtbw.getBatchWriter(doctable);
        termTableBw = mtbw.getBatchWriter(termtable);

        tokenizer = ConfigUtils.getFreeTextTokenizer(conf);
        validPredicates = ConfigUtils.getFreeTextPredicates(conf);

        queryTermLimit = ConfigUtils.getFreeTextTermLimit(conf);
    }


  //initialization occurs in setConf because index is created using reflection
    @Override
    public void setConf(Configuration conf) {
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
        return this.conf;
    }


    private void storeStatement(Statement statement) throws IOException {
        // if the predicate list is empty, accept all predicates.
        // Otherwise, make sure the predicate is on the "valid" list
        boolean isValidPredicate = validPredicates.isEmpty() || validPredicates.contains(statement.getPredicate());

        if (isValidPredicate && (statement.getObject() instanceof Literal)) {

            // Get the tokens
            String text = statement.getObject().stringValue().toLowerCase();
            SortedSet<String> tokens = tokenizer.tokenize(text);

            if (!tokens.isEmpty()) {
                // Get Document Data
                String docContent = StatementSerializer.writeStatement(statement);

                String docId = Md5Hash.md5Base64(docContent);

                // Setup partition
                Text partition = genPartition(docContent.hashCode(), docTableNumPartitions);

                Mutation docTableMut = new Mutation(partition);
                List<Mutation> termTableMutations = new ArrayList<Mutation>();

                Text docIdText = new Text(docId);

                // Store the Document Data
                docTableMut.put(ColumnPrefixes.DOCS_CF_PREFIX, docIdText, new Value(docContent.getBytes(Charsets.UTF_8)));

                // index the statement parts
                docTableMut.put(ColumnPrefixes.getSubjColFam(statement), docIdText, EMPTY_VALUE);
                docTableMut.put(ColumnPrefixes.getPredColFam(statement), docIdText, EMPTY_VALUE);
                docTableMut.put(ColumnPrefixes.getObjColFam(statement), docIdText, EMPTY_VALUE);
                docTableMut.put(ColumnPrefixes.getContextColFam(statement), docIdText, EMPTY_VALUE);

                // index the statement terms
                for (String token : tokens) {
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
                } catch (MutationsRejectedException e) {
                    logger.error("error adding mutation", e);
                    throw new IOException(e);
                }

            }

        }
    }

    @Override
    public void storeStatement(RyaStatement statement) throws IOException {
        storeStatement(RyaToRdfConversions.convertStatement(statement));
    }

    private static Mutation createEmptyPutMutation(Text row) {
        Mutation m = new Mutation(row);
        m.put(EMPTY_TEXT, EMPTY_TEXT, EMPTY_VALUE);
        return m;
    }

    private static Mutation createEmptyPutDeleteMutation(Text row) {
        Mutation m = new Mutation(row);
        m.putDelete(EMPTY_TEXT, EMPTY_TEXT);
        return m;
    }

    private static Text genPartition(int partition, int numParitions) {
        int length = Integer.toString(numParitions).length();
        return new Text(String.format("%0" + length + "d", Math.abs(partition % numParitions)));
    }

    @Override
    public Set<URI> getIndexablePredicates() {
        return validPredicates;
    }

    /** {@inheritDoc} */
    @Override
    public void flush() throws IOException {
        try {
            mtbw.flush();
        } catch (MutationsRejectedException e) {
            logger.error("error flushing the batch writer", e);
            throw new IOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
        try {
            mtbw.close();
        } catch (MutationsRejectedException e) {
            logger.error("error closing the batch writer", e);
            throw new IOException(e);
        }
    }

    private Set<String> unrollWildcard(String string, boolean reverse) throws IOException {
        Scanner termTableScan = getScanner(getFreeTextTermTablename(conf));

        Set<String> unrolledTerms = new HashSet<String>();

        Text queryTerm;
        if (reverse) {
            String t = StringUtils.removeStart(string, "*").toLowerCase();
            queryTerm = ColumnPrefixes.getRevTermListColFam(t);
        } else {
            String t = StringUtils.removeEnd(string, "*").toLowerCase();
            queryTerm = ColumnPrefixes.getTermListColFam(t);
        }

        // perform query and read results
        termTableScan.setRange(Range.prefix(queryTerm));

        for (Entry<Key, Value> e : termTableScan) {
            String term = ColumnPrefixes.removePrefix(e.getKey().getRow()).toString();
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

    private void unrollWildcards(SimpleNode node) throws IOException {
        if (node instanceof ASTExpression || node instanceof ASTSimpleNode) {
            for (SimpleNode n : getNodeIterator(node)) {
                unrollWildcards(n);
            }
        } else if (node instanceof ASTTerm) {
            ASTTerm term = (ASTTerm) node;
            boolean isWildTerm = term.getType().equals(ASTTerm.WILDTERM);
            boolean isPreWildTerm = term.getType().equals(ASTTerm.PREFIXTERM);
            if (isWildTerm || isPreWildTerm) {
                Set<String> unrolledTerms = unrollWildcard(term.getTerm(), isPreWildTerm);

                // create a new expression
                ASTExpression newExpression = new ASTExpression(QueryParserTreeConstants.JJTEXPRESSION);
                newExpression.setType(ASTExpression.OR);
                newExpression.setNotFlag(term.isNotFlag());

                for (String unrolledTerm : unrolledTerms) {
                    ASTTerm t = new ASTTerm(QueryParserTreeConstants.JJTTERM);
                    t.setNotFlag(false);
                    t.setTerm(unrolledTerm);
                    t.setType(ASTTerm.TERM);
                    ASTNodeUtils.pushChild(newExpression, t);
                }

                // replace "term" node with "expression" node in "term" node parent
                SimpleNode parent = (SimpleNode) term.jjtGetParent();
                int index = ASTNodeUtils.getChildIndex(parent, term);

                Validate.isTrue(index >= 0, "child not found in parent");

                parent.jjtAddChild(newExpression, index);
            }

        } else {
            throw new IllegalArgumentException("Node is of unknown type: " + node.getClass().getName());
        }
    }

    private Scanner getScanner(String tablename) throws IOException {
        try {
            return ConfigUtils.createScanner(tablename, conf);
        } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
            logger.error("Error connecting to " + tablename);
            throw new IOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryText(String query, StatementConstraints contraints)
            throws IOException {
        Scanner docTableScan = getScanner(getFreeTextDocTablename(conf));

        // test the query to see if it's parses correctly.
        SimpleNode root = parseQuery(query);

        // unroll any wildcard nodes before it goes to the server
        unrollWildcards(root);

        String unrolledQuery = ASTNodeUtils.serializeExpression(root);

        // Add S P O C constraints to query
        StringBuilder constrainedQuery = new StringBuilder("(" + unrolledQuery + ")");

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
            List<String> predicates = new ArrayList<String>();
            for (URI u : contraints.getPredicates()) {
                predicates.add(ColumnPrefixes.getPredColFam(u.stringValue()).toString());
            }
            constrainedQuery.append(StringUtils.join(predicates, " OR "));
            constrainedQuery.append(")");
        }

        // Verify that the query is a reasonable size
        root = parseQuery(constrainedQuery.toString());
        int termCount = ASTNodeUtils.termCount(root);

        if (termCount > queryTermLimit) {
            throw new IOException("Query contains too many terms.  Term limit: " + queryTermLimit + ".  Term Count: " + termCount);
        }

        // perform query
        docTableScan.clearScanIterators();
        docTableScan.clearColumns();

        int iteratorPriority = 20;
        String iteratorName = "booleanTree";
        IteratorSetting ii = new IteratorSetting(iteratorPriority, iteratorName, BooleanTreeIterator.class);
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
                Entry<Key, Value> entry = i.next();
                Value v = entry.getValue();
                try {
                    String dataString = Text.decode(v.get(), 0, v.getSize());
                    Statement s = StatementSerializer.readStatement(dataString);
                    return s;
                } catch (CharacterCodingException e) {
                    logger.error("Error decoding value", e);
                    throw new QueryEvaluationException(e);
                } catch (IOException e) {
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
    private static SimpleNode parseQuery(String query) throws IOException {
        SimpleNode root = null;
        try {
            root = QueryParser.parse(query);
        } catch (ParseException e) {
            logger.error("Parser Exception on Client Side. Query: " + query, e);
            throw new IOException(e);
        } catch (TokenMgrError e) {
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
     * Get all the tables used by this index.
     * @param conf configuration map
     * @return an unmodifiable list of all the table names.
     */
    public static List<String> getTableNames(Configuration conf) {
        return Collections.unmodifiableList( Arrays.asList( 
                getFreeTextDocTablename(conf),
                getFreeTextTermTablename(conf) ));
    }
    
    /**
     * Get the Document index's table name.
     * @param conf
     * @return the Free Text Document index table's name
     */
    public static String getFreeTextDocTablename(Configuration conf) {
        return mvm.rya.indexing.accumulo.ConfigUtils.getTablePrefix(conf)  + TABLE_SUFFFIX_DOC;
    }

    /**
     * Get the Term index's table name.
     * @param conf
     * @return the Free Text Term index table's name
     */
    public static String getFreeTextTermTablename(Configuration conf) {
        return mvm.rya.indexing.accumulo.ConfigUtils.getTablePrefix(conf)  + TABLE_SUFFIX_TERM;
    }

    private void deleteStatement(Statement statement) throws IOException {
        // if the predicate list is empty, accept all predicates.
        // Otherwise, make sure the predicate is on the "valid" list
        boolean isValidPredicate = validPredicates.isEmpty() || validPredicates.contains(statement.getPredicate());

        if (isValidPredicate && (statement.getObject() instanceof Literal)) {

            // Get the tokens
            String text = statement.getObject().stringValue().toLowerCase();
            SortedSet<String> tokens = tokenizer.tokenize(text);

            if (!tokens.isEmpty()) {
                // Get Document Data
                String docContent = StatementSerializer.writeStatement(statement);

                String docId = Md5Hash.md5Base64(docContent);

                // Setup partition
                Text partition = genPartition(docContent.hashCode(), docTableNumPartitions);

                Mutation docTableMut = new Mutation(partition);
                List<Mutation> termTableMutations = new ArrayList<Mutation>();

                Text docIdText = new Text(docId);

                // Delete the Document Data
                docTableMut.putDelete(ColumnPrefixes.DOCS_CF_PREFIX, docIdText);

                // Delete the statement parts in index
                docTableMut.putDelete(ColumnPrefixes.getSubjColFam(statement), docIdText);
                docTableMut.putDelete(ColumnPrefixes.getPredColFam(statement), docIdText);
                docTableMut.putDelete(ColumnPrefixes.getObjColFam(statement), docIdText);
                docTableMut.putDelete(ColumnPrefixes.getContextColFam(statement), docIdText);


                // Delete the statement terms in index
                for (String token : tokens) {
                    if (IS_TERM_TABLE_TOKEN_DELETION_ENABLED) {
                        int rowId = Integer.parseInt(partition.toString());
                        boolean doesTermExistInOtherDocs = doesTermExistInOtherDocs(token, rowId, docIdText);
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
                } catch (MutationsRejectedException e) {
                    logger.error("error adding mutation", e);
                    throw new IOException(e);
                }

            }
        }
    }

    @Override
    public void deleteStatement(RyaStatement statement) throws IOException {
        deleteStatement(RyaToRdfConversions.convertStatement(statement));
    }

    /**
     * Checks to see if the provided term appears in other documents.
     * @param term the term to search for.
     * @param currentDocId the current document ID that the search term exists in.
     * @return {@code true} if the term was found in other documents. {@code false} otherwise.
     */
    private boolean doesTermExistInOtherDocs(String term, int currentDocId, Text docIdText) {
        try {
            String freeTextDocTableName = getFreeTextDocTablename(conf);
            Scanner scanner = getScanner(freeTextDocTableName);

            String t = StringUtils.removeEnd(term, "*").toLowerCase();
            Text queryTerm = ColumnPrefixes.getTermColFam(t);

            // perform query and read results
            scanner.fetchColumnFamily(queryTerm);

            for (Entry<Key, Value> entry : scanner) {
                Key key = entry.getKey();
                Text row = key.getRow();
                int rowId = Integer.parseInt(row.toString());
                // We only want to check other documents from the one we're deleting
                if (rowId != currentDocId) {
                    Text columnFamily = key.getColumnFamily();
                    String columnFamilyValue = columnFamily.toString();
                    // Check that the value has the term prefix
                    if (columnFamilyValue.startsWith(ColumnPrefixes.TERM_CF_PREFIX.toString())) {
                        Text text = ColumnPrefixes.removePrefix(columnFamily);
                        String value = text.toString();
                        if (value.equals(term)) {
                            return true;
                        }
                    }
                }
            }
        } catch (IOException e) {
            logger.error("Error searching for the existance of the term in other documents", e);
        }
        return false;
    }


	@Override
	public void init() {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void setConnector(Connector connector) {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void destroy() {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void purge(RdfCloudTripleStoreConfiguration configuration) {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void dropAndDestroy() {
		// TODO Auto-generated method stub
		
	}
}
