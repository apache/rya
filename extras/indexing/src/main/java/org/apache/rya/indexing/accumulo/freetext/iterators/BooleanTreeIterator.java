package org.apache.rya.indexing.accumulo.freetext.iterators;

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



import static org.apache.rya.indexing.accumulo.freetext.query.ASTNodeUtils.allChildrenAreNot;
import static org.apache.rya.indexing.accumulo.freetext.query.ASTNodeUtils.findFirstNonNotChild;
import static org.apache.rya.indexing.accumulo.freetext.query.ASTNodeUtils.getNodeIterator;
import static org.apache.rya.indexing.accumulo.freetext.query.ASTNodeUtils.isNotFlag;
import static org.apache.rya.indexing.accumulo.freetext.query.ASTNodeUtils.pushChild;
import static org.apache.rya.indexing.accumulo.freetext.query.ASTNodeUtils.swapChildren;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.rya.indexing.accumulo.freetext.ColumnPrefixes;
import org.apache.rya.indexing.accumulo.freetext.query.ASTExpression;
import org.apache.rya.indexing.accumulo.freetext.query.ASTTerm;
import org.apache.rya.indexing.accumulo.freetext.query.ParseException;
import org.apache.rya.indexing.accumulo.freetext.query.QueryParser;
import org.apache.rya.indexing.accumulo.freetext.query.QueryParserTreeConstants;
import org.apache.rya.indexing.accumulo.freetext.query.SimpleNode;
import org.apache.rya.indexing.accumulo.freetext.query.TokenMgrError;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.system.MultiIterator;
import org.apache.commons.lang.Validate;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

public class BooleanTreeIterator implements SortedKeyValueIterator<Key, Value>, OptionDescriber {
    private static Logger logger = Logger.getLogger(BooleanTreeIterator.class);

    private static String queryOptionName = "query";

    private SortedKeyValueIterator<Key, Value> iter;
    private SortedKeyValueIterator<Key, Value> docSource;

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {

        // pull out the query
        String query = options.get(queryOptionName);

        // create the parse tree
        SimpleNode root;
        try {
            root = QueryParser.parse(query);
        } catch (ParseException e) {
            // log and wrap in IOException
            logger.error("ParseException encountered while parsing: " + query, e);
            throw new IOException(e);
        } catch (TokenMgrError e) {
            // log and wrap in IOException
            logger.error("TokenMgrError encountered while parsing: " + query, e);
            throw new IOException(e);
        }

        docSource = source.deepCopy(env);
        iter = createIterator((SimpleNode) root.jjtGetChild(0), source, env);
    }

    private SortedKeyValueIterator<Key, Value> createIterator(SimpleNode root, SortedKeyValueIterator<Key, Value> source,
            IteratorEnvironment env) {
        // if the root is only a single term, wrap it in an expression node
        if (root instanceof ASTTerm) {
            ASTExpression expression = new ASTExpression(QueryParserTreeConstants.JJTEXPRESSION);
            expression.setNotFlag(false);
            expression.setType(ASTExpression.AND);

            pushChild(expression, root);
            root.jjtSetParent(expression);

            root = expression;
        }

        // Pre-process the tree to compensate for iterator specific issues with certain topologies
        preProcessTree(root);

        // Build an iterator tree
        return createIteratorRecursive(root, source, env);
    }

    private SortedKeyValueIterator<Key, Value> createIteratorRecursive(SimpleNode node, SortedKeyValueIterator<Key, Value> source,
            IteratorEnvironment env) {

        Validate.isTrue(node instanceof ASTExpression, "node must be of type ASTExpression.  Node is instance of "
                + node.getClass().getName());

        ASTExpression expression = (ASTExpression) node;

        if (expression.getType().equals(ASTExpression.AND)) {
            return getAndIterator(node, source, env);
        }

        if (expression.getType().equals(ASTExpression.OR)) {
            return getOrIterator(node, source, env);
        }

        throw new IllegalArgumentException("Expression is of unknown type: " + expression.getType());

    }

    private MultiIterator getOrIterator(SimpleNode node, SortedKeyValueIterator<Key, Value> source, IteratorEnvironment env) {
        List<SortedKeyValueIterator<Key, Value>> iters = new ArrayList<SortedKeyValueIterator<Key, Value>>();

        for (SimpleNode n : getNodeIterator(node)) {
            if (n instanceof ASTExpression) {
                iters.add(createIteratorRecursive(n, source, env));
            } else if (n instanceof ASTTerm) {
                iters.add(getSimpleAndingIterator((ASTTerm) n, source, env));
            } else {
                throw new IllegalArgumentException("Node is of unknown type: " + n.getClass().getName());
            }
        }

        return new MultiIterator(iters, new Range());
    }

    private AndingIterator getAndIterator(SimpleNode node, SortedKeyValueIterator<Key, Value> source, IteratorEnvironment env) {

        AndingIterator anding = new AndingIterator();

        for (SimpleNode n : getNodeIterator(node)) {
            boolean isNotFlag = isNotFlag(n);
            if (n instanceof ASTExpression) {
                anding.addSource(createIteratorRecursive(n, source, env), env, null, isNotFlag);
            } else if (n instanceof ASTTerm) {
                ASTTerm term = ((ASTTerm) n);
                anding.addSource(source, env, getTermColFam(term), isNotFlag);
            } else {
                throw new IllegalArgumentException("Node is of unknown type: " + n.getClass().getName());
            }
        }

        return anding;
    }

    private static Text getTermColFam(ASTTerm termnode) {
        String term = termnode.getTerm();
        if (term == null) {
            // if the term is null, then I want all of the documents
            return ColumnPrefixes.DOCS_CF_PREFIX;
        }
        if (term.contains("\0")) {
            // if the term is contain a null char, then it's already formated for a CF
            return new Text(term);
        }

        // otherwise, point to the term CF
        return ColumnPrefixes.getTermColFam(term.toLowerCase());
    }

    private AndingIterator getSimpleAndingIterator(ASTTerm node, SortedKeyValueIterator<Key, Value> source, IteratorEnvironment env) {
        Validate.isTrue(!node.isNotFlag(), "Simple Anding node must not have \"not\" flag set");

        AndingIterator anding = new AndingIterator();
        anding.addSource(source, env, getTermColFam(node), false);
        return anding;
    }

    /**
     * Handle "lonely nots" (i.e. expressions with only nots), "or" statements containing nots, and make sure that the first term in an
     * "and" statement is not a not. This is due to implementation specific limitations of the iterators.
     * <p>
     * For example:
     * <ul>
     * <li>lonely nots: (!a & !b) -> [all] & !a & !b</li>
     * <li>"or" nots: (!a | b) -> ( ([all] & !a) | b)</li>
     * <li>reorder "and" nots: (!a & b) -> ( b & !a )</li>
     * </ul>
     **/
    public static void preProcessTree(SimpleNode s) {
        for (SimpleNode child : getNodeIterator(s)) {
            preProcessTree(child);
        }

        if (s instanceof ASTExpression) {
            ASTExpression expression = (ASTExpression) s;

            if (expression.getType().equals(ASTExpression.AND)) {
                if (allChildrenAreNot(expression)) {
                    // lonely nots: (!a & !b) -> [all] & !a & !b
                    ASTTerm allDocsTerm = createAllDocTermNode();
                    pushChild(expression, allDocsTerm);
                } else if (isNotFlag(expression.jjtGetChild(0))) {
                    // reorder "and" nots: (!a & b) -> ( b & !a )
                    int firstNonNotChild = findFirstNonNotChild(expression);
                    swapChildren(expression, 0, firstNonNotChild);
                }
            }

            if (expression.getType().equals(ASTExpression.OR)) {
                for (int i = 0; i < expression.jjtGetNumChildren(); i++) {
                    SimpleNode child = (SimpleNode) expression.jjtGetChild(i);
                    if (isNotFlag(child)) {
                        // "or" nots: (!a | b) -> ( ([all] & !a) | b)
                        // create the new expression
                        ASTExpression newExpression = new ASTExpression(QueryParserTreeConstants.JJTEXPRESSION);
                        newExpression.setNotFlag(false);
                        newExpression.setType(ASTExpression.AND);
                        pushChild(newExpression, child);
                        pushChild(newExpression, createAllDocTermNode());

                        // tie the new expression to the old one
                        newExpression.jjtSetParent(expression);
                        expression.jjtAddChild(newExpression, i);
                    }
                }
            }
        }

    }

    public static ASTTerm createAllDocTermNode() {
        ASTTerm t = new ASTTerm(QueryParserTreeConstants.JJTTERM);
        t.setNotFlag(false);
        t.setType(ASTTerm.TERM);
        // note: a "null" signifies "all docs" should be returned.
        t.setTerm(null);
        return t;
    }

    @Override
    public boolean hasTop() {
        return iter.hasTop();
    }

    @Override
    public void next() throws IOException {
        iter.next();
        if (iter.hasTop()) {
            seekDocSource(iter.getTopKey());
        }
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        iter.seek(range, columnFamilies, inclusive);
        if (iter.hasTop()) {
            seekDocSource(iter.getTopKey());
        }
    }

    private void seekDocSource(Key key) throws IOException {
        Key docKey = new Key(key.getRow(), ColumnPrefixes.DOCS_CF_PREFIX, key.getColumnQualifier());
        docSource.seek(new Range(docKey, true, null, false), Collections.<ByteSequence> emptyList(), false);
    }

    @Override
    public Key getTopKey() {
        // from intersecting iterator:
        // RowID: shardID
        // CF: (empty)
        // CQ: docID
        return iter.getTopKey();
    }

    @Override
    public Value getTopValue() {
        if (!iter.hasTop()) {
            throw new NoSuchElementException();
        }

        return docSource.getTopValue();
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        throw new UnsupportedOperationException();
    }

    public static void setQuery(IteratorSetting cfg, String query) {
        cfg.addOption(BooleanTreeIterator.queryOptionName, query);
    }

    @Override
    public IteratorOptions describeOptions() {
        return new IteratorOptions("FreeTextBooleanTree", "Perform a FreeText Query on properly formated table",
                Collections.singletonMap(queryOptionName, "the free text query"),
                null);
    }

    @Override
    public boolean validateOptions(Map<String, String> options) {
        String q = options.get(queryOptionName);
        if (q == null || q.isEmpty())
            throw new IllegalArgumentException(queryOptionName + " must not be empty");
        return true;
    }

}
