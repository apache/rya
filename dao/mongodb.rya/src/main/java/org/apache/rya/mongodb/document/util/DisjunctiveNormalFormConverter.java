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
package org.apache.rya.mongodb.document.util;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility.Node;
import org.apache.accumulo.core.security.ColumnVisibility.NodeType;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.rya.mongodb.document.visibility.DocumentVisibility;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

/**
 * Utility for converting document visibility boolean expressions into
 * Disjunctive Normal Form.
 */
public final class DisjunctiveNormalFormConverter {
    private static final Logger log = Logger.getLogger(DisjunctiveNormalFormConverter.class);

    /**
     * Private constructor to prevent instantiation.
     */
    private DisjunctiveNormalFormConverter() {
    }

    /**
     * Creates a new document visibility based on the boolean expression string
     * that is converted into disjunctive normal form.
     * @param expression the boolean string expression.
     * @return the {@link DocumentVisibility} in DNF.
     */
    public static DocumentVisibility createDnfDocumentVisibility(final String expression) {
        return createDnfDocumentVisibility(expression.getBytes(UTF_8));
    }

    /**
     * Creates a new document visibility based on the boolean expression that is
     * converted into disjunctive normal form.
     * @param expression the boolean expression bytes.
     * @return the {@link DocumentVisibility} in DNF.
     */
    public static DocumentVisibility createDnfDocumentVisibility(final byte[] expression) {
        final DocumentVisibility documentVisibility = new DocumentVisibility(expression);
        final DocumentVisibility dnfDv = convertToDisjunctiveNormalForm(documentVisibility);
        return dnfDv;
    }

    /**
     * Creates a document visibility boolean expression string into Disjunctive
     * Normal Form (DNF).  Expressions use this format in DNF:<pre>
     * (P1 & P2 & P3 ... Pn) | (Q1 & Q2 ... Qm) ...
     * </pre>
     * @param documentVisibility the {@link DocumentVisibility}.
     * @return a new {@link DocumentVisibility} with its expression in DNF.
     */
    public static DocumentVisibility convertToDisjunctiveNormalForm(final DocumentVisibility documentVisibility) {
        // Find all the terms used in the expression
        final List<String> terms = findNodeTerms(documentVisibility.getParseTree(), documentVisibility.getExpression());
        // Create an appropriately sized truth table that has the correct 0's
        // and 1's in place based on the number of terms.
        // This size should be [numberOfTerms][2 ^ numberOfTerms].
        final byte[][] truthTable = createTruthTableInputs(terms);

        // Go through each row in the truth table.
        // If the row has a 1 for the term then create an Authorization for it
        // and test if it works.
        // If the row passes then that means all the terms that were a 1 and
        // were used can be AND'ed together to pass the expression.
        // All the rows that pass can be OR'd together.
        // Disjunction Normal Form: (P1 & P2 & P3 ... Pn) | (Q1 & Q2 ... Qm) ...
        final List<List<String>> termRowsThatPass = new ArrayList<>();
        for (final byte[] row : truthTable) {
            final List<String> termRowToCheck = new ArrayList<>();
            // If the truth table input is a 1 then include the corresponding
            // term that it matches.
            for (int i = 0; i < row.length; i++) {
                final byte entry = row[i];
                if (entry == 1) {
                    termRowToCheck.add(terms.get(i));
                }
            }

            final List<String> authList = new ArrayList<>();
            for (final String auth : termRowToCheck) {
                String formattedAuth = auth;
                formattedAuth = StringUtils.removeStart(formattedAuth, "\"");
                formattedAuth = StringUtils.removeEnd(formattedAuth, "\"");
                authList.add(formattedAuth);
            }
            final Authorizations auths = new Authorizations(authList.toArray(new String[0]));
            final boolean hasAccess = DocumentVisibilityUtil.doesUserHaveDocumentAccess(auths, documentVisibility, false);
            if (hasAccess) {
                boolean alreadyCoveredBySimplerTerms = false;
                // If one 'AND' group is (A&C) and another is (A&B&C) then we
                // can drop (A&B&C) since it is already covered by simpler terms
                // (it's a subset)
                for (final List<String> existingTermRowThatPassed : termRowsThatPass) {
                    alreadyCoveredBySimplerTerms = termRowToCheck.containsAll(existingTermRowThatPassed);
                    if (alreadyCoveredBySimplerTerms) {
                        break;
                    }
                }
                if (!alreadyCoveredBySimplerTerms) {
                    termRowsThatPass.add(termRowToCheck);
                }
            }
        }

        // Rebuild the term rows that passed as a document visibility boolean
        // expression string.
        final StringBuilder sb = new StringBuilder();
        boolean isFirst = true;
        final boolean hasMultipleGroups = termRowsThatPass.size() > 1;
        for (final List<String> termRowThatPassed : termRowsThatPass) {
            if (isFirst) {
                isFirst = false;
            } else {
                sb.append("|");
            }
            if (hasMultipleGroups && termRowThatPassed.size() > 1) {
                sb.append("(");
            }
            sb.append(Joiner.on("&").join(termRowThatPassed));
            if (hasMultipleGroups && termRowThatPassed.size() > 1) {
                sb.append(")");
            }
        }

        log.trace(sb.toString());
        final DocumentVisibility dnfDv = new DocumentVisibility(sb.toString());
        return dnfDv;
    }

    /**
     * Searches a node for all unique terms in its expression and returns them.
     * Duplicates are not included.
     * @param node the {@link Node}.
     * @return an unmodifiable {@link List} of string terms without duplicates.
     */
    public static List<String> findNodeTerms(final Node node, final byte[] expression) {
        final Set<String> terms = new LinkedHashSet<>();
        if (node.getType() == NodeType.TERM) {
            final String data = DocumentVisibilityUtil.getTermNodeData(node, expression);
            terms.add(data);
        }
        for (final Node child : node.getChildren()) {
            switch (node.getType()) {
                case AND:
                case OR:
                    terms.addAll(findNodeTerms(child, expression));
                    break;
                default:
                    break;
            }
        }
        return Collections.unmodifiableList(Lists.newArrayList(terms));
    }

    /**
     * Creates the inputs needed to populate a truth table based on the provided
     * number of terms that the expression uses. So, a node that only has 3
     * terms will create a 3 x 8 size table:
     * <pre>
     * 0 0 0
     * 0 0 1
     * 0 1 0
     * 0 1 1
     * 1 0 0
     * 1 0 1
     * 1 1 0
     * 1 1 1
     * </pre>
     * @param node the {@link Node}.
     * @return a two-dimensional array of bytes representing the truth table
     * inputs.  The table will be of size: [termNumber] x [2 ^ termNumber]
     */
    public static byte[][] createTruthTableInputs(final Node node, final byte[] expression) {
        final List<String> terms = findNodeTerms(node, expression);
        return createTruthTableInputs(terms);
    }

    /**
     * Creates the inputs needed to populate a truth table based on the provided
     * number of terms that the expression uses. So, if there are 3 terms then
     * it will create a 3 x 8 size table:
     * <pre>
     * 0 0 0
     * 0 0 1
     * 0 1 0
     * 0 1 1
     * 1 0 0
     * 1 0 1
     * 1 1 0
     * 1 1 1
     * </pre>
     * @param terms the {@link List} of term strings.
     * @return a two-dimensional array of bytes representing the truth table
     * inputs.  The table will be of size: [termNumber] x [2 ^ termNumber]
     */
    public static byte[][] createTruthTableInputs(final List<String> terms) {
        return createTruthTableInputs(terms.size());
    }

    /**
     * Creates the inputs needed to populate a truth table based on the provided
     * number of terms that the expression uses. So, entering 3 for the number
     * of terms will create a 3 x 8 size table:
     * <pre>
     * 0 0 0
     * 0 0 1
     * 0 1 0
     * 0 1 1
     * 1 0 0
     * 1 0 1
     * 1 1 0
     * 1 1 1
     * </pre>
     * @param termNumber the number of terms.
     * @return a two-dimensional array of bytes representing the truth table
     * inputs.  The table will be of size: [termNumber] x [2 ^ termNumber]
     */
    public static byte[][] createTruthTableInputs(final int termNumber) {
        final int numColumns = termNumber;
        final int numRows = (int) Math.pow(2, numColumns);
        final byte[][] truthTable = new byte[numRows][numColumns];

        for (int row = 0; row < numRows; row++) {
            for (int col = 0; col < numColumns; col++) {
                // We're starting from the top-left and going right then down to
                // the next row. The left-side is of a higher order than the
                // right-side so adjust accordingly.
                final int digitOrderPosition = numColumns - 1 - col;
                final int power = (int) Math.pow(2, digitOrderPosition);
                final int toggle = (row / power) % 2;
                truthTable[row][col] = (byte) toggle;
            }
        }

        log.trace("Truth table inputs: " + Arrays.deepToString(truthTable));

        return truthTable;
    }
}