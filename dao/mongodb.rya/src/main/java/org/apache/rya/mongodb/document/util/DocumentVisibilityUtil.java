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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility.Node;
import org.apache.accumulo.core.security.ColumnVisibility.NodeType;
import org.apache.accumulo.core.security.VisibilityEvaluator;
import org.apache.accumulo.core.security.VisibilityParseException;
import org.apache.log4j.Logger;
import org.apache.rya.mongodb.MongoDbRdfConstants;
import org.apache.rya.mongodb.document.visibility.DocumentVisibility;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.mongodb.BasicDBList;

/**
 * Utility methods for converting boolean expressions between a string
 * representation to a MongoDB-friendly multidimensional array form that can be
 * used in MongoDB's aggregation set operations.
 */
public final class DocumentVisibilityUtil {
    private static final Logger log = Logger.getLogger(DocumentVisibilityUtil.class);

    /**
     * Private constructor to prevent instantiation.
     */
    private DocumentVisibilityUtil() {
    }

    /**
     * Converts a boolean string expression into a multidimensional
     * array representation of the boolean expression.
     * @param booleanString the boolean string expression.
     * @return the multidimensional array representation of the boolean
     * expression.
     * @throws DocumentVisibilityConversionException
     */
    public static List<Object> toMultidimensionalArray(final String booleanString) throws DocumentVisibilityConversionException {
        final DocumentVisibility dv = new DocumentVisibility(booleanString);
        return toMultidimensionalArray(dv);
    }

    /**
     * Converts a {@link DocumentVisibility} object into a multidimensional
     * array representation of the boolean expression.
     * @param dv the {@link DocumentVisibility}. (not {@code null})
     * @return the multidimensional array representation of the boolean
     * expression.
     * @throws DocumentVisibilityConversionException
     */
    public static List<Object> toMultidimensionalArray(final DocumentVisibility dv) throws DocumentVisibilityConversionException {
        checkNotNull(dv);
        final byte[] expression = dv.flatten();
        final DocumentVisibility flattenedDv = DisjunctiveNormalFormConverter.createDnfDocumentVisibility(expression);
        final List<Object> result = toMultidimensionalArray(flattenedDv.getParseTree(), flattenedDv.getExpression());
        // If there's only one group then make sure it's wrapped as an array.
        // (i.e. "A" should be ["A"])
        if (!result.isEmpty() && result.get(0) instanceof String) {
            final List<Object> formattedResult = new ArrayList<>();
            formattedResult.add(result);
            return formattedResult;
        }
        return result;
    }

    /**
     * Converts a {@link Node} and its corresponding expression into a
     * multidimensional array representation of the boolean expression.
     * @param node the {@link Node}. (not {@code null})
     * @param expression the expression byte array.
     * @return the multidimensional array representation of the boolean
     * expression.
     * @throws DocumentVisibilityConversionException
     */
    public static List<Object> toMultidimensionalArray(final Node node, final byte[] expression) throws DocumentVisibilityConversionException {
        checkNotNull(node);
        final List<Object> array = new ArrayList<>();

        if (node.getChildren().isEmpty() && node.getType() == NodeType.TERM) {
            final String data = getTermNodeData(node, expression);
            array.add(data);
        }

        log.trace("Children size: " + node.getChildren().size() + " Type: " + node.getType());
        for (final Node child : node.getChildren()) {
            switch (child.getType()) {
                case EMPTY:
                case TERM:
                    String data;
                    if (child.getType() == NodeType.TERM) {
                        data = getTermNodeData(child, expression);
                    } else {
                        data = "";
                    }
                    if (node.getType() == NodeType.OR) {
                        array.add(Lists.newArrayList(data));
                    } else {
                        array.add(data);
                    }
                    break;
                case OR:
                case AND:
                    array.add(toMultidimensionalArray(child, expression));
                    break;
                default:
                    throw new DocumentVisibilityConversionException("Unknown type: " + child.getType());
            }
        }

        return array;
    }

    public static String nodeToBooleanString(final Node node, final byte[] expression) throws DocumentVisibilityConversionException {
        boolean isFirst = true;
        final StringBuilder sb = new StringBuilder();
        if (node.getType() == NodeType.TERM) {
            final String data = getTermNodeData(node, expression);
            sb.append(data);
        }
        if (node.getType() == NodeType.AND) {
            sb.append("(");
        }
        for (final Node child : node.getChildren()) {
            if (isFirst) {
                isFirst = false;
            } else {
                if (node.getType() == NodeType.OR) {
                    sb.append("|");
                } else if (node.getType() == NodeType.AND) {
                    sb.append("&");
                }
            }
            switch (child.getType()) {
                case EMPTY:
                    sb.append("");
                    break;
                case TERM:
                    final String data = getTermNodeData(child, expression);
                    sb.append(data);
                    break;
                case OR:
                    sb.append("(");
                    sb.append(nodeToBooleanString(child, expression));
                    sb.append(")");
                    break;
                case AND:
                    sb.append(nodeToBooleanString(child, expression));
                    break;
                default:
                    throw new DocumentVisibilityConversionException("Unknown type: " + child.getType());
            }
        }
        if (node.getType() == NodeType.AND) {
            sb.append(")");
        }

        return sb.toString();
    }

    /**
     * Converts a multidimensional array object representation of the document
     * visibility boolean expression into a string.
     * @param object the multidimensional array object representing the
     * document visibility boolean expression.
     * @return the boolean string expression.
     */
    public static String multidimensionalArrayToBooleanString(final Object[] object) {
        final String booleanString = multidimensionalArrayToBooleanStringInternal(object);

        // Simplify and clean up the formatting.
        final DocumentVisibility dv = DisjunctiveNormalFormConverter.createDnfDocumentVisibility(booleanString);
        final byte[] bytes = dv.flatten();
        final String result = new String(bytes, Charsets.UTF_8);

        return result;
    }

    private static String multidimensionalArrayToBooleanStringInternal(final Object[] object) {
        final StringBuilder sb = new StringBuilder();

        int count = 0;
        boolean isAnd = false;
        for (final Object child : object) {
            if (child instanceof String) {
                isAnd = true;
                if (count > 0) {
                    sb.append("&");
                }
                sb.append(child);
            } else if (child instanceof Object[]) {
                if (count > 0 && isAnd) {
                    sb.append("&");
                }
                final Object[] obj = (Object[]) child;
                sb.append("(");
                sb.append(multidimensionalArrayToBooleanStringInternal(obj));
                sb.append(")");
            } else if (child instanceof List) {
                if (count > 0 && isAnd) {
                    sb.append("&");
                }
                final List<Object> obj = (List<Object>) child;
                sb.append("(");
                sb.append(multidimensionalArrayToBooleanStringInternal(obj.toArray()));
                sb.append(")");
            }

            if (object.length > 1 && count + 1 < object.length && !isAnd) {
                sb.append("|");
            }
            count++;
        }

        return sb.toString();
    }

    /**
     * Conditionally adds quotes around a string.
     * @param data the string to add quotes to.
     * @param addQuotes {@code true} to add quotes. {@code false} to leave the
     * string as is.
     * @return the quoted string if {@code addQuotes} is {@code true}.
     * Otherwise, returns the string as is.
     */
    public static String addQuotes(final String data, final boolean addQuotes) {
        if (addQuotes) {
            return "\"" + data + "\"";
        } else {
            return data;
        }
    }

    /**
     * Returns the term node's data.
     * @param node the {@link Node}.
     * @return the term node's data.
     */
    public static String getTermNodeData(final Node node, final byte[] expression) {
        final boolean isQuotedTerm = expression[node.getTermStart()] == '"';
        final ByteSequence bs = node.getTerm(expression);
        final String data = addQuotes(new String(bs.toArray(), Charsets.UTF_8), isQuotedTerm);
        return data;
    }

    /**
     * Checks if the user's authorizations allows them to have access to the
     * provided document based on its document visibility.
     * @param authorizations the {@link Authorizations}.
     * @param documentVisibility the document visibility byte expression.
     * @return {@code true} if the user has access to the document.
     * {@code false} otherwise.
     */
    public static boolean doesUserHaveDocumentAccess(final Authorizations authorizations, final byte[] documentVisibilityExpression) {
        final byte[] expression = documentVisibilityExpression != null ? documentVisibilityExpression : MongoDbRdfConstants.EMPTY_DV.getExpression();
        final DocumentVisibility documentVisibility = new DocumentVisibility(expression);
        return doesUserHaveDocumentAccess(authorizations, documentVisibility);
    }

    /**
     * Checks if the user's authorizations allows them to have access to the
     * provided document based on its document visibility.
     * @param authorizations the {@link Authorizations}.
     * @param documentVisibility the {@link DocumentVisibility}.
     * @return {@code true} if the user has access to the document.
     * {@code false} otherwise.
     */
    public static boolean doesUserHaveDocumentAccess(final Authorizations authorizations, final DocumentVisibility documentVisibility) {
        return doesUserHaveDocumentAccess(authorizations, documentVisibility, true);
    }

    /**
     * Checks if the user's authorizations allows them to have access to the
     * provided document based on its document visibility.
     * @param authorizations the {@link Authorizations}.
     * @param documentVisibility the {@link DocumentVisibility}.
     * @param doesEmptyAccessPass {@code true} if an empty authorization pass
     * allows access to everything. {@code false} otherwise.
     * @return {@code true} if the user has access to the document.
     * {@code false} otherwise.
     */
    public static boolean doesUserHaveDocumentAccess(final Authorizations authorizations, final DocumentVisibility documentVisibility, final boolean doesEmptyAccessPass) {
        final Authorizations userAuths = authorizations != null ? authorizations : MongoDbRdfConstants.ALL_AUTHORIZATIONS;
        final VisibilityEvaluator visibilityEvaluator = new VisibilityEvaluator(userAuths);
        boolean accept = false;
        if (doesEmptyAccessPass && MongoDbRdfConstants.ALL_AUTHORIZATIONS.equals(userAuths)) {
            accept = true;
        } else {
            try {
                accept = visibilityEvaluator.evaluate(documentVisibility);
            } catch (final VisibilityParseException e) {
                log.error("Could not parse document visibility.");
            }
        }

        return accept;
    }

    /**
     * Converts a {@link BasicDBList} into an array of {@link Object}s.
     * @param basicDbList the {@link BasicDBList} to convert.
     * @return the array of {@link Object}s.
     */
    public static List<Object> convertObjectArrayToList(final Object[] array) {
        final List<Object> list = new ArrayList<>();
        for (final Object child : array) {
            if (child instanceof Object[]) {
                list.add(convertObjectArrayToList((Object[])child));
            } else {
                list.add(child);
            }
        }
        return list;
    }
}