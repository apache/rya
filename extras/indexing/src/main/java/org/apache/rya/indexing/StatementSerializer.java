package org.apache.rya.indexing;

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

import java.io.IOException;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.rya.api.utils.LiteralLanguageUtils;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

/**
 * A set of Utilities to serialize {@link Statement}s to/from {@link String}s.
 */
public class StatementSerializer {
    private static String SEP = "\u0000";

    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    /**
     * Read a {@link Statement} from a {@link String}
     *
     * @param in
     *            the {@link String} to parse
     * @return a {@link Statement}
     */
    public static Statement readStatement(final String in) throws IOException {
        final String[] parts = in.split(SEP);

        if (parts.length != 4) {
            throw new IOException("Not a valid statement: " + in);
        }

        final String contextString = parts[0];
        final String subjectString = parts[1];
        final String predicateString = parts[2];
        final String objectString = parts[3];
        return readStatement(subjectString, predicateString, objectString, contextString);
    }

    public static Statement readStatement(final String subjectString, final String predicateString, final String objectString) {
        return readStatement(subjectString, predicateString, objectString, "");
    }

    public static Statement readStatement(final String subjectString, final String predicateString, final String objectString, final String contextString) {
        final Resource subject = createResource(subjectString);
        final IRI predicate = VF.createIRI(predicateString);

        final boolean isObjectLiteral = objectString.startsWith("\"");

        Value object = null;
        if (isObjectLiteral) {
            object = parseLiteral(objectString);
        } else {
            object = createResource(objectString);
        }

        if (contextString == null || contextString.isEmpty()) {
            return VF.createStatement(subject, predicate, object);
        } else {
            final Resource context = VF.createIRI(contextString);
            return VF.createStatement(subject, predicate, object, context);
        }
    }

    private static Resource createResource(final String str) {
        if (str.startsWith("_")) {
            return VF.createBNode(str.substring(2));
        }
        return VF.createIRI(str);

    }

    private static Literal parseLiteral(final String fullLiteralString) {
        Validate.notNull(fullLiteralString);
        Validate.isTrue(fullLiteralString.length() > 1);

        if (fullLiteralString.endsWith("\"")) {
            final String fullLiteralWithoutQuotes = fullLiteralString.substring(1, fullLiteralString.length() - 1);
            return VF.createLiteral(fullLiteralWithoutQuotes);
        } else {

            // find the closing quote
            final int labelEnd = fullLiteralString.lastIndexOf("\"");

            final String label = fullLiteralString.substring(1, labelEnd);

            final String data = fullLiteralString.substring(labelEnd + 1);

            if (data.startsWith(LiteralLanguageUtils.LANGUAGE_DELIMITER)) {
                // the data is "language"
                final String lang = data.substring(1);
                return VF.createLiteral(label, lang);
            } else if (data.startsWith("^^<")) {
                // the data is a "datatype"
                final String datatype = data.substring(3, data.length() - 1);
                final IRI datatypeUri = VF.createIRI(datatype);
                return VF.createLiteral(label, datatypeUri);
            }
        }
        return null;

    }

    public static String writeSubject(final Statement statement) {
        return statement.getSubject().toString();
    }

    public static String writeObject(final Statement statement) {
        return statement.getObject().toString();
    }

    public static String writePredicate(final Statement statement) {
        return statement.getPredicate().toString();
    }

    public static String writeSubjectPredicate(final Statement statement) {
        Validate.notNull(statement);
        Validate.notNull(statement.getSubject());
        Validate.notNull(statement.getPredicate());
        return statement.getSubject().toString() + SEP + statement.getPredicate().toString();
    }

    public static String writeContext(final Statement statement) {
        if (statement.getContext() == null) {
            return "";
        }
        return statement.getContext().toString();
    }

    /**
     * Write a {@link Statement} to a {@link String}
     *
     * @param statement
     *            the {@link Statement} to write
     * @return a {@link String} representation of the statement
     */
    public static String writeStatement(final Statement statement) {
        final Resource subject = statement.getSubject();
        final Resource context = statement.getContext();
        final IRI predicate = statement.getPredicate();
        final Value object = statement.getObject();

        Validate.notNull(subject);
        Validate.notNull(predicate);
        Validate.notNull(object);

        String s = "";
        if (context == null) {
            s = SEP + subject.toString() + SEP + predicate.toString() + SEP + object.toString();
        } else {
            s = context.toString() + SEP + subject.toString() + SEP + predicate.toString() + SEP + object.toString();
        }
        return s;
    }

    /**
     * Creates a Regular Expression to match serialized statements meeting these constraints. A <code>null</code> or empty parameters imply
     * no constraint. A <code>null</code> return value implies no constraints.
     *
     * @param context
     *            context constraint
     * @param subject
     *            subject constraint
     * @param predicates
     *            list of predicate constraints
     * @return a regular expression that can be used to match serialized statements. A <code>null</code> return value implies no
     *         constraints.
     */
    public static String createStatementRegex(final StatementConstraints contraints) {
        final Resource context = contraints.getContext();
        final Resource subject = contraints.getSubject();
        final Set<IRI> predicates = contraints.getPredicates();
        if (context == null && subject == null && (predicates == null || predicates.isEmpty())) {
            return null;
        }

        // match on anything but a separator
        final String anyReg = "[^" + SEP + "]*";

        // if context is empty, match on any context
        final String contextReg = (context == null) ? anyReg : context.stringValue();

        // if subject is empty, match on any subject
        final String subjectReg = (subject == null) ? anyReg : subject.stringValue();

        // if the predicates are empty, match on any predicate. Otherwise, "or" the predicates.
        String predicateReg = "";
        if (predicates == null || predicates.isEmpty()) {
            predicateReg = anyReg;
        } else {
            predicateReg = "(" + StringUtils.join(predicates, "|") + ")";
        }

        return "^" + contextReg + SEP + subjectReg + SEP + predicateReg + SEP + ".*";
    }

}
