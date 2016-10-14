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
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ContextStatementImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.ValueFactoryImpl;

/**
 * A set of Utilities to serialize {@link Statement}s to/from {@link String}s.
 */
public class StatementSerializer {
    private static String SEP = "\u0000";

    private static ValueFactory VALUE_FACTORY = new ValueFactoryImpl();

    /**
     * Read a {@link Statement} from a {@link String}
     * 
     * @param in
     *            the {@link String} to parse
     * @return a {@link Statement}
     */
    public static Statement readStatement(String in) throws IOException {
        String[] parts = in.split(SEP);
        
        if (parts.length != 4) {
            throw new IOException("Not a valid statement: " + in);
        }
        
        String contextString = parts[0];
        String subjectString = parts[1];
        String predicateString = parts[2];
        String objectString = parts[3];
        return readStatement(subjectString, predicateString, objectString, contextString);
    }

    public static Statement readStatement(String subjectString, String predicateString, String objectString) {
        return readStatement(subjectString, predicateString, objectString, "");
    }

    public static Statement readStatement(String subjectString, String predicateString, String objectString, String contextString) {
        Resource subject = createResource(subjectString);
        URI predicate = VALUE_FACTORY.createURI(predicateString);

        boolean isObjectLiteral = objectString.startsWith("\"");

        Value object = null;
        if (isObjectLiteral) {
            object = parseLiteral(objectString);
        } else {
            object = createResource(objectString);
        }

        if (contextString == null || contextString.isEmpty()) {
            return new StatementImpl(subject, predicate, object);
        } else {
            Resource context = VALUE_FACTORY.createURI(contextString);
            return new ContextStatementImpl(subject, predicate, object, context);
        }
    }

    private static Resource createResource(String str) {
        if (str.startsWith("_")) {
            return VALUE_FACTORY.createBNode(str.substring(2));
        }
        return VALUE_FACTORY.createURI(str);

    }

    private static Literal parseLiteral(String fullLiteralString) {
        Validate.notNull(fullLiteralString);
        Validate.isTrue(fullLiteralString.length() > 1);

        if (fullLiteralString.endsWith("\"")) {
            String fullLiteralWithoutQuotes = fullLiteralString.substring(1, fullLiteralString.length() - 1);
            return VALUE_FACTORY.createLiteral(fullLiteralWithoutQuotes, (String) null);
        } else {

            // find the closing quote
            int labelEnd = fullLiteralString.lastIndexOf("\"");

            String label = fullLiteralString.substring(1, labelEnd);

            String data = fullLiteralString.substring(labelEnd + 1);

            if (data.startsWith("@")) {
                // the data is "language"
                String lang = data.substring(1);
                return VALUE_FACTORY.createLiteral(label, lang);
            } else if (data.startsWith("^^<")) {
                // the data is a "datatype"
                String datatype = data.substring(3, data.length() - 1);
                URI datatypeUri = VALUE_FACTORY.createURI(datatype);
                return VALUE_FACTORY.createLiteral(label, datatypeUri);
            }
        }
        return null;

    }

    public static String writeSubject(Statement statement) {
        return statement.getSubject().toString();
    }

    public static String writeObject(Statement statement) {
        return statement.getObject().toString();
    }

    public static String writePredicate(Statement statement) {
        return statement.getPredicate().toString();
    }

    public static String writeSubjectPredicate(Statement statement) {
        Validate.notNull(statement);
        Validate.notNull(statement.getSubject());
        Validate.notNull(statement.getPredicate());
        return statement.getSubject().toString() + SEP + statement.getPredicate().toString();
    }

    public static String writeContext(Statement statement) {
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
    public static String writeStatement(Statement statement) {
        Resource subject = statement.getSubject();
        Resource context = statement.getContext();
        URI predicate = statement.getPredicate();
        Value object = statement.getObject();

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
    public static String createStatementRegex(StatementConstraints contraints) {
        Resource context = contraints.getContext();
        Resource subject = contraints.getSubject();
        Set<URI> predicates = contraints.getPredicates();
        if (context == null && subject == null && (predicates == null || predicates.isEmpty())) {
            return null;
        }

        // match on anything but a separator
        String anyReg = "[^" + SEP + "]*";

        // if context is empty, match on any context
        String contextReg = (context == null) ? anyReg : context.stringValue();

        // if subject is empty, match on any subject
        String subjectReg = (subject == null) ? anyReg : subject.stringValue();

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
