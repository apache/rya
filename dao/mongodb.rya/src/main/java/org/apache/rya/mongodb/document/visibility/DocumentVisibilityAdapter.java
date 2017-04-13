/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.mongodb.document.visibility;

import org.apache.log4j.Logger;
import org.apache.rya.mongodb.MongoDbRdfConstants;
import org.apache.rya.mongodb.dao.SimpleMongoDBStorageStrategy;
import org.apache.rya.mongodb.document.util.DocumentVisibilityConversionException;
import org.apache.rya.mongodb.document.util.DocumentVisibilityUtil;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Serializes the document visibility field of a Rya Statement for use in
 * MongoDB.
 * The {@link DBObject} will look like:
 * <pre>
 * {@code
 * {
 *   "documentVisibility": &lt;array&gt;,
 * }
 * </pre>
 */
@DefaultAnnotation(NonNull.class)
public final class DocumentVisibilityAdapter {
    private static final Logger log = Logger.getLogger(DocumentVisibilityAdapter.class);

    public static final String DOCUMENT_VISIBILITY_KEY = SimpleMongoDBStorageStrategy.DOCUMENT_VISIBILITY;

    /**
     * Private constructor to prevent instantiation.
     */
    private DocumentVisibilityAdapter() {
    }

    /**
     * Serializes a document visibility expression byte array to a MongoDB
     * {@link DBObject}.
     * @param expression the document visibility expression byte array to be
     * serialized.
     * @return The MongoDB {@link DBObject}.
     */
    public static BasicDBObject toDBObject(final byte[] expression) {
        DocumentVisibility dv;
        if (expression == null) {
            dv = MongoDbRdfConstants.EMPTY_DV;
        } else {
            dv = new DocumentVisibility(expression);
        }
        return toDBObject(dv);
    }

    /**
     * Serializes a {@link DocumentVisibility} to a MongoDB {@link DBObject}.
     * @param documentVisibility the {@link DocumentVisibility} to be
     * serialized.
     * @return The MongoDB {@link DBObject}.
     */
    public static BasicDBObject toDBObject(final DocumentVisibility documentVisibility) {
        DocumentVisibility dv;
        if (documentVisibility == null) {
            dv = MongoDbRdfConstants.EMPTY_DV;
        } else {
            dv = documentVisibility;
        }
        Object[] dvArray = null;
        try {
            dvArray = DocumentVisibilityUtil.toMultidimensionalArray(dv);
        } catch (final DocumentVisibilityConversionException e) {
            log.error("Unable to convert document visibility");
        }

        final BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();
        builder.add(DOCUMENT_VISIBILITY_KEY, dvArray);
        return (BasicDBObject) builder.get();
    }

    /**
     * Deserializes a MongoDB {@link DBObject} to a {@link DocumentVisibility}.
     * @param mongoObj the {@link DBObject} to be deserialized.
     * @return the {@link DocumentVisibility} object.
     * @throws MalformedDocumentVisibilityException
     */
    public static DocumentVisibility toDocumentVisibility(final DBObject mongoObj) throws MalformedDocumentVisibilityException {
        try {
            final BasicDBObject basicObj = (BasicDBObject) mongoObj;

            final Object documentVisibilityObject = basicObj.get(DOCUMENT_VISIBILITY_KEY);
            Object[] documentVisibilityArray = null;
            if (documentVisibilityObject instanceof Object[]) {
                documentVisibilityArray = (Object[]) documentVisibilityObject;
            } else if (documentVisibilityObject instanceof BasicDBList) {
                documentVisibilityArray = DocumentVisibilityUtil.convertBasicDBListToObjectArray((BasicDBList) documentVisibilityObject);
            }

            final String documentVisibilityString = DocumentVisibilityUtil.multidimensionalArrayToBooleanString(documentVisibilityArray);
            final DocumentVisibility dv = documentVisibilityString == null ? MongoDbRdfConstants.EMPTY_DV : new DocumentVisibility(documentVisibilityString);

            return dv;
        } catch(final Exception e) {
            throw new MalformedDocumentVisibilityException("Failed to make Document Visibility from Mongo Object, it is malformed.", e);
        }
    }

    /**
     * Exception thrown when a MongoDB {@link DBObject} is malformed when
     * attempting to adapt it into a {@link DocumentVisibility}.
     */
    public static class MalformedDocumentVisibilityException extends Exception {
        private static final long serialVersionUID = 1L;

        /**
         * Creates a new {@link MalformedDocumentVisibilityException}
         * @param message - The message to be displayed by the exception.
         * @param e - The source cause of the exception.
         */
        public MalformedDocumentVisibilityException(final String message, final Throwable e) {
            super(message, e);
        }
    }
}