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
package org.apache.rya.indexing.entity.storage.mongo;

import org.bson.Document;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Converts an object to/from a {@link Document}.
 *
 * @param <T> - The type of object that is converted to/from a {@link Document}.
 */
@DefaultAnnotation(NonNull.class)
public interface DocumentConverter<T> {

    /**
     * Converts an object into a {@link Document}.
     *
     * @param object - The object to convert. (not null)
     * @return A {@link Document} representing the object.
     * @throws DocumentConverterException A problem occurred while converting the object.
     */
    public Document toDocument(T object) throws DocumentConverterException;

    /**
     * Converts a {@link Document} into the target object.
     *
     * @param document - The document to convert. (not null)
     * @return The target object representation of the document.
     * @throws DocumentConverterException A problem occurred while converting the {@link Document}.
     */
    public T fromDocument(Document document) throws DocumentConverterException;

    /**
     * A problem occurred while converting an object while using a {@link DocumentConverter}.
     */
    public static class DocumentConverterException extends Exception {
        private static final long serialVersionUID = 1L;

        public DocumentConverterException(final String message) {
            super(message);
        }

        public DocumentConverterException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }
}