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
package org.apache.rya.indexing.pcj.fluo.app.export;

import org.apache.rya.api.model.VisibilityBindingSet;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Exports a single Binding Set that is a new result for a SPARQL query to some
 * other location.
 */
@DefaultAnnotation(NonNull.class)
public interface IncrementalBindingSetExporter extends IncrementalResultExporter {

    /**
     * Export a Binding Set that is a result of a SPARQL query that does not include a Group By clause.
     *
     * @param queryId - The PCJ ID of the SPARQL query the binding set is a result of. (not null)
     * @param bindingSetString - The Binding Set as it was represented within the Fluo application. (not null)
     * @throws ResultExportException The result could not be exported.
     */
    public void export(String queryId, VisibilityBindingSet result) throws ResultExportException;

    /**
     * A result could not be exported.
     */
    public static class ResultExportException extends Exception {
        private static final long serialVersionUID = 1L;

        /**
         * Constructs an instance of {@link ResultExportException}.
         *
         * @param message - Explains why the exception was thrown.
         */
        public ResultExportException(final String message) {
            super(message);
        }

        /**
         * Constructs an instance of {@link ResultExportException}.
         *
         * @param message - Explains why the exception was thrown.
         * @param cause - The exception that caused this one to be thrown.
         */
        public ResultExportException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }
}