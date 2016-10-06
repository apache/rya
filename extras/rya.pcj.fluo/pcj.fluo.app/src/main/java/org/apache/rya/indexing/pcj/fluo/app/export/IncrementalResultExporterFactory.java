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

import javax.annotation.ParametersAreNonnullByDefault;

import com.google.common.base.Optional;

import org.apache.fluo.api.observer.Observer.Context;

/**
 * Builds instances of {@link IncrementalResultExporter} using the provided
 * configurations.
 */
@ParametersAreNonnullByDefault
public interface IncrementalResultExporterFactory {

    /**
     * Builds an instance of {@link IncrementalResultExporter} using the
     * configurations that are provided.
     *
     * @param context - Contains the host application's configuration values
     *   and any parameters that were provided at initialization. (not null)
     * @return An exporter if configurations were found in the context; otherwise absent.
     * @throws IncrementalExporterFactoryException A non-configuration related
     *   problem has occurred and the exporter could not be created as a result.
     * @throws ConfigurationException Thrown if configuration values were
     *   provided, but an instance of the exporter could not be initialized
     *   using them. This could be because they were improperly formatted,
     *   a required field was missing, or some other configuration based problem.
     */
    public Optional<IncrementalResultExporter> build(Context context) throws IncrementalExporterFactoryException, ConfigurationException;

    /**
     * Indicates a {@link IncrementalResultExporter} could not be created by a
     * {@link IncrementalResultExporterFactory}.
     */
    public static class IncrementalExporterFactoryException extends Exception {
        private static final long serialVersionUID = 1L;

        /**
         * Constructs an instance of {@link }.
         *
         * @param message - Explains why this exception is being thrown.
         */
        public IncrementalExporterFactoryException(final String message) {
            super(message);
        }

        /**
         * Constructs an instance of {@link }.
         *
         * @param message - Explains why this exception is being thrown.
         * @param cause - The exception that caused this one to be thrown.
         */
        public IncrementalExporterFactoryException(final String message, final Throwable t) {
            super(message, t);
        }
    }

    /**
     * The configuration could not be interpreted because required fields were
     * missing or a value wasn't properly formatted.
     */
    public static class ConfigurationException extends IncrementalExporterFactoryException {
        private static final long serialVersionUID = 1L;

        /**
         * Constructs an instance of {@link ConfigurationException}.
         *
         * @param message - Explains why this exception is being thrown.
         */
        public ConfigurationException(final String message) {
            super(message);
        }

        /**
         * Constructs an instance of {@link ConfigurationException}.
         *
         * @param message - Explains why this exception is being thrown.
         * @param cause - The exception that caused this one to be thrown.
         */
        public ConfigurationException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }
}