package org.apache.rya.indexing.pcj.fluo.app.export;
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
import org.apache.fluo.api.observer.Observer.Context;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalBindingSetExporterFactory.ConfigurationException;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalBindingSetExporterFactory.IncrementalExporterFactoryException;

import com.google.common.base.Optional;

/**
 * Builds instances of {@link IncrementalRyaSubGraphExporter} using the provided
 * configurations.
 */
public interface IncrementalRyaSubGraphExporterFactory {

    /**
     * Builds an instance of {@link IncrementalRyaSubGraphExporter} using the
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
    public Optional<IncrementalRyaSubGraphExporter> build(Context context) throws IncrementalExporterFactoryException, ConfigurationException;
}
