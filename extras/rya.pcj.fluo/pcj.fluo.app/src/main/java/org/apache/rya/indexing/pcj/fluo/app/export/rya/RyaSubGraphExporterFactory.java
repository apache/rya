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
package org.apache.rya.indexing.pcj.fluo.app.export.rya;

import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.observer.Observer.Context;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalResultExporter;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalResultExporterFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

/**
 * Factory class for building {@link RyaSubGraphExporter}s.
 *
 */
public class RyaSubGraphExporterFactory implements IncrementalResultExporterFactory {

    @Override
    public Optional<IncrementalResultExporter> build(Context context) throws IncrementalExporterFactoryException, ConfigurationException {
        Preconditions.checkNotNull(context);
        
        RyaSubGraphExportParameters params = new RyaSubGraphExportParameters(context.getObserverConfiguration().toMap());

        if (params.getUseRyaSubGraphExporter()) {
            try {
                //Get FluoConfiguration from params
                FluoConfiguration conf = params.getFluoConfiguration();
                FluoClient fluo = FluoFactory.newClient(conf);
                
                //Create exporter
                RyaSubGraphExporter exporter = new RyaSubGraphExporter(fluo);
                return Optional.of(exporter);
            } catch (Exception e) {
                throw new IncrementalExporterFactoryException("Could not initialize the RyaSubGraphExporter", e);
            }
        }
        return Optional.absent();
    }
}
