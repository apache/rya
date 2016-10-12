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

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalResultExporter;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalResultExporterFactory;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPcjStorage;

import com.google.common.base.Optional;

import org.apache.fluo.api.observer.Observer.Context;

/**
 * Creates instances of {@link RyaResultExporter}.
 */
public class RyaResultExporterFactory implements IncrementalResultExporterFactory {

    @Override
    public Optional<IncrementalResultExporter> build(final Context context) throws IncrementalExporterFactoryException, ConfigurationException {
        checkNotNull(context);

        // Wrap the context's parameters for parsing.
        final RyaExportParameters params = new RyaExportParameters( context.getObserverConfiguration().toMap() );

        if(params.isExportToRya()) {
            // Setup Zookeeper connection info.
            final String accumuloInstance = params.getAccumuloInstanceName().get();
            final String zookeeperServers =  params.getZookeeperServers().get().replaceAll(";", ",");
            final Instance inst = new ZooKeeperInstance(accumuloInstance, zookeeperServers);

            try {
                // Setup Accumulo connection info.
                final String exporterUsername = params.getExporterUsername().get();
                final String exporterPassword = params.getExporterPassword().get();
                final Connector accumuloConn = inst.getConnector(exporterUsername, new PasswordToken(exporterPassword));

                // Setup Rya PCJ Storage.
                final String ryaInstanceName = params.getRyaInstanceName().get();
                final PrecomputedJoinStorage pcjStorage = new AccumuloPcjStorage(accumuloConn, ryaInstanceName);

                // Make the exporter.
                final IncrementalResultExporter exporter = new RyaResultExporter(pcjStorage);
                return Optional.of(exporter);

            } catch (final AccumuloException | AccumuloSecurityException e) {
                throw new IncrementalExporterFactoryException("Could not initialize the Accumulo connector using the provided configuration.", e);
            }
        } else {
            return Optional.absent();
        }
    }
}
