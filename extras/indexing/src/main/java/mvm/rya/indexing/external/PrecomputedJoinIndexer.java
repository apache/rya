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
package mvm.rya.indexing.external;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage.PCJStorageException;
import org.apache.rya.indexing.pcj.update.PrecomputedJoinUpdater;
import org.apache.rya.indexing.pcj.update.PrecomputedJoinUpdater.PcjUpdateException;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;

import mvm.rya.accumulo.experimental.AccumuloIndexer;
import mvm.rya.api.RdfCloudTripleStoreConfiguration;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.domain.RyaURI;
import mvm.rya.api.persist.RyaDAO;
import mvm.rya.indexing.external.accumulo.AccumuloPcjStorage;
import mvm.rya.indexing.external.accumulo.AccumuloPcjStorageSupplier;
import mvm.rya.indexing.external.fluo.FluoPcjUpdaterSupplier;

/**
 * Updates the state of the Precomputed Join indices that are used by Rya.
 */
@ParametersAreNonnullByDefault
public class PrecomputedJoinIndexer implements AccumuloIndexer {
    private static final Logger log = Logger.getLogger(PrecomputedJoinIndexer.class);

    /**
     * This configuration object must be set before {@link #init()} is invoked.
     * It is set by {@link #setConf(Configuration)}.
     */
    private Optional<Configuration> conf = Optional.absent();

    /**
     * The Accumulo Connector that must be used when accessing an Accumulo storage.
     * This value is provided by {@link #setConnector(Connector)}.
     */
    private Optional<Connector> accumuloConn = Optional.absent();

    /**
     * Provides access to the {@link Configuration} that was provided to this class
     * using {@link #setConf(Configuration)}.
     */
    private final Supplier<Configuration> configSupplier = new Supplier<Configuration>() {
        @Override
        public Configuration get() {
            return getConf();
        }
    };

    /**
     * Provides access to the Accumulo {@link Connector} that was provided to
     * this class using {@link #setConnector(Connector)}.
     */
    private final Supplier<Connector> accumuloSupplier = new Supplier<Connector>() {
        @Override
        public Connector get() {
            return accumuloConn.get();
        }
    };

    /**
     * Creates and grants access to the {@link PrecomputedJoinStorage} that will be used
     * to interact with the PCJ results that are stored and used by Rya.
     */
    private final PrecomputedJoinStorageSupplier pcjStorageSupplier =
            new PrecomputedJoinStorageSupplier(
                    configSupplier,
                    new AccumuloPcjStorageSupplier(configSupplier, accumuloSupplier));

    /**
     * Creates and grants access to the {@link PrecomputedJoinUpdater} that will
     * be used to update the state stored within the PCJ tables that are stored
     * in Accumulo.
     */
    private final PrecomputedJoinUpdaterSupplier pcjUpdaterSupplier =
            new PrecomputedJoinUpdaterSupplier(
                    configSupplier,
                    new FluoPcjUpdaterSupplier(configSupplier));

    @Override
    public void setConf(final Configuration conf) {
        this.conf = Optional.fromNullable(conf);
    }

    @Override
    public Configuration getConf() {
        return this.conf.get();
    }

    /**
     * Set the connector that will be used by {@link AccumuloPcjStorage} if the
     * application is configured to store the PCJs within Accumulo.
     */
    @Override
    public void setConnector(final Connector connector) {
        checkNotNull(connector);
        accumuloConn = Optional.of( connector );
    }

    /**
     * This is invoked when the host {@link RyaDAO#init()} method is invoked.
     */
    @Override
    public void init() {
        pcjStorageSupplier.get();
        pcjUpdaterSupplier.get();
    }

    @Override
    public void storeStatement(final RyaStatement statement) throws IOException {
        checkNotNull(statement);
        storeStatements( Collections.singleton(statement) );
    }

    @Override
    public void storeStatements(final Collection<RyaStatement> statements) throws IOException {
        checkNotNull(statements);
        try {
            pcjUpdaterSupplier.get().addStatements(statements);
        } catch (final PcjUpdateException e) {
            throw new IOException("Could not update the PCJs by adding the provided statements.", e);
        }
    }

    @Override
    public void deleteStatement(final RyaStatement statement) throws IOException {
        checkNotNull(statement);
        try {
            pcjUpdaterSupplier.get().deleteStatements( Collections.singleton(statement) );
        } catch (final PcjUpdateException e) {
            throw new IOException("Could not update the PCJs by removing the provided statement.", e);
        }
    }

    @Override
    public void flush() throws IOException {
        try {
            pcjUpdaterSupplier.get().flush();
        } catch (final PcjUpdateException e) {
            throw new IOException("Could not flush the PCJ Updater.", e);
        }
    }

    @Override
    public void close() {
        try {
            pcjStorageSupplier.get().close();
        } catch (final PCJStorageException e) {
            log.error("Could not close the PCJ Storage instance.", e);
        }

        try {
            pcjUpdaterSupplier.get().close();
        } catch (final PcjUpdateException e) {
            log.error("Could not close the PCJ Updater instance.", e);
        }
    }

    /**
     * This is invoked when the host {@link RyaDAO#destroy()} method is invoked.
     */
    @Override
    public void destroy() {
        close();
    }

    /**
     * Deletes all data from the PCJ indices that are managed by a {@link PrecomputedJoinStorage}.
     */
    @Override
    public void purge(final RdfCloudTripleStoreConfiguration configuration) {
        final PrecomputedJoinStorage storage = pcjStorageSupplier.get();

        try {
            for(final String pcjId : storage.listPcjs()) {
                try {
                    storage.purge(pcjId);
                } catch(final PCJStorageException e) {
                    log.error("Could not purge the PCJ index with id: " + pcjId, e);
                }
            }
        } catch (final PCJStorageException e) {
            log.error("Could not purge the PCJ indicies because they could not be listed.", e);
        }
    }

    /**
     * Deletes all of the PCJ indices that are managed by {@link PrecomputedJoinStorage}.
     */
    @Override
    public void dropAndDestroy() {
        final PrecomputedJoinStorage storage = pcjStorageSupplier.get();

        try {
            for(final String pcjId : storage.listPcjs()) {
                try {
                    storage.dropPcj(pcjId);
                } catch(final PCJStorageException e) {
                    log.error("Could not delete the PCJ index with id: " + pcjId, e);
                }
            }
        } catch(final PCJStorageException e) {
            log.error("Could not delete the PCJ indicies because they could not be listed.", e);
        }
    }

    @Override
    public void setMultiTableBatchWriter(final MultiTableBatchWriter writer) throws IOException {
        // We do not need to use the writer that also writes to the core RYA tables.
    }

    @Override
    public void dropGraph(final RyaURI... graphs) {
        log.warn("PCJ indices do not store Graph metadata, so graph results can not be dropped.");
    }

    @Override
    public String getTableName() {
        // This method makes assumptions about how PCJs are stored. It's only
        // used by AccumuloRyaDAO to purge data, so it should be replaced with
        // a purge() method.
        log.warn("PCJ indicies are not stored within a single table, so this method can not be implemented.");
        return null;
    }
}