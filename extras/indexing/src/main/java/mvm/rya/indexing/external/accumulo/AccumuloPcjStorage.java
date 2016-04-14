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
package mvm.rya.indexing.external.accumulo;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.accumulo.core.client.Connector;
import org.apache.rya.indexing.pcj.storage.PcjMetadata;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage;
import org.apache.rya.indexing.pcj.storage.accumulo.PcjTableNameFactory;
import org.apache.rya.indexing.pcj.storage.accumulo.PcjTables;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;

/**
 * An Accumulo backed implementation of {@link PrecomputedJoinStorage}.
 */
@ParametersAreNonnullByDefault
public class AccumuloPcjStorage implements PrecomputedJoinStorage {

    private final PcjTableNameFactory pcjIdFactory = new PcjTableNameFactory();
    private final PcjTables pcjTables = new PcjTables();

    private final Connector accumuloConn;
    private final String ryaInstanceName;

    /**
     * Constructs an instance of {@link AccumuloPcjStorage}.
     *
     * @param accumuloConn - The connector that will be used to connect to  Accumulo. (not null)
     * @param ryaInstanceName - The name of the RYA instance that will be accessed. (not null)
     */
    public AccumuloPcjStorage(final Connector accumuloConn, final String ryaInstanceName) {
        this.accumuloConn = checkNotNull(accumuloConn);
        this.ryaInstanceName = checkNotNull(ryaInstanceName);
    }

    @Override
    public List<String> listPcjs() throws PCJStorageException {
        return pcjTables.listPcjTables(accumuloConn, ryaInstanceName);
    }

    @Override
    public String createPcj(final String sparql, final Set<VariableOrder> varOrders) throws PCJStorageException {
        final String pcjId = pcjIdFactory.makeTableName(ryaInstanceName);
        pcjTables.createPcjTable(accumuloConn, pcjId, varOrders, sparql);
        return pcjId;
    }

    @Override
    public PcjMetadata getPcjMetadata(final String pcjId) throws PCJStorageException {
        return pcjTables.getPcjMetadata(accumuloConn, pcjId);
    }

    @Override
    public void addResults(final String pcjId, final Collection<VisibilityBindingSet> results) throws PCJStorageException {
        pcjTables.addResults(accumuloConn, pcjId, results);
    }

    @Override
    public void purge(final String pcjId) throws PCJStorageException {
        pcjTables.purgePcjTable(accumuloConn, pcjId);
    }

    @Override
    public void dropPcj(final String pcjId) throws PCJStorageException {
        pcjTables.dropPcjTable(accumuloConn, pcjId);
    }

    @Override
    public void close() throws PCJStorageException {
        // Accumulo Connectors don't require closing.
    }
}