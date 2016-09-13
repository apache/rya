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
package org.apache.rya.accumulo.utils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.rya.accumulo.instance.AccumuloRyaInstanceDetailsRepository;
import org.apache.rya.api.instance.RyaDetails;
import org.apache.rya.api.instance.RyaDetailsRepository;
import org.apache.rya.api.instance.RyaDetailsRepository.NotInitializedException;
import org.apache.rya.api.instance.RyaDetailsRepository.RyaDetailsRepositoryException;
import org.apache.rya.api.layout.TableLayoutStrategy;
import org.apache.rya.api.layout.TablePrefixLayoutStrategy;
import org.apache.rya.indexing.accumulo.entity.EntityCentricIndex;
import org.apache.rya.indexing.accumulo.freetext.AccumuloFreeTextIndexer;
import org.apache.rya.indexing.accumulo.temporal.AccumuloTemporalIndexer;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage.PCJStorageException;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPcjStorage;
import org.apache.rya.indexing.pcj.storage.accumulo.PcjTableNameFactory;

/**
 * A utility that may be used to determine which Accumulo tables are part of  a Rya instance.
 */
public class RyaTableNames {

    /**
     * Get the the Accumulo table names that are used by an instance of Rya.
     *
     * @param ryaInstanceName - The name of the Rya instance. (not null)
     * @param conn - A connector to the host Accumulo instance. (not null)
     * @return The Accumulo table names that are used by the Rya instance.
     * @throws NotInitializedException The instance's Rya Details have not been initialized.
     * @throws RyaDetailsRepositoryException General problem with the Rya Details repository.
     * @throws PCJStorageException General problem with the PCJ storage.
     */
    public List<String> getTableNames(final String ryaInstanceName, final Connector conn) throws NotInitializedException, RyaDetailsRepositoryException, PCJStorageException {
        // Build the list of tables that may be present within the Rya instance.
        final List<String> tables = new ArrayList<>();

        // Core Rya tables.
        final TableLayoutStrategy coreTableNames = new TablePrefixLayoutStrategy(ryaInstanceName);
        tables.add( coreTableNames.getSpo() );
        tables.add( coreTableNames.getPo() );
        tables.add( coreTableNames.getOsp() );
        tables.add( coreTableNames.getEval() );
        tables.add( coreTableNames.getNs() );
        tables.add( coreTableNames.getProspects() );
        tables.add( coreTableNames.getSelectivity() );

        // Rya Details table.
        tables.add( AccumuloRyaInstanceDetailsRepository.makeTableName(ryaInstanceName) );

        // Secondary Indexer Tables.
        final RyaDetailsRepository detailsRepo = new AccumuloRyaInstanceDetailsRepository(conn, ryaInstanceName);
        final RyaDetails details = detailsRepo.getRyaInstanceDetails();

        if(details.getEntityCentricIndexDetails().isEnabled()) {
            tables.add( EntityCentricIndex.makeTableName(ryaInstanceName) );
        }

        if(details.getFreeTextIndexDetails().isEnabled()) {
            tables.addAll( AccumuloFreeTextIndexer.makeTableNames(ryaInstanceName) );
        }

        if(details.getTemporalIndexDetails().isEnabled()) {
            tables.add( AccumuloTemporalIndexer.makeTableName(ryaInstanceName) );
        }

/**
 *         if(details.getGeoIndexDetails().isEnabled()) {
 *             tables.add( GeoMesaGeoIndexer.makeTableName(ryaInstanceName) );
 *         }
 */

        if(details.getPCJIndexDetails().isEnabled()) {
            final List<String> pcjIds = new AccumuloPcjStorage(conn, ryaInstanceName).listPcjs();

            final PcjTableNameFactory tableNameFactory = new PcjTableNameFactory();
            for(final String pcjId : pcjIds) {
                tables.add( tableNameFactory.makeTableName(ryaInstanceName, pcjId) );
            }
        }

        // Verify they actually exist. If any don't, remove them from the list.
        final TableOperations tableOps = conn.tableOperations();

        final Iterator<String> tablesIt = tables.iterator();
        while(tablesIt.hasNext()) {
            final String table = tablesIt.next();
            if(!tableOps.exists(table)) {
                tablesIt.remove();
            }
        }

        return tables;
    }
}