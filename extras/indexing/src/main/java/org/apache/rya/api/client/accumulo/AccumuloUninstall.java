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
package org.apache.rya.api.client.accumulo;

import static java.util.Objects.requireNonNull;

import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.log4j.Logger;
import org.apache.rya.accumulo.utils.RyaTableNames;
import org.apache.rya.api.client.InstanceDoesNotExistException;
import org.apache.rya.api.client.InstanceExists;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.api.client.Uninstall;
import org.apache.rya.api.instance.RyaDetailsRepository.RyaDetailsRepositoryException;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage.PCJStorageException;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * An Accumulo implementation of the {@link Uninstall} command.
 */
@DefaultAnnotation(NonNull.class)
public class AccumuloUninstall extends AccumuloCommand implements Uninstall {

    private static final Logger log = Logger.getLogger(AccumuloUninstall.class);

    private final InstanceExists instanceExists;

    /**
     * Constructs an instance of {@link AccumuloUninstall}.
     *
     * @param connectionDetails - Details about the values that were used to create
     *   the connector to the cluster. (not null)
     * @param connector - Provides programmatic access to the instance of Accumulo
     *   that hosts Rya instance. (not null)
     */
    public AccumuloUninstall(final AccumuloConnectionDetails connectionDetails, final Connector connector) {
        super(connectionDetails, connector);
        instanceExists = new AccumuloInstanceExists(connectionDetails, connector);
    }

    @Override
    public void uninstall(final String ryaInstanceName) throws InstanceDoesNotExistException, RyaClientException {
        requireNonNull(ryaInstanceName);

        // Ensure the Rya Instance exists.
        if(!instanceExists.exists(ryaInstanceName)) {
            throw new InstanceDoesNotExistException(String.format("There is no Rya instance named '%s'.", ryaInstanceName));
        }

        try {
            // Build the list of tables that are present within the Rya instance.
            final List<String> tables = new RyaTableNames().getTableNames(ryaInstanceName, getConnector());

            // Delete them.
            final TableOperations tableOps = getConnector().tableOperations();
            for(final String table : tables) {
                try {
                    tableOps.delete(table);
                } catch(final TableNotFoundException e) {
                    log.warn("Uninstall could not delete table named '" + table + "' because it does not exist. " +
                            "Something else is also deleting tables.");
                }
            }
        } catch (PCJStorageException | RyaDetailsRepositoryException e) {
            throw new RyaClientException("Could not uninstall the Rya instance named '" + ryaInstanceName +
                    "' because we could not determine which tables are associated with it.", e);
        } catch (AccumuloException | AccumuloSecurityException e) {
            throw new RyaClientException("Could not uninstall the Rya instance named '" + ryaInstanceName +
                    "' because of a problem interacting with Accumulo..", e);
        }
    }
}