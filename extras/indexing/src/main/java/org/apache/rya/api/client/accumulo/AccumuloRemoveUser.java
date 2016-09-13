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
package org.apache.rya.api.client.accumulo;

import static java.util.Objects.requireNonNull;

import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.rya.accumulo.instance.AccumuloRyaInstanceDetailsRepository;
import org.apache.rya.accumulo.utils.RyaTableNames;
import org.apache.rya.accumulo.utils.TablePermissions;
import org.apache.rya.api.client.InstanceDoesNotExistException;
import org.apache.rya.api.client.RemoveUser;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.api.instance.RyaDetails;
import org.apache.rya.api.instance.RyaDetailsRepository.RyaDetailsRepositoryException;
import org.apache.rya.api.instance.RyaDetailsUpdater;
import org.apache.rya.api.instance.RyaDetailsUpdater.RyaDetailsMutator.CouldNotApplyMutationException;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage.PCJStorageException;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * An Accumulo implementation of the {@link DeleteUser} command.
 */
@DefaultAnnotation(NonNull.class)
public class AccumuloRemoveUser extends AccumuloCommand implements RemoveUser {

    private static final TablePermissions TABLE_PERMISSIONS = new TablePermissions();

    /**
     * Constructs an instance of {@link AccumuloDeleteUser}.
     *
     * @param connectionDetails - Details about the values that were used to create
     *   the connector to the cluster. (not null)
     * @param connector - Provides programmatic access to the instance of Accumulo
     *   that hosts Rya instance. (not null)
     */
    public AccumuloRemoveUser(
            final AccumuloConnectionDetails connectionDetails,
            final Connector connector) {
        super(connectionDetails, connector);
    }

    @Override
    public void removeUser(final String instanceName, final String username) throws InstanceDoesNotExistException, RyaClientException {
        requireNonNull(instanceName);
        requireNonNull(username);

        // Update the instance details.
        final RyaDetailsUpdater updater = new RyaDetailsUpdater( new AccumuloRyaInstanceDetailsRepository(getConnector(), instanceName) );
        try {
            updater.update(originalDetails -> RyaDetails.builder( originalDetails ).removeUser(username).build());
        } catch (RyaDetailsRepositoryException | CouldNotApplyMutationException e) {
            throw new RyaClientException("Could not remove the user '" + username + "' from the Rya instance's details.", e);
        }

        // Revoke all access to all the instance's tables.
        try {
            // Build the list of tables that are present within the Rya instance.
            final List<String> tables = new RyaTableNames().getTableNames(instanceName, getConnector());

            // Update the user permissions for those tables.
            for(final String table : tables) {
                try {
                    TABLE_PERMISSIONS.revokeAllPermissions(username, table, getConnector());
                } catch (AccumuloException | AccumuloSecurityException e) {
                    throw new RyaClientException("Could not revoke access to table '" + table + "' from user '" + username + "'.", e);
                }
            }
        } catch (PCJStorageException | RyaDetailsRepositoryException e) {
            throw new RyaClientException("Could not determine which tables exist for the '" + instanceName + "' instance of Rya.", e);
        }
    }
}