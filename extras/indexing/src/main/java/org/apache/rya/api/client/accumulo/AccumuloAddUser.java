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
import org.apache.rya.api.client.AddUser;
import org.apache.rya.api.client.InstanceDoesNotExistException;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.api.instance.RyaDetails;
import org.apache.rya.api.instance.RyaDetailsRepository.RyaDetailsRepositoryException;
import org.apache.rya.api.instance.RyaDetailsUpdater;
import org.apache.rya.api.instance.RyaDetailsUpdater.RyaDetailsMutator.CouldNotApplyMutationException;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage.PCJStorageException;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * An Accumulo implementation of the {@link AddUser} command.
 */
@DefaultAnnotation(NonNull.class)
public class AccumuloAddUser extends AccumuloCommand implements AddUser {

    private static final TablePermissions TABLE_PERMISSIONS = new TablePermissions();

    /**
     * Constructs an instance of {@link AccumuloAddUser}.
     *
     * @param connectionDetails - Details about the values that were used to create
     *   the connector to the cluster. (not null)
     * @param connector - Provides programmatic access to the instance of Accumulo
     *   that hosts Rya instance. (not null)
     */
    public AccumuloAddUser(
            final AccumuloConnectionDetails connectionDetails,
            final Connector connector) {
        super(connectionDetails, connector);
    }

    @Override
    public void addUser(final String instanceName, final String username) throws InstanceDoesNotExistException, RyaClientException {
        requireNonNull(instanceName);
        requireNonNull(username);

        // If the user has already been added, then return immediately.
        try {
            final RyaDetails details = new AccumuloRyaInstanceDetailsRepository(getConnector(), instanceName).getRyaInstanceDetails();
            final List<String> users = details.getUsers();
            if(users.contains(username)) {
                return;
            }
        } catch (final RyaDetailsRepositoryException e) {
            throw new RyaClientException("Could not fetch the RyaDetails for Rya instance named '" + instanceName + ".", e);
        }

        // Update the instance details
        final RyaDetailsUpdater updater = new RyaDetailsUpdater( new AccumuloRyaInstanceDetailsRepository(getConnector(), instanceName) );
        try {
            updater.update(originalDetails -> RyaDetails.builder( originalDetails ).addUser(username).build());
        } catch (RyaDetailsRepositoryException | CouldNotApplyMutationException e) {
            throw new RyaClientException("Could not add the user '" + username + "' to the Rya instance's details.", e);
        }

        // Grant all access to all the instance's tables.
        try {
            // Build the list of tables that are present within the Rya instance.
            final List<String> tables = new RyaTableNames().getTableNames(instanceName, getConnector());

            // Update the user permissions for those tables.
            for(final String table : tables) {
                try {
                    TABLE_PERMISSIONS.grantAllPermissions(username, table, getConnector());
                } catch (AccumuloException | AccumuloSecurityException e) {
                    throw new RyaClientException("Could not grant access to table '" + table + "' for user '" + username + "'.", e);
                }
            }
        } catch (PCJStorageException | RyaDetailsRepositoryException e) {
            throw new RyaClientException("Could not determine which tables exist for the '" + instanceName + "' instance of Rya.", e);
        }
    }
}