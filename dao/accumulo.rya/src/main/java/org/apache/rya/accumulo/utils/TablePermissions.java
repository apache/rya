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
package org.apache.rya.accumulo.utils;

import static java.util.Objects.requireNonNull;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.security.TablePermission;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A utility that makes it easier to grant and revoke table permissions within
 * an instance of Accumulo.
 */
@DefaultAnnotation(NonNull.class)
public class TablePermissions {

    /**
     * Grants the following Table Permissions for an Accumulo user to an Accumulo table.
     * <ul>
     *   <li>ALTER_TABLE</li>
     *   <li>BULK_IMPORT</li>
     *   <li>DROP_TABLE</li>
     *   <li>GRANT</li>
     *   <li>READ</li>
     *   <li>WRITE</li>
     * </ul>
     *
     * @param user - The user who will be granted the permissions. (not null)
     * @param table - The Accumulo table the permissions are granted to. (not null)
     * @param conn - The connector that is used to access the Accumulo instance
     *   that hosts the the {@code user} and {@code table}. (not null)
     * @throws AccumuloSecurityException If a general error occurs.
     * @throws AccumuloException If the user does not have permission to grant a user permissions.
     */
    public void grantAllPermissions(final String user, final String table, final Connector conn) throws AccumuloException, AccumuloSecurityException {
        requireNonNull(user);
        requireNonNull(table);
        requireNonNull(conn);

        final SecurityOperations secOps = conn.securityOperations();
        secOps.grantTablePermission(user, table, TablePermission.ALTER_TABLE);
        secOps.grantTablePermission(user, table, TablePermission.BULK_IMPORT);
        secOps.grantTablePermission(user, table, TablePermission.DROP_TABLE);
        secOps.grantTablePermission(user, table, TablePermission.GRANT);
        secOps.grantTablePermission(user, table, TablePermission.READ);
        secOps.grantTablePermission(user, table, TablePermission.WRITE);
    }

    /**
     * Revokes the following Table Permissions for an Accumulo user from an Accumulo table.
     * <ul>
     *   <li>ALTER_TABLE</li>
     *   <li>BULK_IMPORT</li>
     *   <li>DROP_TABLE</li>
     *   <li>GRANT</li>
     *   <li>READ</li>
     *   <li>WRITE</li>
     * </ul>
     *
     * @param user - The user whose permissions will be revoked. (not null)
     * @param table - The Accumulo table the permissions are revoked from. (not null)
     * @param conn - The connector that is used to access the Accumulo instance
     *   that hosts the the {@code user} and {@code table}. (not null)
     * @throws AccumuloException If a general error occurs.
     * @throws AccumuloSecurityException If the user does not have permission to revoke a user's permissions.
     */
    public void revokeAllPermissions(final String user, final String table, final Connector conn) throws AccumuloException, AccumuloSecurityException {
        requireNonNull(user);
        requireNonNull(table);
        requireNonNull(conn);

        final SecurityOperations secOps = conn.securityOperations();
        secOps.revokeTablePermission(user, table, TablePermission.ALTER_TABLE);
        secOps.revokeTablePermission(user, table, TablePermission.BULK_IMPORT);
        secOps.revokeTablePermission(user, table, TablePermission.DROP_TABLE);
        secOps.revokeTablePermission(user, table, TablePermission.GRANT);
        secOps.revokeTablePermission(user, table, TablePermission.READ);
        secOps.revokeTablePermission(user, table, TablePermission.WRITE);
    }
}