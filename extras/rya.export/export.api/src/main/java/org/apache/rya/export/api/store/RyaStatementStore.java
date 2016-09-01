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
package org.apache.rya.export.api.store;

import java.util.Iterator;

import mvm.rya.api.domain.RyaStatement;

/**
 * Allows specific CRUD operations on {@link RyaStatement} storage systems.
 * <p>
 * The operations specifically:
 * <li>fetch all rya statements in the store</li>
 * <li>add a rya statement to the store</li>
 * <li>remove a rya statement from the store</li>
 * <li>update an existing rya statement with a new one</li>
 *
 * One would use this {@link RyaStatementStore} when they have a database or
 * some storage system that is used when merging in data or exporting data.
 */
public interface RyaStatementStore {
    /**
     * @return an {@link Iterator} containing all {@link RyaStatement}s found
     * in this {@link RyaStatementStore}.  The statements will be sorted by
     * timestamp.
     * @throws FetchStatementException - Thrown when fetching a statement fails.
     */
    public Iterator<RyaStatement> fetchStatements() throws FetchStatementException;

    /**
     * @param statement - The {@link RyaStatement} to add to this {@link RyaStatementStore}.
     * @throws AddStatementException Thrown when adding a statement fails.
     */
    public void addStatement(final RyaStatement statement) throws AddStatementException;

    /**
     * @param statement - The {@link RyaStatement} to remove from this {@link RyaStatementStore}.
     * @throws RemoveStatementException - Thrown when the statement is not removed
     */
    public void removeStatement(final RyaStatement statement) throws RemoveStatementException;

    /**
     * Updates the original {@link RyaStatement} with a new one.
     * @param original - The {@link RyaStatement} to update.
     * @param update - The new {@link RyaStatement} to replace the original one.
     * @throws UpdateStatementException - Thrown when updating a statement fails.
     */
    public void updateStatement(final RyaStatement original, final RyaStatement update) throws UpdateStatementException;

    /**
     * Queries to see if the statement is contained in the statement store.
     * @param ryaStatement the {@link RyaStatement} to search for.
     * @return {@code true} if the statement store contains the statement.
     * {@code false} otherwise.
     * @throws ContainsStatementException - Thrown when an exception occurs trying to check for the statement.
     */
    public boolean containsStatement(final RyaStatement ryaStatement) throws ContainsStatementException;
}
