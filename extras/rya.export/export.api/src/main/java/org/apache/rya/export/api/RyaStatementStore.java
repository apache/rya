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
package org.apache.rya.export.api;

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
     * @return an {@link Iterator} contianing all {@link RyaStatement}s found
     * in this {@link RyaStatementStore}.
     */
    public Iterator<RyaStatement> fetchStatements();

    /**
     * @param statement - The {@link RyaStatement} to add to this {@link RyaStatementStore}.
     */
    public void addStatement(final RyaStatement statement);

    /**
     * @param statement - The {@link RyaStatement} to remove from this {@link RyaStatementStore}.
     */
    public void removeStatement(final RyaStatement statement);

    /**
     * Updates the original {@link RyaStatement} with a new one.
     * @param originial - The {@link RyaStatement} to update.
     * @param update - The new {@link RyaStatement} to replace the original one.
     */
    public void updateStatement(final RyaStatement originial, final RyaStatement update);
}
