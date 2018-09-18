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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;
import java.util.Optional;

import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.export.api.metadata.MergeParentMetadata;
import org.apache.rya.export.api.metadata.ParentMetadataExistsException;

/**
 * Decorates a {@link RyaStatementStore}.  This is to be used when the default
 * actions for {@link RyaStatement}s in the {@link RyaStatementStore} need to
 * do something more specific.
 */
public abstract class RyaStatementStorePolicy implements RyaStatementStore {
    protected final RyaStatementStore store;

    /**
     * Creates a new {@link RyaStatementStorePolicy} around the provided {@link RyaStatementStore}.
     * @param store - The {@link RyaStatementStore} to decorate.
     */
    public RyaStatementStorePolicy(final RyaStatementStore store) {
        this.store = checkNotNull(store);
    }

    @Override
    public Iterator<RyaStatement> fetchStatements() throws FetchStatementException {
        return store.fetchStatements();
    }

    @Override
    public void addStatement(final RyaStatement statement) throws AddStatementException {
        store.addStatement(statement);
    }

    @Override
    public void addStatements(final Iterator<RyaStatement> statements) throws AddStatementException {
        store.addStatements(statements);
    }

    @Override
    public void removeStatement(final RyaStatement statement) throws RemoveStatementException {
        store.removeStatement(statement);
    }

    @Override
    public void updateStatement(final RyaStatement original, final RyaStatement update) throws UpdateStatementException {
        store.updateStatement(original, update);
    }

    @Override
    public boolean containsStatement(final RyaStatement statement) throws ContainsStatementException {
        return store.containsStatement(statement);
    }

    @Override
    public Optional<MergeParentMetadata> getParentMetadata() {
        return store.getParentMetadata();
    }

    @Override
    public void setParentMetadata(final MergeParentMetadata metadata) throws ParentMetadataExistsException {
        store.setParentMetadata(metadata);
    }

    @Override
    public String getRyaInstanceName() {
        return store.getRyaInstanceName();
    }

    @Override
    public long count() {
        return store.count();
    }
}
