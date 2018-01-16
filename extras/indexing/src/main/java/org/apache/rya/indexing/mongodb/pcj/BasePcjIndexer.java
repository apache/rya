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
package org.apache.rya.indexing.mongodb.pcj;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Collections.singleton;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.groupingBy;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.indexing.entity.model.Entity;
import org.apache.rya.indexing.entity.storage.EntityStorage;
import org.apache.rya.indexing.entity.storage.EntityStorage.EntityStorageException;
import org.apache.rya.indexing.pcj.storage.mongo.MongoPcjDocuments;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;
import org.apache.rya.mongodb.MongoSecondaryIndex;
import org.openrdf.model.URI;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A base class that may be used to update an {@link EntityStorage} as new
 * {@link RyaStatement}s are added to/removed from the Rya instance.
 */
@DefaultAnnotation(NonNull.class)
public abstract class BasePcjIndexer implements PcjIndexer, MongoSecondaryIndex {

    private final AtomicReference<MongoDBRdfConfiguration> configuration = new AtomicReference<>();
    private final AtomicReference<MongoPcjDocuments> pcjDocs = new AtomicReference<>();

    @Override
    public void setConf(final Configuration conf) {
        requireNonNull(conf);
        pcjDocs.set( getPcjStorage(conf) );
    }

    @Override
    public Configuration getConf() {
        return configuration.get();
    }

    @Override
    public void storeStatement(final RyaStatement statement) throws IOException {
        requireNonNull(statement);
        storeStatements( singleton(statement) );
    }

    @Override
    public void storeStatements(final Collection<RyaStatement> statements) throws IOException {
        requireNonNull(statements);

        final Map<RyaURI,List<RyaStatement>> groupedBySubject = statements.stream()
            .collect(groupingBy(RyaStatement::getSubject));

        for(final Entry<RyaURI, List<RyaStatement>> entry : groupedBySubject.entrySet()) {
            try {
                updateEntity(entry.getKey(), entry.getValue());
            } catch (final EntityStorageException e) {
                throw new IOException("Failed to update the Entity index.", e);
            }
        }
    }

    /**
     * Updates a {@link Entity} to reflect new {@link RyaStatement}s.
     *
     * @param subject - The Subject of the {@link Entity} the statements are for. (not null)
     * @param statements - Statements that the {@link Entity} will be updated with. (not null)
     */
    private void updateEntity(final RyaURI subject, final Collection<RyaStatement> statements) throws EntityStorageException {
        requireNonNull(subject);
        requireNonNull(statements);

        final MongoPcjDocuments pcjDocStore = pcjDocs.get();
        checkState(pcjDocStore != null, "Must set this indexers configuration before storing statements.");
    }

    @Override
    public void deleteStatement(final RyaStatement statement) throws IOException {
        requireNonNull(statement);

        final MongoPcjDocuments pcjDocStore = pcjDocs.get();
        checkState(pcjDocStore != null, "Must set this indexers configuration before storing statements.");
    }

    @Override
    public String getTableName() {
        // Storage details have been abstracted away from the indexer.
        return null;
    }

    @Override
    public void flush() throws IOException {
        // We do not need to do anything to flush since we do not batch work.
    }

    @Override
    public void close() throws IOException {
        // Nothing to close.
    }

    @Override
    public void dropGraph(final RyaURI... graphs) {
    }

    @Override
    public Set<URI> getIndexablePredicates() {
        // This isn't used anywhere in Rya, so it will not be implemented.
        return null;
    }
}
