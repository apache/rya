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
package org.apache.rya.export.accumulo;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.log4j.Logger;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.api.RdfCloudTripleStoreConstants;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.api.persist.utils.RyaDAOHelper;
import org.apache.rya.api.resolver.RyaTripleContext;
import org.apache.rya.api.resolver.triple.TripleRowResolverException;
import org.apache.rya.export.accumulo.parent.AccumuloParentMetadataRepository;
import org.apache.rya.export.accumulo.util.AccumuloRyaUtils;
import org.apache.rya.export.api.metadata.MergeParentMetadata;
import org.apache.rya.export.api.metadata.ParentMetadataExistsException;
import org.apache.rya.export.api.store.AddStatementException;
import org.apache.rya.export.api.store.ContainsStatementException;
import org.apache.rya.export.api.store.FetchStatementException;
import org.apache.rya.export.api.store.RemoveStatementException;
import org.apache.rya.export.api.store.RyaStatementStore;
import org.apache.rya.export.api.store.UpdateStatementException;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Allows specific CRUD operations an Accumulo {@link RyaStatement} storage
 * system.
 * <p>
 * The operations specifically:
 * <li>fetch all rya statements in the store</li>
 * <li>add a rya statement to the store</li>
 * <li>remove a rya statement from the store</li>
 * <li>update an existing rya statement with a new one</li>
 *
 * One would use this {@link AccumuloRyaStatementStore} when they have an
 * Accumulo database that is used when merging in data or exporting data.
 */
public class AccumuloRyaStatementStore implements RyaStatementStore {
    private static final Logger log = Logger.getLogger(AccumuloRyaStatementStore.class);

    private final AccumuloRyaDAO accumuloRyaDao;
    private final String tablePrefix;
    private final Set<IteratorSetting> iteratorSettings = new HashSet<>();
    private final AccumuloParentMetadataRepository metadataRepo;

    /**
     * Creates a new instance of {@link AccumuloRyaStatementStore}.
     * @param dao the {@AccumuloRyaDAO}.
     * @param tablePrefix the Rya instance's table prefix.
     * @param ryaInstance the Rya instance name.
     */
    public AccumuloRyaStatementStore(final AccumuloRyaDAO dao, final String tablePrefix, final String ryaInstance) {
        this.tablePrefix = tablePrefix;
        if (tablePrefix != null) {
            RdfCloudTripleStoreConstants.prefixTables(tablePrefix);
        }
        accumuloRyaDao = dao;
        metadataRepo = new AccumuloParentMetadataRepository(dao);
    }

    @Override
    public Iterator<RyaStatement> fetchStatements() throws FetchStatementException {
        try {
            final RyaTripleContext ryaTripleContext = RyaTripleContext.getInstance(accumuloRyaDao.getConf());

            Scanner scanner = null;
            try {
                scanner = AccumuloRyaUtils.getScanner(tablePrefix + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX, accumuloRyaDao.getConf());
                for (final IteratorSetting iteratorSetting : iteratorSettings) {
                    scanner.addScanIterator(iteratorSetting);
                }
            } catch (final IOException e) {
                throw new FetchStatementException("Unable to get scanner to fetch Rya Statements", e);
            }
            // Convert Entry iterator to RyaStatement iterator
            final Iterator<Entry<Key, Value>> entryIter = scanner.iterator();
            final Iterator<RyaStatement> ryaStatementIter = Iterators.transform(entryIter, new Function<Entry<Key, Value>, RyaStatement>() {
               @Override
               public RyaStatement apply(final Entry<Key, Value> entry) {
                   final Key key = entry.getKey();
                   final Value value = entry.getValue();
                   RyaStatement ryaStatement = null;
                   try {
                       ryaStatement = AccumuloRyaUtils.createRyaStatement(key, value, ryaTripleContext);
                   } catch (final TripleRowResolverException e) {
                       log.error("Unable to convert the key/value pair into a Rya Statement", e);
                   }
                   return ryaStatement;
               }
            });
            return ryaStatementIter;
        } catch (final Exception e) {
            throw new FetchStatementException("Failed to fetch statements.", e);
        }
    }

    @Override
    public void addStatement(final RyaStatement statement) throws AddStatementException {
        try {
            accumuloRyaDao.add(statement);
            accumuloRyaDao.flush();
            //This is a hack since a statement re-added with the same timestamp won't reappear since its been marked for deletion.
            //RYA-197 is the ticket for fixing this hack.
            if(!containsStatement(statement)) {
                statement.setTimestamp(statement.getTimestamp() + 1L);
                accumuloRyaDao.add(statement);
            }
        } catch (final RyaDAOException | ContainsStatementException e) {
            throw new AddStatementException("Unable to add the Rya Statement", e);
        }
    }

    @Override
    public void removeStatement(final RyaStatement statement) throws RemoveStatementException {
        try {
            accumuloRyaDao.delete(statement, accumuloRyaDao.getConf());
        } catch (final RyaDAOException e) {
            throw new RemoveStatementException("Unable to delete the Rya Statement", e);
        }
    }

    @Override
    public void updateStatement(final RyaStatement original, final RyaStatement update) throws UpdateStatementException {
        try {
            removeStatement(original);
            addStatement(update);
        } catch (final AddStatementException | RemoveStatementException e) {
            throw new UpdateStatementException("Unable to update the Rya Statement", e);
        }
    }

    @Override
    public boolean containsStatement(final RyaStatement ryaStatement) throws ContainsStatementException {
        try {
            final RyaStatement resultRyaStatement = findStatement(ryaStatement);
            return resultRyaStatement != null;
        } catch (final RyaDAOException e) {
            throw new ContainsStatementException("Encountered an error while querying for statement.", e);
        }
    }

    @Override
    public Optional<MergeParentMetadata> getParentMetadata() {
        MergeParentMetadata metadata = null;
        try {
            metadata = metadataRepo.get();
        } catch (final Exception e) {
            // Catching any exception to ensure we always return Optional.ofNullable(metadata).
            // Logging at the debug level if exceptional behavior needs to be investigated while deployed.
            log.debug("Parent metadata missing or exceptional behavior occurred.", e);
        }
        return Optional.ofNullable(metadata);
    }

    @Override
    public void setParentMetadata(final MergeParentMetadata metadata) throws ParentMetadataExistsException {
        metadataRepo.set(metadata);
    }


    public RyaStatement findStatement(final RyaStatement ryaStatement) throws RyaDAOException {
        RyaStatement resultRyaStatement = null;
        CloseableIteration<RyaStatement, RyaDAOException> iter = null;
        try {
            iter = RyaDAOHelper.query(accumuloRyaDao.getQueryEngine(), ryaStatement, accumuloRyaDao.getConf());
            if (iter.hasNext()) {
                resultRyaStatement = iter.next();
            }
        } finally {
            if (iter != null) {
                iter.close();
            }
        }

        return resultRyaStatement;
    }

    /**
     * Adds an iterator setting to the statement store for it to use when it
     * fetches statements.
     * @param iteratorSetting the {@link IteratorSetting} to add.
     * (not {@code null})
     */
    public void addIterator(final IteratorSetting iteratorSetting) {
        checkNotNull(iteratorSetting);
        iteratorSettings.add(iteratorSetting);
    }
}