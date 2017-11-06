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
package org.apache.rya.indexing.pcj.fluo.app.query;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.STATEMENT_PATTERN_ID;
import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.VAR_DELIM;
import static org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns.STATEMENT_PATTERN_IDS;
import static org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns.STATEMENT_PATTERN_IDS_HASH;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Bytes;

import com.google.common.collect.Sets;

/**
 * This class caches the StatementPattern Ids so they don't have
 * to be looked up each time a new Statement needs to be processed
 * in the TripleObserver.
 *
 */
public class StatementPatternIdCache {

    private final ReentrantLock lock = new ReentrantLock();
    private static Optional<String> HASH = Optional.empty();
    private static Set<String> IDS = new HashSet<>();

    /**
     * This method retrieves the StatementPattern NodeIds registered in the Fluo table.
     * To determine whether the StatementPattern NodeIds have changed in the underlying Fluo table,
     * this class maintains a local hash of the ids.  When this method is called, it looks up the
     * hash of the StatementPattern Id Strings in the Fluo table, and only if it is different
     * than the local hash will the StatementPattern nodeIds be retrieved from the Fluo table.  Otherwise,
     * this method returns a local cache of the StatementPattern nodeIds.  This method is thread safe.
     * @param tx
     * @return - Set of StatementPattern nodeIds
     */
    public Set<String> getStatementPatternIds(TransactionBase tx) {
        checkNotNull(tx);
        Optional<Bytes> hashBytes = Optional.ofNullable(tx.get(Bytes.of(STATEMENT_PATTERN_ID), STATEMENT_PATTERN_IDS_HASH));
        if (hashBytes.isPresent()) {
            String hash = hashBytes.get().toString();
            if ((HASH.isPresent() && HASH.get().equals(hash))) {
                return IDS;
            }
            lock.lock();
            try {
                String ids = tx.get(Bytes.of(STATEMENT_PATTERN_ID), STATEMENT_PATTERN_IDS).toString();
                IDS = Sets.newHashSet(ids.split(VAR_DELIM));
                HASH = Optional.of(hash);
                return IDS;
            } finally {
                lock.unlock();
            }
        }
        return IDS;
    }

    /**
     * Clears contexts of cache so that it will be re-populated next time
     * {@link StatementPatternIdCache#getStatementPatternIds(TransactionBase)} is called.
     */
    public void clear() {
        HASH = Optional.empty();
        IDS.clear();
    }

}
