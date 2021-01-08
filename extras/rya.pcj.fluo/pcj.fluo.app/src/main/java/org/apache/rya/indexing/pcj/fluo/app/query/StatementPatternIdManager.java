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

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Bytes;

import java.util.HashSet;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.STATEMENT_PATTERN_ID;
import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.VAR_DELIM;
import static org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns.STATEMENT_PATTERN_IDS;
import static org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns.STATEMENT_PATTERN_IDS_HASH;

/**
 * Utility class for updating and removing StatementPattern nodeIds in the Fluo table. All StatementPattern nodeIds are
 * stored in a single set under a single entry in the Fluo table. This is to eliminate the need for a scan to find all
 * StatementPattern nodeIds every time a new Triple enters the Fluo table. Instead, the nodeIds are cached locally, and
 * only updated when the local hash of the nodeId set is dirty (not equal to the hash in the Fluo table).
 */
public class StatementPatternIdManager {

    /**
     * Add specified Set of ids to the Fluo table with Column {@link FluoQueryColumns#STATEMENT_PATTERN_IDS}. Also
     * updates the hash of the updated nodeId Set and writes that to the Column
     * {@link FluoQueryColumns#STATEMENT_PATTERN_IDS_HASH}
     *
     * @param tx - Fluo Transaction object for performing atomic operations on Fluo table.
     * @param ids - ids to add to the StatementPattern nodeId Set
     */
    public static void addStatementPatternIds(TransactionBase tx, Set<String> ids) {
        checkNotNull(tx);
        checkNotNull(ids);
        Optional<Bytes> val = Optional.fromNullable(tx.get(Bytes.of(STATEMENT_PATTERN_ID), STATEMENT_PATTERN_IDS));
        StringBuilder builder = new StringBuilder();
        if (val.isPresent()) {
            builder.append(val.get().toString());
            builder.append(VAR_DELIM);
        }
        String idString = builder.append(Joiner.on(VAR_DELIM).join(ids)).toString();
        tx.set(Bytes.of(STATEMENT_PATTERN_ID), STATEMENT_PATTERN_IDS, Bytes.of(idString));
        tx.set(Bytes.of(STATEMENT_PATTERN_ID), STATEMENT_PATTERN_IDS_HASH, Bytes.of(Hashing.sha256().hashUnencodedChars(idString).toString()));
    }

    /**
     * Remove specified Set of ids from the Fluo table and updates the entry with Column
     * {@link FluoQueryColumns#STATEMENT_PATTERN_IDS}. Also updates the hash of the updated nodeId Set and writes that
     * to the Column {@link FluoQueryColumns#STATEMENT_PATTERN_IDS_HASH}
     *
     * @param tx - Fluo Transaction object for performing atomic operations on Fluo table.
     * @param ids - ids to remove from the StatementPattern nodeId Set
     */
    public static void removeStatementPatternIds(TransactionBase tx, Set<String> ids) {
        checkNotNull(tx);
        checkNotNull(ids);
        Optional<Bytes> val = Optional.fromNullable(tx.get(Bytes.of(STATEMENT_PATTERN_ID), STATEMENT_PATTERN_IDS));
        Set<String> storedIds = new HashSet<>();
        if (val.isPresent()) {
            storedIds = Sets.newHashSet(val.get().toString().split(VAR_DELIM));
        }
        storedIds.removeAll(ids);
        String idString = Joiner.on(VAR_DELIM).join(ids);
        tx.set(Bytes.of(STATEMENT_PATTERN_ID), STATEMENT_PATTERN_IDS, Bytes.of(idString));
        tx.set(Bytes.of(STATEMENT_PATTERN_ID), STATEMENT_PATTERN_IDS_HASH, Bytes.of(Hashing.sha256().hashUnencodedChars(idString).toString()));
    }

}
