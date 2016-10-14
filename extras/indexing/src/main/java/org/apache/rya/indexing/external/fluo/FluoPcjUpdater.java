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
package org.apache.rya.indexing.external.fluo;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.log4j.Logger;
import org.apache.rya.indexing.pcj.fluo.api.InsertTriples;
import org.apache.rya.indexing.pcj.update.PrecomputedJoinUpdater;

import com.google.common.base.Optional;

import org.apache.fluo.api.client.FluoClient;
import org.apache.rya.api.domain.RyaStatement;

/**
 * Updates the PCJ indices by forwarding the statement additions/removals to
 * a Fluo application.
 */
@ParametersAreNonnullByDefault
public class FluoPcjUpdater implements PrecomputedJoinUpdater {
    private static final Logger log = Logger.getLogger(FluoPcjUpdater.class);

    // Used to only print the unsupported delete operation once.
    private boolean deleteWarningPrinted = false;

    private final FluoClient fluoClient;
    private final InsertTriples insertTriples = new InsertTriples();
    private final String statementVis;

    /**
     * Constructs an instance of {@link FluoPcjUpdater}.
     *
     * @param fluoClient - A connection to the Fluo table new statements will be
     *   inserted into and deleted from. (not null)
     * @param statementVis - The visibility label that will be applied to all
     *   statements that are inserted via the Fluo PCJ updater. (not null)
     */
    public FluoPcjUpdater(final FluoClient fluoClient, final String statementVis) {
        this.fluoClient = checkNotNull(fluoClient);
        this.statementVis = checkNotNull(statementVis);
    }

    @Override
    public void addStatements(final Collection<RyaStatement> statements) throws PcjUpdateException {
        insertTriples.insert(fluoClient, statements, Optional.of(statementVis));
    }

    @Override
    public void deleteStatements(final Collection<RyaStatement> statements) throws PcjUpdateException {
        // The Fluo application does not support statement deletion.
        if(!deleteWarningPrinted) {
            log.warn("The Fluo PCJ updating application does not support Statement deletion, " +
                    "but you are trying to use that feature. This may result in your PCJ index " +
                    "no longer reflecting the Statemetns that are stored in the core Rya tables.");
            deleteWarningPrinted = true;
        }
    }

    @Override
    public void flush() {
        // The Fluo application does not do any batching, so this doesn't do anything.
    }

    @Override
    public void close() {
        fluoClient.close();
    }
}