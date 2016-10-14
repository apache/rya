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
package org.apache.rya.indexing.pcj.fluo.client.util;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.rya.indexing.pcj.fluo.api.InsertTriples;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.helpers.RDFHandlerBase;

import com.google.common.base.Optional;

import org.apache.fluo.api.client.FluoClient;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.resolver.RdfToRyaConversions;

/**
 * When used as the handler of an {@link RDFParser}, instances of this class
 * will batch load {@link Statement}s into the Fluo app 1000 statements at a time.
 */
public class FluoLoader extends RDFHandlerBase {
    private static final Logger log = LogManager.getLogger(FluoLoader.class);

    private static final int FLUSH_SIZE = 1000;
    private final ArrayList<RyaStatement> buff = new ArrayList<>(1000);

    private final FluoClient fluoClient;
    private final InsertTriples insertTriples;

    /**
     * Constructs an instance of {@link FluoLoader}.
     *
     * @param fluoClient - The client that will be used to connect to Fluo. (not null)
     * @param insertTriples - The interactor that loads triples into a Fluo table. (not null)
     */
    public FluoLoader(final FluoClient fluoClient, final InsertTriples insertTriples) {
        this.fluoClient = checkNotNull(fluoClient);
        this.insertTriples = checkNotNull(insertTriples);
    }

    @Override
    public void startRDF() throws RDFHandlerException {
        log.trace("Start of RDF file encountered.");
    }

    @Override
    public void handleStatement(final Statement st) throws RDFHandlerException {
        // If the buffer is full, flush it to the Fluo table.
        if(buff.size() == FLUSH_SIZE) {
            log.trace("Flushing " + buff.size() + " Statements from the buffer to Fluo.");
            insertTriples.insert(fluoClient, buff, Optional.<String>absent());
            buff.clear();
        }

        // Enqueue the statement for the next job.
        final RyaStatement ryaSt = RdfToRyaConversions.convertStatement(st);
        buff.add( ryaSt );
    }

    @Override
    public void endRDF() throws RDFHandlerException {
        log.trace("End of RDF file encountered.");

        if(!buff.isEmpty()) {
            log.trace("Flushing the last " + buff.size() + " Statements from the buffer to Fluo.");
            insertTriples.insert(fluoClient, buff, Optional.<String>absent());
            buff.clear();
        }
    }
}