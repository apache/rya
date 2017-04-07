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
package org.apache.rya.indexing.pcj.fluo.api;

import static org.junit.Assert.assertEquals;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.config.ObserverSpecification;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.indexing.pcj.fluo.RyaExportITBase;
import org.junit.Test;

import com.google.common.base.Optional;

/**
 * Tests the methods of {@link CountStatements}.
 */
public class CountStatementsIT extends RyaExportITBase {

    /**
     * Overriden so that no Observers will be started. This ensures whatever
     * statements are inserted as part of the test will not be consumed.
     */
    @Override
    protected void preFluoInitHook() throws Exception {
        // Setup the observers that will be used by the Fluo PCJ Application.
        final List<ObserverSpecification> observers = new ArrayList<>();

        // Add the observers to the Fluo Configuration.
        super.getFluoConfiguration().addObservers(observers);
    }

    @Test
    public void countStatements() {
        // Insert some Triples into the Fluo app.
        final List<RyaStatement> triples = new ArrayList<>();
        triples.add( RyaStatement.builder().setSubject(new RyaURI("http://Alice")).setPredicate(new RyaURI("http://talksTo")).setObject(new RyaURI("http://Bob")).build() );
        triples.add( RyaStatement.builder().setSubject(new RyaURI("http://Bob")).setPredicate(new RyaURI("http://talksTo")).setObject(new RyaURI("http://Alice")).build() );
        triples.add( RyaStatement.builder().setSubject(new RyaURI("http://Charlie")).setPredicate(new RyaURI("http://talksTo")).setObject(new RyaURI("http://Bob")).build() );
        triples.add( RyaStatement.builder().setSubject(new RyaURI("http://David")).setPredicate(new RyaURI("http://talksTo")).setObject(new RyaURI("http://Bob")).build() );
        triples.add( RyaStatement.builder().setSubject(new RyaURI("http://Eve")).setPredicate(new RyaURI("http://talksTo")).setObject(new RyaURI("http://Bob")).build() );

        try(FluoClient fluoClient = FluoFactory.newClient(super.getFluoConfiguration())) {
            new InsertTriples().insert(fluoClient, triples, Optional.<String>absent());

            // Load some statements into the Fluo app.
            final BigInteger count = new CountStatements().countStatements(fluoClient);

            // Ensure the count matches the expected values.
            assertEquals(BigInteger.valueOf(5), count);
        }
    }
}