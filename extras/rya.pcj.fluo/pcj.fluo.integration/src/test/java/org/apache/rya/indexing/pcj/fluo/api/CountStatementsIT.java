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

import org.apache.rya.indexing.pcj.fluo.ITBase;
import org.junit.Test;

import com.google.common.base.Optional;

import org.apache.fluo.api.client.FluoAdmin;
import org.apache.fluo.api.client.FluoAdmin.AlreadyInitializedException;
import org.apache.fluo.api.client.FluoAdmin.TableExistsException;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.config.ObserverSpecification;
import org.apache.fluo.api.mini.MiniFluo;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaURI;

/**
 * Tests the methods of {@link CountStatements}.
 */
public class CountStatementsIT extends ITBase {

    /**
     * Overriden so that no Observers will be started. This ensures whatever
     * statements are inserted as part of the test will not be consumed.
     *
     * @return A Mini Fluo cluster.
     */
    @Override
    protected MiniFluo startMiniFluo() throws AlreadyInitializedException, TableExistsException {
        // Setup the observers that will be used by the Fluo PCJ Application.
        final List<ObserverSpecification> observers = new ArrayList<>();

        // Configure how the mini fluo will run.
        final FluoConfiguration config = new FluoConfiguration();
        config.setMiniStartAccumulo(false);
        config.setAccumuloInstance(instanceName);
        config.setAccumuloUser(ACCUMULO_USER);
        config.setAccumuloPassword(ACCUMULO_PASSWORD);
        config.setInstanceZookeepers(zookeepers + "/fluo");
        config.setAccumuloZookeepers(zookeepers);

        config.setApplicationName(FLUO_APP_NAME);
        config.setAccumuloTable("fluo" + FLUO_APP_NAME);

        config.addObservers(observers);

        FluoFactory.newAdmin(config).initialize(
                new FluoAdmin.InitializationOptions().setClearTable(true).setClearZookeeper(true) );
        final MiniFluo miniFluo = FluoFactory.newMiniFluo(config);
        return miniFluo;
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

        new InsertTriples().insert(fluoClient, triples, Optional.<String>absent());

        // Load some statements into the Fluo app.
        final BigInteger count = new CountStatements().countStatements(fluoClient);

        // Ensure the count matches the expected values.
        assertEquals(BigInteger.valueOf(5), count);
    }
}