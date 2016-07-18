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
package org.apache.rya.indexing.pcj.fluo.visibility;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.rya.indexing.pcj.fluo.ITBase;
import org.apache.rya.indexing.pcj.fluo.api.CreatePcj;
import org.apache.rya.indexing.pcj.fluo.api.InsertTriples;
import org.apache.rya.indexing.pcj.fluo.app.export.rya.RyaExportParameters;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPcjStorage;
import org.apache.rya.indexing.pcj.storage.accumulo.PcjTableNameFactory;
import org.junit.Test;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.impl.BindingImpl;

import com.beust.jcommander.internal.Sets;
import com.google.common.base.Optional;

import mvm.rya.api.domain.RyaStatement;

public class PcjVisibilityIT extends ITBase {

    /**
     * Configure the export observer to use the Mini Accumulo instance as the
     * export destination for new PCJ results.
     */
    @Override
    protected Map<String, String> makeExportParams() {
        final HashMap<String, String> params = new HashMap<>();

        final RyaExportParameters ryaParams = new RyaExportParameters(params);
        ryaParams.setExportToRya(true);
        ryaParams.setAccumuloInstanceName(instanceName);
        ryaParams.setZookeeperServers(zookeepers);
        ryaParams.setExporterUsername(ITBase.ACCUMULO_USER);
        ryaParams.setExporterPassword(ITBase.ACCUMULO_PASSWORD);
        ryaParams.setRyaInstanceName(RYA_INSTANCE_NAME);
        return params;
    }

    @Test
    public void createWithVisibilityFluo() throws Exception {
        final String sparql =
                "SELECT ?customer ?worker ?city " +
                "{ " +
                  "?customer <http://talksTo> ?worker. " +
                  "?worker <http://livesIn> ?city. " +
                  "?worker <http://worksAt> <http://Chipotle>. " +
                "}";

        // Triples that will be streamed into Fluo after the PCJ has been created.
        final Map<RyaStatement, String> streamedTriples = new HashMap<>();
        addStatementVisibilityEntry(streamedTriples, makeRyaStatement("http://Alice", "http://talksTo", "http://Bob"), "A&B");
        addStatementVisibilityEntry(streamedTriples, makeRyaStatement("http://Bob", "http://livesIn", "http://London"), "A");
        addStatementVisibilityEntry(streamedTriples, makeRyaStatement("http://Bob", "http://worksAt", "http://Chipotle"), "B");

        addStatementVisibilityEntry(streamedTriples, makeRyaStatement("http://Alice", "http://talksTo", "http://Charlie"), "B&C");
        addStatementVisibilityEntry(streamedTriples, makeRyaStatement("http://Charlie", "http://livesIn", "http://London"), "B");
        addStatementVisibilityEntry(streamedTriples, makeRyaStatement("http://Charlie", "http://worksAt", "http://Chipotle"), "C");

        addStatementVisibilityEntry(streamedTriples, makeRyaStatement("http://Alice", "http://talksTo", "http://David"), "C&D");
        addStatementVisibilityEntry(streamedTriples, makeRyaStatement("http://David", "http://livesIn", "http://London"), "C");
        addStatementVisibilityEntry(streamedTriples, makeRyaStatement("http://David", "http://worksAt", "http://Chipotle"), "D");

        addStatementVisibilityEntry(streamedTriples, makeRyaStatement("http://Alice", "http://talksTo", "http://Eve"), "D&E");
        addStatementVisibilityEntry(streamedTriples, makeRyaStatement("http://Eve", "http://livesIn", "http://Leeds"), "D");
        addStatementVisibilityEntry(streamedTriples, makeRyaStatement("http://Eve", "http://worksAt", "http://Chipotle"), "E");

        addStatementVisibilityEntry(streamedTriples, makeRyaStatement("http://Frank", "http://talksTo", "http://Alice"), "");
        addStatementVisibilityEntry(streamedTriples, makeRyaStatement("http://Frank", "http://livesIn", "http://London"), "");
        addStatementVisibilityEntry(streamedTriples, makeRyaStatement("http://Frank", "http://worksAt", "http://Chipotle"), "");

        // Create the PCJ Table in Accumulo.
        final PrecomputedJoinStorage rootStorage = new AccumuloPcjStorage(accumuloConn, RYA_INSTANCE_NAME);
        final String pcjId = rootStorage.createPcj(sparql);

        // Create the PCJ in Fluo.
        new CreatePcj().withRyaIntegration(pcjId, rootStorage, fluoClient, ryaRepo);

        // Stream the data into Fluo.
        for(final RyaStatement statement : streamedTriples.keySet()) {
            new InsertTriples().insert(fluoClient, statement, Optional.of(streamedTriples.get(statement)));
        }

        // Fetch the exported results from Accumulo once the observers finish working.
        fluo.waitForObservers();

        setupTestUsers(accumuloConn, RYA_INSTANCE_NAME, pcjId);

        // Verify ABCDE using root.
        final Set<BindingSet> rootResults = toSet( rootStorage.listResults(pcjId));

        final Set<BindingSet> rootExpected = Sets.newHashSet();
        rootExpected.add( makeBindingSet(
                new BindingImpl("customer", new URIImpl("http://Alice")),
                new BindingImpl("worker", new URIImpl("http://Bob")),
                new BindingImpl("city", new URIImpl("http://London"))));
        rootExpected.add( makeBindingSet(
                new BindingImpl("customer", new URIImpl("http://Alice")),
                new BindingImpl("worker", new URIImpl("http://Charlie")),
                new BindingImpl("city", new URIImpl("http://London"))));
        rootExpected.add( makeBindingSet(
                new BindingImpl("customer", new URIImpl("http://Alice")),
                new BindingImpl("worker", new URIImpl("http://Eve")),
                new BindingImpl("city", new URIImpl("http://Leeds"))));
        rootExpected.add( makeBindingSet(
                new BindingImpl("customer", new URIImpl("http://Alice")),
                new BindingImpl("worker", new URIImpl("http://David")),
                new BindingImpl("city", new URIImpl("http://London"))));

        assertEquals(rootExpected, rootResults);

        // Verify AB
        final Connector abConn = cluster.getConnector("abUser", "password");
        final PrecomputedJoinStorage abStorage = new AccumuloPcjStorage(abConn, RYA_INSTANCE_NAME);
        final Set<BindingSet> abResults = toSet( abStorage.listResults(pcjId) );

        final Set<BindingSet> abExpected = Sets.newHashSet();
        abExpected.add( makeBindingSet(
                new BindingImpl("customer", new URIImpl("http://Alice")),
                new BindingImpl("worker", new URIImpl("http://Bob")),
                new BindingImpl("city", new URIImpl("http://London"))));

        assertEquals(abExpected, abResults);

        // Verify ABC
        final Connector abcConn = cluster.getConnector("abcUser", "password");
        final PrecomputedJoinStorage abcStorage = new AccumuloPcjStorage(abcConn, RYA_INSTANCE_NAME);
        final Set<BindingSet> abcResults = toSet( abcStorage.listResults(pcjId) );

        final Set<BindingSet> abcExpected = Sets.newHashSet();
        abcExpected.add(makeBindingSet(
                new BindingImpl("customer", new URIImpl("http://Alice")),
                new BindingImpl("worker", new URIImpl("http://Bob")),
                new BindingImpl("city", new URIImpl("http://London"))));
        abcExpected.add(makeBindingSet(
                new BindingImpl("customer", new URIImpl("http://Alice")),
                new BindingImpl("worker", new URIImpl("http://Charlie")),
                new BindingImpl("city", new URIImpl("http://London"))));

        assertEquals(abcExpected, abcResults);

        // Verify ADE
        final Connector adeConn = cluster.getConnector("adeUser", "password");
        final PrecomputedJoinStorage adeStorage = new AccumuloPcjStorage(adeConn, RYA_INSTANCE_NAME);
        final Set<BindingSet> adeResults = toSet( adeStorage.listResults(pcjId) );

        final Set<BindingSet> adeExpected = Sets.newHashSet();
        adeExpected.add(makeBindingSet(
                new BindingImpl("customer", new URIImpl("http://Alice")),
                new BindingImpl("worker", new URIImpl("http://Eve")),
                new BindingImpl("city", new URIImpl("http://Leeds"))));

        assertEquals(adeExpected, adeResults);

        // Verify no auths.
        final Connector noAuthConn = cluster.getConnector("noAuth", "password");
        final PrecomputedJoinStorage noAuthStorage = new AccumuloPcjStorage(noAuthConn, RYA_INSTANCE_NAME);
        final Set<BindingSet> noAuthResults = toSet( noAuthStorage.listResults(pcjId) );
        assertTrue( noAuthResults.isEmpty() );
    }

    private void setupTestUsers(final Connector accumuloConn, final String ryaInstanceName, final String pcjId) throws AccumuloException, AccumuloSecurityException {
        final PasswordToken pass = new PasswordToken("password");
        final SecurityOperations secOps = accumuloConn.securityOperations();

        // XXX We need the table name so that we can update security for the users.
        final String pcjTableName = new PcjTableNameFactory().makeTableName(ryaInstanceName, pcjId);

        // Give the 'roor' user authorizations to see everything.
        secOps.changeUserAuthorizations("root", new Authorizations("A", "B", "C", "D", "E"));

        // Create a user that can see things with A and B.
        secOps.createLocalUser("abUser", pass);
        secOps.changeUserAuthorizations("abUser", new Authorizations("A", "B"));
        secOps.grantTablePermission("abUser", pcjTableName, TablePermission.READ);

        // Create a user that can see things with A, B, and C.
        secOps.createLocalUser("abcUser", pass);
        secOps.changeUserAuthorizations("abcUser", new Authorizations("A", "B", "C"));
        secOps.grantTablePermission("abcUser", pcjTableName, TablePermission.READ);

        // Create a user that can see things with A, D, and E.
        secOps.createLocalUser("adeUser", pass);
        secOps.changeUserAuthorizations("adeUser", new Authorizations("A", "D", "E"));
        secOps.grantTablePermission("adeUser", pcjTableName, TablePermission.READ);

        // Create a user that can't see anything.
        secOps.createLocalUser("noAuth", pass);
        secOps.changeUserAuthorizations("noAuth", new Authorizations());
        secOps.grantTablePermission("noAuth", pcjTableName, TablePermission.READ);
    }

    protected static void addStatementVisibilityEntry(final Map<RyaStatement, String> triplesMap, final RyaStatement statement, final String visibility) {
        triplesMap.put(statement, visibility);
    }

    private Set<BindingSet> toSet(final Iterable<BindingSet> bindingSets) {
        final Set<BindingSet> set = new HashSet<>();
        for(final BindingSet bindingSet : bindingSets) {
            set.add( bindingSet );
        }
        return set;
    }
}
