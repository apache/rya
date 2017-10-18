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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.hadoop.io.Text;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.api.RdfTripleStoreConfiguration;
import org.apache.rya.api.client.RyaClient;
import org.apache.rya.api.client.accumulo.AccumuloRyaClientFactory;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.external.PrecomputedJoinIndexerConfig;
import org.apache.rya.indexing.pcj.fluo.api.CreateFluoPcj;
import org.apache.rya.indexing.pcj.fluo.api.InsertTriples;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage.CloseableIterator;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPcjStorage;
import org.apache.rya.indexing.pcj.storage.accumulo.PcjTableNameFactory;
import org.apache.rya.pcj.fluo.test.base.RyaExportITBase;
import org.apache.rya.rdftriplestore.RyaSailRepository;
import org.apache.rya.sail.config.RyaSailFactory;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.sail.Sail;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Integration tests that ensure the Fluo Application properly exports PCJ
 * results with the correct Visibility values.
 */
public class PcjVisibilityIT extends RyaExportITBase {

    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    // Constants used within the test.
    private static final IRI ALICE = vf.createIRI("urn:Alice");
    private static final IRI BOB = vf.createIRI("urn:Bob");
    private static final IRI TALKS_TO = vf.createIRI("urn:talksTo");
    private static final IRI LIVES_IN = vf.createIRI("urn:livesIn");
    private static final IRI WORKS_AT = vf.createIRI("urn:worksAt");
    private static final IRI HAPPYVILLE = vf.createIRI("urn:Happyville");
    private static final IRI BURGER_JOINT = vf.createIRI("urn:BurgerJoint");

    @Test
    public void visibilitySimplified() throws Exception {
        // Create a PCJ index within Rya.
        final String sparql =
                "SELECT ?customer ?worker ?city " +
                "{ " +
                  "?customer <" + TALKS_TO + "> ?worker. " +
                  "?worker <" + LIVES_IN + "> ?city. " +
                  "?worker <" + WORKS_AT + "> <" + BURGER_JOINT + ">. " +
                "}";

        final Connector accumuloConn = super.getAccumuloConnector();
        final String instanceName = super.getMiniAccumuloCluster().getInstanceName();
        final String zookeepers = super.getMiniAccumuloCluster().getZooKeepers();

        final RyaClient ryaClient = AccumuloRyaClientFactory.build(createConnectionDetails(), accumuloConn);

        final String pcjId = ryaClient.getCreatePCJ().createPCJ(getRyaInstanceName(), sparql);

        // Grant the root user the "u" authorization.
        super.getAccumuloConnector().securityOperations().changeUserAuthorizations(getUsername(), new Authorizations("u"));

        // Setup a connection to the Rya instance that uses the "u" authorizations. This ensures
        // any statements that are inserted will have the "u" authorization on them and that the
        // PCJ updating application will have to maintain visibilities.
        final AccumuloRdfConfiguration ryaConf = new AccumuloRdfConfiguration();
        ryaConf.setTablePrefix(getRyaInstanceName());

        // Accumulo connection information.
        ryaConf.setAccumuloUser(getUsername());
        ryaConf.setAccumuloPassword(getPassword());
        ryaConf.setAccumuloInstance(super.getAccumuloConnector().getInstance().getInstanceName());
        ryaConf.setAccumuloZookeepers(super.getAccumuloConnector().getInstance().getZooKeepers());
        ryaConf.set(ConfigUtils.CLOUDBASE_AUTHS, "u");
        ryaConf.set(RdfTripleStoreConfiguration.CONF_CV, "u");

        // PCJ configuration information.
        ryaConf.set(ConfigUtils.USE_PCJ, "true");
        ryaConf.set(ConfigUtils.USE_PCJ_UPDATER_INDEX, "true");
        ryaConf.set(ConfigUtils.FLUO_APP_NAME, super.getFluoConfiguration().getApplicationName());
        ryaConf.set(ConfigUtils.PCJ_STORAGE_TYPE,
                PrecomputedJoinIndexerConfig.PrecomputedJoinStorageType.ACCUMULO.toString());
        ryaConf.set(ConfigUtils.PCJ_UPDATER_TYPE,
                PrecomputedJoinIndexerConfig.PrecomputedJoinUpdaterType.FLUO.toString());

        Sail sail = null;
        RyaSailRepository ryaRepo = null;
        RepositoryConnection ryaConn = null;

        try {
            sail = RyaSailFactory.getInstance(ryaConf);
            ryaRepo = new RyaSailRepository(sail);
            ryaConn = ryaRepo.getConnection();

            // Load a few Statements into Rya.
            ryaConn.add(VF.createStatement(ALICE, TALKS_TO, BOB));
            ryaConn.add(VF.createStatement(BOB, LIVES_IN, HAPPYVILLE));
            ryaConn.add(VF.createStatement(BOB, WORKS_AT, BURGER_JOINT));

            // Wait for Fluo to finish processing.
            super.getMiniFluo().waitForObservers();

            // Fetch the exported result and show that its column visibility has been simplified.
            final String pcjTableName = new PcjTableNameFactory().makeTableName(getRyaInstanceName(), pcjId);
            final Scanner scan = accumuloConn.createScanner(pcjTableName, new Authorizations("u"));
            scan.fetchColumnFamily(new Text("customer;worker;city"));

            final Entry<Key, Value> result = scan.iterator().next();
            final Key key = result.getKey();
            assertEquals(new Text("u"), key.getColumnVisibility());

        } finally {
            if(ryaConn != null) {
                try {
                    ryaConn.close();
                } finally { }
            }

            if(ryaRepo != null) {
                try {
                    ryaRepo.shutDown();
                } finally { }
            }

            if(sail != null) {
                try {
                    sail.shutDown();
                } finally { }
            }
        }
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
        addStatementVisibilityEntry(streamedTriples, new RyaStatement(new RyaURI("http://Alice"), new RyaURI("http://talksTo"),new RyaURI("http://Bob")), "A&B");
        addStatementVisibilityEntry(streamedTriples, new RyaStatement(new RyaURI("http://Bob"), new RyaURI("http://livesIn"),new RyaURI("http://London")), "A");
        addStatementVisibilityEntry(streamedTriples, new RyaStatement(new RyaURI("http://Bob"), new RyaURI("http://worksAt"),new RyaURI("http://Chipotle")), "B");

        addStatementVisibilityEntry(streamedTriples, new RyaStatement(new RyaURI("http://Alice"), new RyaURI("http://talksTo"),new RyaURI("http://Charlie")), "B&C");
        addStatementVisibilityEntry(streamedTriples, new RyaStatement(new RyaURI("http://Charlie"), new RyaURI("http://livesIn"),new RyaURI("http://London")), "B");
        addStatementVisibilityEntry(streamedTriples, new RyaStatement(new RyaURI("http://Charlie"), new RyaURI("http://worksAt"),new RyaURI("http://Chipotle")), "C");

        addStatementVisibilityEntry(streamedTriples, new RyaStatement(new RyaURI("http://Alice"), new RyaURI("http://talksTo"),new RyaURI("http://David")), "C&D");
        addStatementVisibilityEntry(streamedTriples, new RyaStatement(new RyaURI("http://David"), new RyaURI("http://livesIn"),new RyaURI("http://London")), "C");
        addStatementVisibilityEntry(streamedTriples, new RyaStatement(new RyaURI("http://David"), new RyaURI("http://worksAt"),new RyaURI("http://Chipotle")), "D");

        addStatementVisibilityEntry(streamedTriples, new RyaStatement(new RyaURI("http://Alice"), new RyaURI("http://talksTo"),new RyaURI("http://Eve")), "D&E");
        addStatementVisibilityEntry(streamedTriples, new RyaStatement(new RyaURI("http://Eve"), new RyaURI("http://livesIn"),new RyaURI("http://Leeds")), "D");
        addStatementVisibilityEntry(streamedTriples, new RyaStatement(new RyaURI("http://Eve"), new RyaURI("http://worksAt"),new RyaURI("http://Chipotle")), "E");

        addStatementVisibilityEntry(streamedTriples, new RyaStatement(new RyaURI("http://Frank"), new RyaURI("http://talksTo"),new RyaURI("http://Alice")), "");
        addStatementVisibilityEntry(streamedTriples, new RyaStatement(new RyaURI("http://Frank"), new RyaURI("http://livesIn"),new RyaURI("http://London")), "");
        addStatementVisibilityEntry(streamedTriples, new RyaStatement(new RyaURI("http://Frank"), new RyaURI("http://worksAt"),new RyaURI("http://Chipotle")), "");

        final Connector accumuloConn = super.getAccumuloConnector();

        // Create the PCJ Table in Accumulo.
        final PrecomputedJoinStorage rootStorage = new AccumuloPcjStorage(accumuloConn, getRyaInstanceName());
        final String pcjId = rootStorage.createPcj(sparql);


        try( final FluoClient fluoClient = FluoFactory.newClient( super.getFluoConfiguration() )) {
            // Create the PCJ in Fluo.
            new CreateFluoPcj().withRyaIntegration(pcjId, rootStorage, fluoClient, accumuloConn, getRyaInstanceName());

            // Stream the data into Fluo.
            for(final RyaStatement statement : streamedTriples.keySet()) {
                final Optional<String> visibility = Optional.of(streamedTriples.get(statement));
                new InsertTriples().insert(fluoClient, statement, visibility);
            }
        }

        // Fetch the exported results from Accumulo once the observers finish working.
        super.getMiniFluo().waitForObservers();

        setupTestUsers(accumuloConn, getRyaInstanceName(), pcjId);

        // Verify ABCDE using root.
        final Set<BindingSet> rootResults = toSet( rootStorage.listResults(pcjId));

        final Set<BindingSet> rootExpected = Sets.newHashSet();
        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("customer", vf.createIRI("http://Alice"));
        bs.addBinding("worker", vf.createIRI("http://Bob"));
        bs.addBinding("city", vf.createIRI("http://London"));
        rootExpected.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("customer", vf.createIRI("http://Alice"));
        bs.addBinding("worker", vf.createIRI("http://Charlie"));
        bs.addBinding("city", vf.createIRI("http://London"));
        rootExpected.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("customer", vf.createIRI("http://Alice"));
        bs.addBinding("worker", vf.createIRI("http://Eve"));
        bs.addBinding("city", vf.createIRI("http://Leeds"));
        rootExpected.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("customer", vf.createIRI("http://Alice"));
        bs.addBinding("worker", vf.createIRI("http://David"));
        bs.addBinding("city", vf.createIRI("http://London"));
        rootExpected.add(bs);

        assertEquals(rootExpected, rootResults);

        final MiniAccumuloCluster cluster = super.getMiniAccumuloCluster();

        // Verify AB
        final Connector abConn = cluster.getConnector("abUser", "password");
        try(final PrecomputedJoinStorage abStorage = new AccumuloPcjStorage(abConn, getRyaInstanceName())) {
            final Set<BindingSet> abResults = toSet( abStorage.listResults(pcjId) );

            final Set<BindingSet> abExpected = Sets.newHashSet();
            bs = new MapBindingSet();
            bs.addBinding("customer", vf.createIRI("http://Alice"));
            bs.addBinding("worker", vf.createIRI("http://Bob"));
            bs.addBinding("city", vf.createIRI("http://London"));
            abExpected.add(bs);

            assertEquals(abExpected, abResults);
        }

        // Verify ABC
        final Connector abcConn = cluster.getConnector("abcUser", "password");
        try(final PrecomputedJoinStorage abcStorage = new AccumuloPcjStorage(abcConn, getRyaInstanceName())) {
            final Set<BindingSet> abcResults = toSet( abcStorage.listResults(pcjId) );

            final Set<BindingSet> abcExpected = Sets.newHashSet();
            bs = new MapBindingSet();
            bs.addBinding("customer", vf.createIRI("http://Alice"));
            bs.addBinding("worker", vf.createIRI("http://Bob"));
            bs.addBinding("city", vf.createIRI("http://London"));
            abcExpected.add(bs);

            bs = new MapBindingSet();
            bs.addBinding("customer", vf.createIRI("http://Alice"));
            bs.addBinding("worker", vf.createIRI("http://Charlie"));
            bs.addBinding("city", vf.createIRI("http://London"));
            abcExpected.add(bs);

            assertEquals(abcExpected, abcResults);
        }

        // Verify ADE
        final Connector adeConn = cluster.getConnector("adeUser", "password");
        try(final PrecomputedJoinStorage adeStorage = new AccumuloPcjStorage(adeConn, getRyaInstanceName())) {
            final Set<BindingSet> adeResults = toSet( adeStorage.listResults(pcjId) );

            final Set<BindingSet> adeExpected = Sets.newHashSet();
            bs = new MapBindingSet();
            bs.addBinding("customer", vf.createIRI("http://Alice"));
            bs.addBinding("worker", vf.createIRI("http://Eve"));
            bs.addBinding("city", vf.createIRI("http://Leeds"));
            adeExpected.add(bs);

            assertEquals(adeExpected, adeResults);
        }

        // Verify no auths.
        final Connector noAuthConn = cluster.getConnector("noAuth", "password");
        try(final PrecomputedJoinStorage noAuthStorage = new AccumuloPcjStorage(noAuthConn, getRyaInstanceName())) {
            final Set<BindingSet> noAuthResults = toSet( noAuthStorage.listResults(pcjId) );
            assertTrue( noAuthResults.isEmpty() );
        }
    }

    private void setupTestUsers(final Connector accumuloConn, final String ryaInstanceName, final String pcjId) throws AccumuloException, AccumuloSecurityException {
        final PasswordToken pass = new PasswordToken("password");
        final SecurityOperations secOps = accumuloConn.securityOperations();

        // We need the table name so that we can update security for the users.
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

    private Set<BindingSet> toSet(final CloseableIterator<BindingSet> bindingSets) throws Exception {
        final Set<BindingSet> set = new HashSet<>();
        try {
            while(bindingSets.hasNext()) {
                set.add( bindingSets.next() );
            }
        } finally {
            bindingSets.close();
        }
        return set;
    }
}