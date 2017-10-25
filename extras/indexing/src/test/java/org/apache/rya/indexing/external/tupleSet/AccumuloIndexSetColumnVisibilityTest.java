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

package org.apache.rya.indexing.external.tupleSet;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.instance.AccumuloRyaInstanceDetailsRepository;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.instance.RyaDetails;
import org.apache.rya.api.instance.RyaDetails.EntityCentricIndexDetails;
import org.apache.rya.api.instance.RyaDetails.FreeTextIndexDetails;
import org.apache.rya.api.instance.RyaDetails.JoinSelectivityDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails;
import org.apache.rya.api.instance.RyaDetails.ProspectorDetails;
import org.apache.rya.api.instance.RyaDetails.TemporalIndexDetails;
import org.apache.rya.api.instance.RyaDetailsRepository;
import org.apache.rya.api.instance.RyaDetailsRepository.RyaDetailsRepositoryException;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage.PCJStorageException;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPcjStorage;
import org.apache.rya.indexing.pcj.storage.accumulo.PcjTableNameFactory;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

/**
 * Tests the evaluation of {@link AccumuloIndexSet}.
 */
public class AccumuloIndexSetColumnVisibilityTest {
    private static final Logger log = Logger.getLogger(AccumuloIndexSetColumnVisibilityTest.class);

    // Accumulo cluster resources.
    private static MiniAccumuloCluster accumulo;
    private static String instance;
    private static String zooKeepers;
    private static Connector accCon;

    // Rya resources.
    private static String ryaInstanceName = "rya_";
    private static Configuration conf;
    private static AccumuloPcjStorage storage;

    // PCJ values used when testing.
    private static String pcjId;
    private static QueryBindingSet pcjBs1;
    private static QueryBindingSet pcjBs2;
    private static ValueFactory vf = SimpleValueFactory.getInstance();

    @BeforeClass
    public static void init() throws AccumuloException, AccumuloSecurityException, PCJStorageException, IOException, InterruptedException, TableNotFoundException,
            RyaDetailsRepositoryException {
        // Setup the mini accumulo instance used by the test.
        accumulo = startMiniAccumulo();
        accumulo.getZooKeepers();
        instance = accumulo.getInstanceName();
        zooKeepers = accumulo.getZooKeepers();
        conf = getConf();
        accCon.securityOperations().changeUserAuthorizations("root", new Authorizations("U","USA"));

        // Initialize the Rya Details for the Rya instance.
        initRyaDetails();

        // Initialize a PCJ.
        storage = new AccumuloPcjStorage(accCon, ryaInstanceName);

        pcjId = storage.createPcj(
                "SELECT ?name ?age " + "{" +
                    "?name <http://hasAge> ?age ." +
                    "?name <http://playsSport> \"Soccer\" " +
                "}");

        // Store the PCJ's results.
        pcjBs1 = new QueryBindingSet();
        pcjBs1.addBinding("age", vf.createLiteral(BigInteger.valueOf(14)));
        pcjBs1.addBinding("name", vf.createIRI("http://Alice"));

        pcjBs2 = new QueryBindingSet();
        pcjBs2.addBinding("age", vf.createLiteral(BigInteger.valueOf(16)));
        pcjBs2.addBinding("name", vf.createIRI("http://Bob"));

        final Set<VisibilityBindingSet> visBs = new HashSet<>();
        for (final BindingSet bs : Sets.<BindingSet>newHashSet(pcjBs1, pcjBs2)) {
            visBs.add(new VisibilityBindingSet(bs, "U|USA"));
        }

        storage.addResults(pcjId, visBs);
    }

    @AfterClass
    public static void close() throws RepositoryException, PCJStorageException {
        storage.close();

        if (accumulo != null) {
            try {
                log.info("Shutting down the Mini Accumulo being used as a Rya store.");
                accumulo.stop();
                log.info("Mini Accumulo being used as a Rya store shut down.");
            } catch (final Exception e) {
                log.error("Could not shut down the Mini Accumulo.", e);
            }
        }
    }

    private static MiniAccumuloCluster startMiniAccumulo() throws IOException, InterruptedException, AccumuloException, AccumuloSecurityException {
        final File miniDataDir = Files.createTempDir();

        // Setup and start the Mini Accumulo.
        final MiniAccumuloCluster accumulo = new MiniAccumuloCluster(
                miniDataDir, "password");
        accumulo.start();

        // Store a connector to the Mini Accumulo.
        final Instance instance = new ZooKeeperInstance(
                accumulo.getInstanceName(), accumulo.getZooKeepers());
        accCon = instance.getConnector("root", new PasswordToken("password"));

        return accumulo;
    }

    private static void initRyaDetails() throws RyaDetailsRepositoryException {
        // Initialize the Rya Details for the instance.
        final RyaDetailsRepository detailsRepo = new AccumuloRyaInstanceDetailsRepository(accCon, ryaInstanceName);

        final RyaDetails details = RyaDetails.builder()
                .setRyaInstanceName(ryaInstanceName)
                .setRyaVersion("0.0.0.0")
                .setFreeTextDetails( new FreeTextIndexDetails(true) )
                .setEntityCentricIndexDetails( new EntityCentricIndexDetails(true) )
//RYA-215                .setGeoIndexDetails( new GeoIndexDetails(true) )
                .setTemporalIndexDetails( new TemporalIndexDetails(true) )
                .setPCJIndexDetails(
                        PCJIndexDetails.builder()
                            .setEnabled(true) )
                .setJoinSelectivityDetails( new JoinSelectivityDetails( Optional.absent() ) )
                .setProspectorDetails( new ProspectorDetails( Optional.absent() ))
                .build();

        detailsRepo.initialize(details);
    }

    private static Configuration getConf() {
        final AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
        conf.set(RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX, ryaInstanceName);
        conf.set(ConfigUtils.CLOUDBASE_USER, "root");
        conf.set(ConfigUtils.CLOUDBASE_PASSWORD, "password");
        conf.set(ConfigUtils.CLOUDBASE_INSTANCE, instance);
        conf.set(ConfigUtils.CLOUDBASE_ZOOKEEPERS, zooKeepers);
        conf.set(RdfCloudTripleStoreConfiguration.CONF_QUERY_AUTH, "U,USA");
        return conf;
    }

    @Test
    public void variableInstantiationTest() throws Exception {
        // Setup the object that will be tested.
        final String pcjTableName = new PcjTableNameFactory().makeTableName(ryaInstanceName, pcjId);
        final AccumuloIndexSet ais = new AccumuloIndexSet(conf, pcjTableName);

        // Setup the binding sets that will be evaluated.
        final QueryBindingSet bs = new QueryBindingSet();
        bs.addBinding("name", vf.createIRI("http://Alice"));
        final QueryBindingSet bs2 = new QueryBindingSet();
        bs2.addBinding("name", vf.createIRI("http://Bob"));

        final Set<BindingSet> bSets = Sets.newHashSet(bs, bs2);
        final CloseableIteration<BindingSet, QueryEvaluationException> results = ais.evaluate(bSets);

        final Set<BindingSet> fetchedResults = new HashSet<>();
        while (results.hasNext()) {
            final BindingSet next = results.next();
            fetchedResults.add(next);
        }

        final Set<BindingSet> expected = Sets.newHashSet(pcjBs1, pcjBs2);
        assertEquals(expected, fetchedResults);
    }

    @Test
    public void accumuloIndexSetTestAttemptJoinAccrossTypes() throws Exception {
        // Setup the object that will be tested.
        final String pcjTableName = new PcjTableNameFactory().makeTableName(ryaInstanceName, pcjId);
        final AccumuloIndexSet ais = new AccumuloIndexSet(conf, pcjTableName);

        // Setup the binding sets that will be evaluated.
        final QueryBindingSet bs1 = new QueryBindingSet();
        bs1.addBinding("age", vf.createLiteral(BigInteger.valueOf(16)));
        final QueryBindingSet bs2 = new QueryBindingSet();
        bs2.addBinding("age", vf.createLiteral(BigInteger.valueOf(14)));

        final Set<BindingSet> bSets = Sets.newHashSet(bs1, bs2);
        final CloseableIteration<BindingSet, QueryEvaluationException> results = ais.evaluate(bSets);

        final Set<BindingSet> fetchedResults = new HashSet<>();
        while (results.hasNext()) {
            final BindingSet next = results.next();
            fetchedResults.add(next);
        }

        final Set<BindingSet> expected = Sets.newHashSet(pcjBs1, pcjBs2);
        assertEquals(expected, fetchedResults);
    }
}