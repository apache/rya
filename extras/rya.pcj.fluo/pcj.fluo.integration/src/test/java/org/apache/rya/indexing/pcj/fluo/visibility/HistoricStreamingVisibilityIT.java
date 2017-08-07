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

import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.pcj.fluo.api.CreatePcj;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPcjStorage;
import org.apache.rya.pcj.fluo.test.base.RyaExportITBase;
import org.junit.Assert;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.impl.MapBindingSet;

import com.google.common.collect.Sets;

/**
 * Performs integration tests over the Fluo application geared towards various types of input.
 * <p>
 * These tests are being ignore so that they will not run as unit tests while building the application.
 */
public class HistoricStreamingVisibilityIT extends RyaExportITBase {

    /**
     * Ensure historic matches are included in the result.
     */
    @Test
    public void historicResults() throws Exception {
        // A query that finds people who talk to Eve and work at Chipotle.
        final String sparql =
              "SELECT ?x " +
                "WHERE { " +
                "?x <http://talksTo> <http://Eve>. " +
                "?x <http://worksAt> <http://Chipotle>." +
              "}";

        final Connector accumuloConn = super.getAccumuloConnector();
        accumuloConn.securityOperations().changeUserAuthorizations(getUsername(), new Authorizations("U","V","W"));
        final AccumuloRyaDAO dao = new AccumuloRyaDAO();
        dao.setConnector(accumuloConn);
        dao.setConf(makeConfig());
        dao.init();

        // Triples that are loaded into Rya before the PCJ is created.
        final ValueFactory vf = new ValueFactoryImpl();

        final Set<RyaStatement> historicTriples = Sets.newHashSet(
                makeRyaStatement(vf.createStatement(vf.createURI("http://Alice"), vf.createURI("http://talksTo"), vf.createURI("http://Eve")),"U"),
                makeRyaStatement(vf.createStatement(vf.createURI("http://Bob"), vf.createURI("http://talksTo"), vf.createURI("http://Eve")),"V"),
                makeRyaStatement(vf.createStatement(vf.createURI("http://Charlie"), vf.createURI("http://talksTo"), vf.createURI("http://Eve")),"W"),

                makeRyaStatement(vf.createStatement(vf.createURI("http://Eve"), vf.createURI("http://helps"), vf.createURI("http://Kevin")), "U"),

                makeRyaStatement(vf.createStatement(vf.createURI("http://Bob"), vf.createURI("http://worksAt"), vf.createURI("http://Chipotle")), "W"),
                makeRyaStatement(vf.createStatement(vf.createURI("http://Charlie"), vf.createURI("http://worksAt"), vf.createURI("http://Chipotle")), "V"),
                makeRyaStatement(vf.createStatement(vf.createURI("http://Eve"), vf.createURI("http://worksAt"), vf.createURI("http://Chipotle")), "U"),
                makeRyaStatement(vf.createStatement(vf.createURI("http://David"), vf.createURI("http://worksAt"), vf.createURI("http://Chipotle")), "V"));

        dao.add(historicTriples.iterator());
        dao.flush();

        // The expected results of the SPARQL query once the PCJ has been computed.
        final Set<BindingSet> expected = new HashSet<>();

        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("x", vf.createURI("http://Bob"));
        expected.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("x", vf.createURI("http://Charlie"));
        expected.add(bs);

        // Create the PCJ table.
        final PrecomputedJoinStorage pcjStorage = new AccumuloPcjStorage(accumuloConn, getRyaInstanceName());
        final String pcjId = pcjStorage.createPcj(sparql);

        try(FluoClient fluoClient = FluoFactory.newClient(super.getFluoConfiguration())) {
            new CreatePcj().withRyaIntegration(pcjId, pcjStorage, fluoClient, accumuloConn, getRyaInstanceName());
        }

        // Verify the end results of the query match the expected results.
        super.getMiniFluo().waitForObservers();

        final Set<BindingSet> results = Sets.newHashSet(pcjStorage.listResults(pcjId));
        Assert.assertEquals(expected, results);
    }

    private AccumuloRdfConfiguration makeConfig() {
        final AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
        conf.setTablePrefix(getRyaInstanceName());
        // Accumulo connection information.
        conf.set(ConfigUtils.CLOUDBASE_USER, getUsername());
        conf.set(ConfigUtils.CLOUDBASE_PASSWORD, getPassword());
        conf.set(ConfigUtils.CLOUDBASE_INSTANCE, super.getMiniAccumuloCluster().getInstanceName());
        conf.set(ConfigUtils.CLOUDBASE_ZOOKEEPERS, super.getMiniAccumuloCluster().getZooKeepers());
        conf.set(RdfCloudTripleStoreConfiguration.CONF_QUERY_AUTH, "U,V,W");

        return conf;
    }

    private static RyaStatement makeRyaStatement(final Statement statement, final String visibility) throws UnsupportedEncodingException {
    	final RyaStatement ryaStatement = RdfToRyaConversions.convertStatement(statement);
    	ryaStatement.setColumnVisibility(visibility.getBytes("UTF-8"));
    	return ryaStatement;
    }
}