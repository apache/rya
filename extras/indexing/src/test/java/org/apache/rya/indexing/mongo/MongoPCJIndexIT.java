/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.indexing.mongo;

import static org.junit.Assert.assertEquals;

import java.util.HashSet;
import java.util.Set;

import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.RdfCloudTripleStoreConstants;
import org.apache.rya.api.client.Install.InstallConfiguration;
import org.apache.rya.api.client.RyaClient;
import org.apache.rya.api.client.mongo.MongoConnectionDetails;
import org.apache.rya.api.client.mongo.MongoRyaClientFactory;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;
import org.apache.rya.mongodb.MongoITBase;
import org.apache.rya.sail.config.RyaSailFactory;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.impl.MapBindingSet;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.Sail;

public class MongoPCJIndexIT extends MongoITBase {
    private static final ValueFactory VF = ValueFactoryImpl.getInstance();

    @Override
    protected void updateConfiguration(final MongoDBRdfConfiguration conf) {
        conf.setBoolean(ConfigUtils.USE_MONGO, true);
        conf.setBoolean(ConfigUtils.USE_PCJ, false);
    }

    @Test
    public void sparqlQuery_Test() throws Exception {
        // Setup a Rya Client.
        final MongoConnectionDetails connectionDetails = getConnectionDetails();
        final RyaClient ryaClient = MongoRyaClientFactory.build(connectionDetails, getMongoClient());
        final String pcjQuery = "SELECT ?name WHERE {"
        		+ " ?name <urn:likes> <urn:icecream> ."
        		+ " ?name <urn:hasEyeColor> <urn:blue> ."
        		+ " }";

        // Install an instance of Rya and load statements.
        ryaClient.getInstall().install(conf.getRyaInstanceName(), InstallConfiguration.builder()
                .setEnablePcjIndex(true)
                .build());
        ryaClient.getLoadStatements().loadStatements(conf.getRyaInstanceName(), getStatements());
        final String pcjId = ryaClient.getCreatePCJ().createPCJ(conf.getRyaInstanceName(), pcjQuery);
        ryaClient.getBatchUpdatePCJ().batchUpdate(conf.getRyaInstanceName(), pcjId);

        //purge contents of rya triples collection
        getMongoClient().getDatabase(conf.getRyaInstanceName()).getCollection(conf.getTriplesCollectionName()).drop();
        
        //run the query.  since the triples collection is gone, if the results match, they came from the PCJ index.
        conf.setBoolean(ConfigUtils.USE_PCJ, true);
        conf.setBoolean(ConfigUtils.USE_OPTIMAL_PCJ, true);
        conf.setBoolean(ConfigUtils.DISPLAY_QUERY_PLAN, true);
        final Sail sail = RyaSailFactory.getInstance(conf);
        SailRepositoryConnection conn = new SailRepository(sail).getConnection();
        conn.begin();
        final TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, pcjQuery);
        tupleQuery.setBinding(RdfCloudTripleStoreConfiguration.CONF_QUERYPLAN_FLAG, RdfCloudTripleStoreConstants.VALUE_FACTORY.createLiteral(true));
        final TupleQueryResult rez = tupleQuery.evaluate();
        final Set<BindingSet> results = new HashSet<>();
        while(rez.hasNext()) {
            final BindingSet bs = rez.next();
            results.add(bs);
        }
        
     // Verify the correct results were loaded into the PCJ table.
        final Set<BindingSet> expectedResults = new HashSet<>();

        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("name", VF.createURI("urn:Alice"));
        expectedResults.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("name", VF.createURI("urn:Bob"));
        expectedResults.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("name", VF.createURI("urn:Charlie"));
        expectedResults.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("name", VF.createURI("urn:David"));
        expectedResults.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("name", VF.createURI("urn:Eve"));
        expectedResults.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("name", VF.createURI("urn:Frank"));
        expectedResults.add(bs);

        assertEquals(6, results.size());
        assertEquals(expectedResults, results);
    }
    
    @Test
    public void sparqlQuery_Test_complex() throws Exception {
        // Setup a Rya Client.
        final MongoConnectionDetails connectionDetails = getConnectionDetails();
        final RyaClient ryaClient = MongoRyaClientFactory.build(connectionDetails, getMongoClient());
        final String pcjQuery = "SELECT ?name WHERE {"
        		+ " ?name <urn:likes> <urn:icecream> ."
        		+ " ?name <urn:hasEyeColor> <urn:blue> ."
        		+ " }";
        
        final String testQuery = 
        		  "SELECT ?name WHERE {"
        		+ " ?name <urn:hasHairColor> <urn:brown> ."
        		+ " ?name <urn:likes> <urn:icecream> ."
        		+ " ?name <urn:hasEyeColor> <urn:blue> ."
        		+ " }";

        // Install an instance of Rya and load statements.
        conf.setBoolean(ConfigUtils.USE_PCJ, true);
        conf.setBoolean(ConfigUtils.USE_OPTIMAL_PCJ, true);
        conf.setBoolean(ConfigUtils.DISPLAY_QUERY_PLAN, true);
        ryaClient.getInstall().install(conf.getRyaInstanceName(), InstallConfiguration.builder()
                .setEnablePcjIndex(true)
                .build());
        ryaClient.getLoadStatements().loadStatements(conf.getRyaInstanceName(), getStatements());
        final String pcjId = ryaClient.getCreatePCJ().createPCJ(conf.getRyaInstanceName(), pcjQuery);
        ryaClient.getBatchUpdatePCJ().batchUpdate(conf.getRyaInstanceName(), pcjId);

        System.out.println("Triples: " + getMongoClient().getDatabase(conf.getRyaInstanceName()).getCollection(conf.getTriplesCollectionName()).count());
        System.out.println("PCJS: " + getMongoClient().getDatabase(conf.getRyaInstanceName()).getCollection("pcjs").count());
        
        //run the query.  since the triples collection is gone, if the results match, they came from the PCJ index.
        final Sail sail = RyaSailFactory.getInstance(conf);
        SailRepositoryConnection conn = new SailRepository(sail).getConnection();
        conn.begin();
        final TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, testQuery);
        tupleQuery.setBinding(RdfCloudTripleStoreConfiguration.CONF_QUERYPLAN_FLAG, RdfCloudTripleStoreConstants.VALUE_FACTORY.createLiteral(true));
        final TupleQueryResult rez = tupleQuery.evaluate();

        final Set<BindingSet> results = new HashSet<>();
        while(rez.hasNext()) {
            final BindingSet bs = rez.next();
            results.add(bs);
        }
        
     // Verify the correct results were loaded into the PCJ table.
        final Set<BindingSet> expectedResults = new HashSet<>();

        MapBindingSet bs = new MapBindingSet();
        bs = new MapBindingSet();
        bs.addBinding("name", VF.createURI("urn:David"));
        expectedResults.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("name", VF.createURI("urn:Eve"));
        expectedResults.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("name", VF.createURI("urn:Frank"));
        expectedResults.add(bs);

        assertEquals(3, results.size());
        assertEquals(expectedResults, results);
    }
    
    private MongoConnectionDetails getConnectionDetails() {
        final java.util.Optional<char[]> password = conf.getMongoPassword() != null ?
                java.util.Optional.of(conf.getMongoPassword().toCharArray()) :
                    java.util.Optional.empty();

        return new MongoConnectionDetails(
                conf.getMongoHostname(),
                Integer.parseInt(conf.getMongoPort()),
                java.util.Optional.ofNullable(conf.getMongoUser()),
                password);
    }

    private Set<Statement> getStatements() throws Exception {
    	final Set<Statement> statements = new HashSet<>();
    	statements.add(VF.createStatement(VF.createURI("urn:Alice"), VF.createURI("urn:likes"), VF.createURI("urn:icecream")));
        statements.add(VF.createStatement(VF.createURI("urn:Bob"), VF.createURI("urn:likes"), VF.createURI("urn:icecream")));
        statements.add(VF.createStatement(VF.createURI("urn:Charlie"), VF.createURI("urn:likes"), VF.createURI("urn:icecream")));
        statements.add(VF.createStatement(VF.createURI("urn:David"), VF.createURI("urn:likes"), VF.createURI("urn:icecream")));
        statements.add(VF.createStatement(VF.createURI("urn:Eve"), VF.createURI("urn:likes"), VF.createURI("urn:icecream")));
        statements.add(VF.createStatement(VF.createURI("urn:Frank"), VF.createURI("urn:likes"), VF.createURI("urn:icecream")));
        statements.add(VF.createStatement(VF.createURI("urn:George"), VF.createURI("urn:likes"), VF.createURI("urn:icecream")));
        statements.add(VF.createStatement(VF.createURI("urn:Hillary"), VF.createURI("urn:likes"), VF.createURI("urn:icecream")));
        
        statements.add(VF.createStatement(VF.createURI("urn:Alice"), VF.createURI("urn:hasEyeColor"), VF.createURI("urn:blue")));
        statements.add(VF.createStatement(VF.createURI("urn:Bob"), VF.createURI("urn:hasEyeColor"), VF.createURI("urn:blue")));
        statements.add(VF.createStatement(VF.createURI("urn:Charlie"), VF.createURI("urn:hasEyeColor"), VF.createURI("urn:blue")));
        statements.add(VF.createStatement(VF.createURI("urn:David"), VF.createURI("urn:hasEyeColor"), VF.createURI("urn:blue")));
        statements.add(VF.createStatement(VF.createURI("urn:Eve"), VF.createURI("urn:hasEyeColor"), VF.createURI("urn:blue")));
        statements.add(VF.createStatement(VF.createURI("urn:Frank"), VF.createURI("urn:hasEyeColor"), VF.createURI("urn:blue")));
        statements.add(VF.createStatement(VF.createURI("urn:George"), VF.createURI("urn:hasEyeColor"), VF.createURI("urn:green")));
        statements.add(VF.createStatement(VF.createURI("urn:Hillary"), VF.createURI("urn:hasEyeColor"), VF.createURI("urn:brown")));
        
        statements.add(VF.createStatement(VF.createURI("urn:Alice"), VF.createURI("urn:hasHairColor"), VF.createURI("urn:blue")));
        statements.add(VF.createStatement(VF.createURI("urn:Bob"), VF.createURI("urn:hasHairColor"), VF.createURI("urn:blue")));
        statements.add(VF.createStatement(VF.createURI("urn:Charlie"), VF.createURI("urn:hasHairColor"), VF.createURI("urn:blue")));
        statements.add(VF.createStatement(VF.createURI("urn:David"), VF.createURI("urn:hasHairColor"), VF.createURI("urn:brown")));
        statements.add(VF.createStatement(VF.createURI("urn:Eve"), VF.createURI("urn:hasHairColor"), VF.createURI("urn:brown")));
        statements.add(VF.createStatement(VF.createURI("urn:Frank"), VF.createURI("urn:hasHairColor"), VF.createURI("urn:brown")));
        statements.add(VF.createStatement(VF.createURI("urn:George"), VF.createURI("urn:hasHairColor"), VF.createURI("urn:blonde")));
        statements.add(VF.createStatement(VF.createURI("urn:Hillary"), VF.createURI("urn:hasHairColor"), VF.createURI("urn:blonde")));
        return statements;
    }
}