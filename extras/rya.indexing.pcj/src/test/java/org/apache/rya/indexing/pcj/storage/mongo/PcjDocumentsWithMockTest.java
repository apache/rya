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


package org.apache.rya.indexing.pcj.storage.mongo;

import static org.junit.Assert.assertEquals;

import java.math.BigInteger;
import java.util.HashSet;
import java.util.Set;

import org.apache.rya.indexing.pcj.storage.PcjMetadata;
import org.apache.rya.indexing.pcj.storage.accumulo.PcjTableNameFactory;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;
import org.apache.rya.mongodb.MongoDBRyaDAO;
import org.apache.rya.mongodb.MongoITBase;
import org.apache.rya.mongodb.StatefulMongoDBRdfConfiguration;
import org.apache.rya.rdftriplestore.RdfCloudTripleStore;
import org.apache.rya.rdftriplestore.RyaSailRepository;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.junit.Test;

public class PcjDocumentsWithMockTest extends MongoITBase {
    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    @Override
    protected void updateConfiguration(final MongoDBRdfConfiguration conf) {
        conf.setDisplayQueryPlan(false);
    }

    @Test
    public void populatePcj() throws Exception {
        final RdfCloudTripleStore ryaStore = new RdfCloudTripleStore();
        final MongoDBRyaDAO dao = new MongoDBRyaDAO();
        dao.setConf(new StatefulMongoDBRdfConfiguration(conf, getMongoClient()));
        dao.init();
        ryaStore.setRyaDAO(dao);
        ryaStore.initialize();
        final SailRepositoryConnection ryaConn = new RyaSailRepository(ryaStore).getConnection();

        try {
            // Load some Triples into Rya.
            final Set<Statement> triples = new HashSet<>();
            triples.add( VF.createStatement(VF.createIRI("http://Alice"), VF.createIRI("http://hasAge"), VF.createLiteral(BigInteger.valueOf(14))) );
            triples.add( VF.createStatement(VF.createIRI("http://Alice"), VF.createIRI("http://playsSport"), VF.createLiteral("Soccer")) );
            triples.add( VF.createStatement(VF.createIRI("http://Bob"), VF.createIRI("http://hasAge"), VF.createLiteral(BigInteger.valueOf(16))) );
            triples.add( VF.createStatement(VF.createIRI("http://Bob"), VF.createIRI("http://playsSport"), VF.createLiteral("Soccer")) );
            triples.add( VF.createStatement(VF.createIRI("http://Charlie"), VF.createIRI("http://hasAge"), VF.createLiteral(BigInteger.valueOf(12))) );
            triples.add( VF.createStatement(VF.createIRI("http://Charlie"), VF.createIRI("http://playsSport"), VF.createLiteral("Soccer")) );
            triples.add( VF.createStatement(VF.createIRI("http://Eve"), VF.createIRI("http://hasAge"), VF.createLiteral(BigInteger.valueOf(43))) );
            triples.add( VF.createStatement(VF.createIRI("http://Eve"), VF.createIRI("http://playsSport"), VF.createLiteral("Soccer")) );

            for(final Statement triple : triples) {
                ryaConn.add(triple);
            }

            // Create a PCJ table that will include those triples in its results.
            final String sparql =
                    "SELECT ?name ?age " +
                            "{" +
                            "?name <http://hasAge> ?age." +
                            "?name <http://playsSport> \"Soccer\" " +
                            "}";

            final String pcjTableName = new PcjTableNameFactory().makeTableName(conf.getRyaInstanceName(), "testPcj");
            final MongoPcjDocuments pcjs = new MongoPcjDocuments(getMongoClient(), conf.getRyaInstanceName());
            pcjs.createAndPopulatePcj(ryaConn, pcjTableName, sparql);

            // Make sure the cardinality was updated.
            final PcjMetadata metadata = pcjs.getPcjMetadata(pcjTableName);
            assertEquals(4, metadata.getCardinality());
        } finally {
            ryaConn.close();
            ryaStore.shutDown();
        }
    }

}
