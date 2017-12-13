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
package org.apache.rya.api.client.mongo;

import static org.junit.Assert.assertEquals;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.rya.api.client.Install;
import org.apache.rya.api.client.Install.InstallConfiguration;
import org.apache.rya.api.client.InstanceDoesNotExistException;
import org.apache.rya.api.client.RyaClient;
import org.apache.rya.mongodb.MongoTestBase;
import org.bson.Document;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.RDFFormat;

import com.mongodb.client.MongoCursor;

/**
 * Integration tests the methods of {@link MongoLoadStatementsFile}.
 */
public class MongoLoadStatementsFileIT extends MongoTestBase {

    @Test(expected = InstanceDoesNotExistException.class)
    public void instanceDoesNotExist() throws Exception {

        final RyaClient ryaClient = MongoRyaClientFactory.build(getConnectionDetails(), conf.getMongoClient());
        ryaClient.getLoadStatementsFile().loadStatements(getConnectionDetails().getInstance(), Paths.get("src/test/resources/example.ttl"), RDFFormat.TURTLE);
    }

    @Test
    public void loadTurtleFile() throws Exception {
        // Install an instance of Rya.
        final InstallConfiguration installConfig = InstallConfiguration.builder()
                .setEnableTableHashPrefix(false)
                .setEnableEntityCentricIndex(false)
                .setEnableFreeTextIndex(false)
                .setEnableTemporalIndex(false)
                .setEnablePcjIndex(false)
                .setEnableGeoIndex(false)
                .build();
        MongoConnectionDetails connectionDetails = getConnectionDetails();
        final RyaClient ryaClient = MongoRyaClientFactory.build(connectionDetails, conf.getMongoClient());
        final Install install = ryaClient.getInstall();
        install.install(connectionDetails.getInstance(), installConfig);

        // Load the test statement file.
        ryaClient.getLoadStatementsFile().loadStatements( //
                        connectionDetails.getInstance(), //
                        Paths.get("src/test/resources/example.ttl"), //
                        RDFFormat.TURTLE);

        // Verify that the statements were loaded.
        final ValueFactory vf = new ValueFactoryImpl();

        final List<Statement> expected = new ArrayList<>();
        expected.add( vf.createStatement(vf.createURI("http://example#alice"), vf.createURI("http://example#talksTo"), vf.createURI("http://example#bob")) );
        expected.add( vf.createStatement(vf.createURI("http://example#bob"), vf.createURI("http://example#talksTo"), vf.createURI("http://example#charlie")) );
        expected.add( vf.createStatement(vf.createURI("http://example#charlie"), vf.createURI("http://example#likes"), vf.createURI("http://example#icecream")) );

        final List<Statement> statements = new ArrayList<>();
        MongoCursor<Document> x = getRyaCollection().find().iterator();
        System.out.println("getRyaCollection().count()=" + getRyaCollection().count());
        while (x.hasNext()) {
            Document y = x.next();
            System.out.println("getRyaCollection()=" + y);
        }
        assertEquals("Expect all rows to be read.", 3, getRyaCollection().count());
        // final WholeRowTripleResolver tripleResolver = new WholeRowTripleResolver();
        // final Scanner scanner = getConnector().createScanner(getRyaInstanceName() + "spo", new Authorizations());
        // final Iterator<Entry<Key, Value>> it = scanner.iterator();
        // while(it.hasNext()) {
        // final Entry<Key, Value> next = it.next();
        //
        // final Key key = next.getKey();
        // final byte[] row = key.getRow().getBytes();
        // final byte[] columnFamily = key.getColumnFamily().getBytes();
        // final byte[] columnQualifier = key.getColumnQualifier().getBytes();
        // final TripleRow tripleRow = new TripleRow(row, columnFamily, columnQualifier);
        //
        // final RyaStatement ryaStatement = tripleResolver.deserialize(TABLE_LAYOUT.SPO, tripleRow);
        // final Statement statement = RyaToRdfConversions.convertStatement(ryaStatement);
        //
        // // Filter out the rya version statement if it is present.
        // if(!isRyaMetadataStatement(vf, statement)) {
        // statements.add( statement );
        // }
        // }
        //
        // assertEquals(expected, statements);
    }

    private boolean isRyaMetadataStatement(final ValueFactory vf, final Statement statement) {
        return statement.getPredicate().equals( vf.createURI("urn:org.apache.rya/2012/05#version") ) ||
                statement.getPredicate().equals( vf.createURI("urn:org.apache.rya/2012/05#rts") );
    }
    /**
     * @return copy from conf to MongoConnectionDetails
     */
    private MongoConnectionDetails getConnectionDetails() {
        final MongoConnectionDetails connectionDetails = new MongoConnectionDetails(//
                        conf.getMongoUser(), //
                        conf.getMongoPassword().toCharArray(), //
                        conf.getMongoDBName(), // aka instance
                        conf.getMongoInstance(), // aka hostname
                        conf.getCollectionName()
        );
        return connectionDetails;
    }
}
