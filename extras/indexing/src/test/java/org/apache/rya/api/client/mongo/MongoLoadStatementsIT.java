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

import java.util.ArrayList;
import java.util.List;

import org.apache.rya.api.client.Install;
import org.apache.rya.api.client.Install.DuplicateInstanceNameException;
import org.apache.rya.api.client.Install.InstallConfiguration;
import org.apache.rya.api.client.InstanceDoesNotExistException;
import org.apache.rya.api.client.RyaClient;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.mongodb.MongoTestBase;
import org.bson.Document;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

import com.mongodb.client.MongoCursor;

/**
 * Integration tests the methods of {@link MongoLoadStatements}.
 */
public class MongoLoadStatementsIT extends MongoTestBase {
    @Test(expected = InstanceDoesNotExistException.class)
    public void instanceDoesNotExist() throws Exception {
        org.apache.log4j.BasicConfigurator.configure();
        final RyaClient ryaClient = MongoRyaClientFactory.build(getConnectionDetails(), conf.getMongoClient());
        // Skip the install step to create error causing situation.
        ryaClient.getLoadStatements().loadStatements(getConnectionDetails().getHostname(), makeTestStatements());
    }

    /**
     * Pass a list of statements to our loadStatement class.
     *
     * @throws Exception
     */
    @Test
    public void loadTurtleFile() throws Exception {
        // Install an instance of Rya.
        final List<Statement> loadMe = installAndLoad();
        final List<Statement> stmtResults = new ArrayList<>();
        final MongoCursor<Document> triplesIterator = getRyaCollection().find().iterator();
        final ValueFactory vf = new ValueFactoryImpl();
        while (triplesIterator.hasNext()) {
            final Document triple = triplesIterator.next();
            stmtResults.add(vf.createStatement(vf.createURI(triple.getString("subject")), vf.createURI(triple.getString(
                    "predicate")), vf.createURI(triple.getString("object"))));
        }
        stmtResults.sort(((stmt1, stmt2) -> stmt1.getSubject().toString().compareTo(stmt2.getSubject().toString())));
        assertEquals("Expect all rows to be read.", 3, getRyaCollection().count());
        assertEquals("All rows in DB should match expected rows:", loadMe, stmtResults);
    }

    /**
     * @return some data to load
     */
    private List<Statement> makeTestStatements() {
        final List<Statement> loadMe = new ArrayList<>();
        final ValueFactory vf = new ValueFactoryImpl();

        loadMe.add(vf.createStatement(vf.createURI("http://example#alice"), vf.createURI("http://example#talksTo"), vf
                .createURI("http://example#bob")));
        loadMe.add(vf.createStatement(vf.createURI("http://example#bob"), vf.createURI("http://example#talksTo"), vf
                .createURI("http://example#charlie")));
        loadMe.add(vf.createStatement(vf.createURI("http://example#charlie"), vf.createURI("http://example#likes"), vf
                .createURI("http://example#icecream")));
        return loadMe;
    }

    private List<Statement> installAndLoad() throws DuplicateInstanceNameException, RyaClientException {
        // first install rya
        final InstallConfiguration installConfig = InstallConfiguration.builder()
                .setEnableTableHashPrefix(false)
                .setEnableEntityCentricIndex(false)
                .setEnableFreeTextIndex(false)
                .setEnableTemporalIndex(false)
                .setEnablePcjIndex(false)
                .setEnableGeoIndex(false)
                .build();
        final MongoConnectionDetails connectionDetails = getConnectionDetails();
        final RyaClient ryaClient = MongoRyaClientFactory.build(connectionDetails, conf.getMongoClient());
        final Install install = ryaClient.getInstall();
        install.install(conf.getMongoDBName(), installConfig);
        // next, load data
        final List<Statement> loadMe = makeTestStatements();
        ryaClient.getLoadStatements().loadStatements(
                conf.getMongoDBName(),
                loadMe);
        return loadMe;
    }
    /**
     * @return copy from conf to MongoConnectionDetails
     */
    private MongoConnectionDetails getConnectionDetails() {
        return new MongoConnectionDetails(
                conf.getMongoUser(),
                null,//conf.getMongoPassword().toCharArray(),
                conf.getMongoHostname(),
                Integer.parseInt(conf.getMongoPort()));
    }
}