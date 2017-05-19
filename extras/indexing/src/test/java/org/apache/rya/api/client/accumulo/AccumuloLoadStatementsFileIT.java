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
package org.apache.rya.api.client.accumulo;

import static org.junit.Assert.assertEquals;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.rya.accumulo.AccumuloITBase;
import org.apache.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;
import org.apache.rya.api.client.Install;
import org.apache.rya.api.client.Install.InstallConfiguration;
import org.apache.rya.api.client.InstanceDoesNotExistException;
import org.apache.rya.api.client.RyaClient;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.resolver.RyaToRdfConversions;
import org.apache.rya.api.resolver.triple.TripleRow;
import org.apache.rya.api.resolver.triple.impl.WholeRowTripleResolver;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.RDFFormat;

/**
 * Integration tests the methods of {@link AccumuloLoadStatementsFile}.
 */
public class AccumuloLoadStatementsFileIT extends AccumuloITBase {

    @Test(expected = InstanceDoesNotExistException.class)
    public void instanceDoesNotExist() throws Exception {
        final AccumuloConnectionDetails connectionDetails = new AccumuloConnectionDetails(
                getUsername(),
                getPassword().toCharArray(),
                getInstanceName(),
                getZookeepers());

        final RyaClient ryaClient = AccumuloRyaClientFactory.build(connectionDetails, getConnector());
        ryaClient.getLoadStatementsFile().loadStatements(getRyaInstanceName(), Paths.get("src/test/resources/example.ttl"), RDFFormat.TURTLE);
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
                .setFluoPcjAppName("fluo_app_name")
                .build();

        final AccumuloConnectionDetails connectionDetails = new AccumuloConnectionDetails(
                getUsername(),
                getPassword().toCharArray(),
                getInstanceName(),
                getZookeepers());

        final RyaClient ryaClient = AccumuloRyaClientFactory.build(connectionDetails, getConnector());
        final Install install = ryaClient.getInstall();
        install.install(getRyaInstanceName(), installConfig);

        // Load the test statement file.
        ryaClient.getLoadStatementsFile().loadStatements(getRyaInstanceName(), Paths.get("src/test/resources/example.ttl"), RDFFormat.TURTLE);

        // Verify that the statements were loaded.
        final ValueFactory vf = new ValueFactoryImpl();

        final List<Statement> expected = new ArrayList<>();
        expected.add( vf.createStatement(vf.createURI("http://example#alice"), vf.createURI("http://example#talksTo"), vf.createURI("http://example#bob")) );
        expected.add( vf.createStatement(vf.createURI("http://example#bob"), vf.createURI("http://example#talksTo"), vf.createURI("http://example#charlie")) );
        expected.add( vf.createStatement(vf.createURI("http://example#charlie"), vf.createURI("http://example#likes"), vf.createURI("http://example#icecream")) );

        final List<Statement> statements = new ArrayList<>();

        final WholeRowTripleResolver tripleResolver = new WholeRowTripleResolver();
        final Scanner scanner = getConnector().createScanner(getRyaInstanceName() + "spo", new Authorizations());
        final Iterator<Entry<Key, Value>> it = scanner.iterator();
        while(it.hasNext()) {
            final Entry<Key, Value> next = it.next();

            final Key key = next.getKey();
            final byte[] row = key.getRow().getBytes();
            final byte[] columnFamily = key.getColumnFamily().getBytes();
            final byte[] columnQualifier = key.getColumnQualifier().getBytes();
            final TripleRow tripleRow = new TripleRow(row, columnFamily, columnQualifier);

            final RyaStatement ryaStatement = tripleResolver.deserialize(TABLE_LAYOUT.SPO, tripleRow);
            final Statement statement = RyaToRdfConversions.convertStatement(ryaStatement);

            // Filter out the rya version statement if it is present.
            if(!statement.getPredicate().equals( vf.createURI("urn:mvm.rya/2012/05#version") )) {
                statements.add( statement );
            }
        }

        assertEquals(expected, statements);
    }
}