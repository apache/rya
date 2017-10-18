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
package org.apache.rya.api.client.accumulo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.rya.accumulo.AccumuloITBase;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.api.client.Install.InstallConfiguration;
import org.apache.rya.api.client.RyaClient;
import org.apache.rya.api.instance.RyaDetails;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.sail.config.RyaSailFactory;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailConnection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

/**
 * Integration tests the methods of {@link AccumuloAddUser}.
 */
public class AccumuloAddUserIT extends AccumuloITBase {

    private final String ADMIN_USER = testInstance.createUniqueUser();

    @Before
    public void beforeClass() throws Exception {
        final SecurityOperations secOps = super.getConnector().securityOperations();

        // Create the user that will install the instance of Rya.
        secOps.createLocalUser(ADMIN_USER, new PasswordToken(ADMIN_USER));
        secOps.grantSystemPermission(ADMIN_USER, SystemPermission.CREATE_TABLE);
    }

    @After
    public void afterClass() throws Exception {
        final SecurityOperations secOps = super.getConnector().securityOperations();
        secOps.dropLocalUser(ADMIN_USER);
    }

    /**
     * Ensure that the user who installs the instance of Rya is reported as being a user who can access it.
     */
    @Test
    public void ryaDetailsIncludesOriginalUser() throws Exception {


        // Create a Rya Client for that user.
        final RyaClient userAClient = AccumuloRyaClientFactory.build(
                new AccumuloConnectionDetails(ADMIN_USER, ADMIN_USER.toCharArray(), getInstanceName(), getZookeepers()),
                super.getClusterInstance().getCluster().getConnector(ADMIN_USER, ADMIN_USER));

        // Install the instance of Rya.
        userAClient.getInstall().install(getRyaInstanceName(), InstallConfiguration.builder().build());

        // Ensure the Rya instance's details only contain the username of the user who installed the instance.
        final ImmutableList<String> expectedUsers = ImmutableList.<String>builder()
                .add(ADMIN_USER)
                .build();

        final RyaDetails details = userAClient.getGetInstanceDetails().getDetails(getRyaInstanceName()).get();
        assertEquals(expectedUsers, details.getUsers());
    }

    /**
     * Ensure that when a user is added to a Rya instance that its details are updated to include the new user.
     */
    @Test
    public void userAddedAlsoAddedToRyaDetails() throws Exception {
        final String user = testInstance.createUniqueUser();
        final SecurityOperations secOps = super.getConnector().securityOperations();

        final RyaClient userAClient = AccumuloRyaClientFactory.build(
                new AccumuloConnectionDetails(ADMIN_USER, ADMIN_USER.toCharArray(), getInstanceName(), getZookeepers()),
                super.getClusterInstance().getCluster().getConnector(ADMIN_USER, ADMIN_USER));

        // Create the user that will be added to the instance of Rya.
        secOps.createLocalUser(user, new PasswordToken(user));

        // Install the instance of Rya.
        userAClient.getInstall().install(getRyaInstanceName(), InstallConfiguration.builder().build());

        // Add the user.
        userAClient.getAddUser().get().addUser(getRyaInstanceName(), user);

        // Ensure the Rya instance's details have been updated to include the added user.
        final ImmutableList<String> expectedUsers = ImmutableList.<String>builder()
                .add(ADMIN_USER)
                .add(user)
                .build();

        final RyaDetails details = userAClient.getGetInstanceDetails().getDetails(getRyaInstanceName()).get();
        assertEquals(expectedUsers, details.getUsers());
    }

    /**
     * Ensure a user that has not been added to the Rya instance can not interact with it.
     */
    @Test
    public void userNotAddedCanNotInsert() throws Exception {
        final String user = testInstance.createUniqueUser();
        final SecurityOperations secOps = super.getConnector().securityOperations();

        final RyaClient userAClient = AccumuloRyaClientFactory.build(
                new AccumuloConnectionDetails(ADMIN_USER, ADMIN_USER.toCharArray(), getInstanceName(), getZookeepers()),
                super.getClusterInstance().getCluster().getConnector(ADMIN_USER, ADMIN_USER));

        // Install the instance of Rya.
        userAClient.getInstall().install(getRyaInstanceName(), InstallConfiguration.builder().build());

        // Create the user that will not be added to the instance of Rya, but will try to scan it.
        secOps.createLocalUser(user, new PasswordToken(user));

        //Try to add a statement the Rya instance with the unauthorized user. This should fail.
        boolean securityExceptionThrown = false;

        Sail sail = null;
        SailConnection sailConn = null;
        try {
            final AccumuloRdfConfiguration userCConf = makeRyaConfig(getRyaInstanceName(), user, user, getInstanceName(), getZookeepers());
            sail = RyaSailFactory.getInstance(userCConf);
            sailConn = sail.getConnection();

            final ValueFactory vf = sail.getValueFactory();
            sailConn.addStatement(vf.createIRI("urn:Alice"), vf.createIRI("urn:talksTo"), vf.createIRI("urn:Bob"));

        } catch(final RuntimeException e) {
            final Throwable cause = e.getCause();
            if(cause instanceof AccumuloSecurityException) {
                securityExceptionThrown = true;
            }
        } finally {
            if(sailConn != null) {
                sailConn.close();
            }
            if(sail != null) {
                sail.shutDown();
            }
        }

        assertTrue(securityExceptionThrown);
    }

    /**
     * Ensure a user that has been added to the Rya instance can interact with it.
     */
    @Test
    public void userAddedCanInsert() throws Exception {
        final String user = testInstance.createUniqueUser();
        final SecurityOperations secOps = super.getConnector().securityOperations();

        final RyaClient userAClient = AccumuloRyaClientFactory.build(
                new AccumuloConnectionDetails(ADMIN_USER, ADMIN_USER.toCharArray(), getInstanceName(), getZookeepers()),
                super.getClusterInstance().getCluster().getConnector(ADMIN_USER, ADMIN_USER));

        // Create the user that will not be added to the instance of Rya, but will try to scan it.
        secOps.createLocalUser(user, new PasswordToken(user));

        // Install the instance of Rya.
        userAClient.getInstall().install(getRyaInstanceName(), InstallConfiguration.builder().build());

        // Add the user.
        userAClient.getAddUser().get().addUser(getRyaInstanceName(), user);

        // Try to add a statement to the Rya instance. This should succeed.
        Sail sail = null;
        SailConnection sailConn = null;

        try {
            final AccumuloRdfConfiguration userDConf = makeRyaConfig(getRyaInstanceName(), user, user, getInstanceName(), getZookeepers());
            sail = RyaSailFactory.getInstance(userDConf);
            sailConn = sail.getConnection();

            final ValueFactory vf = sail.getValueFactory();
            sailConn.begin();
            sailConn.addStatement(vf.createIRI("urn:Alice"), vf.createIRI("urn:talksTo"), vf.createIRI("urn:Bob"));
            sailConn.close();

        } finally {
            if(sailConn != null) {
                sailConn.close();
            }
            if(sail != null) {
                sail.shutDown();
            }
        }
    }

    /**
     * Ensure nothing happens if you try to add a user that is already there.
     */
    @Test
    public void addUserTwice() throws Exception {
        final String user = testInstance.createUniqueUser();
        final SecurityOperations secOps = super.getConnector().securityOperations();

        final RyaClient userAClient = AccumuloRyaClientFactory.build(
                new AccumuloConnectionDetails(ADMIN_USER, ADMIN_USER.toCharArray(), getInstanceName(), getZookeepers()),
                super.getClusterInstance().getCluster().getConnector(ADMIN_USER, ADMIN_USER));

        // Create the user that will not be added to the instance of Rya, but will try to scan it.
        secOps.createLocalUser(user, new PasswordToken(user));

        // Install the instance of Rya.
        userAClient.getInstall().install(getRyaInstanceName(), InstallConfiguration.builder().build());

        // Add the user.
        userAClient.getAddUser().get().addUser(getRyaInstanceName(), user);
        userAClient.getAddUser().get().addUser(getRyaInstanceName(), user);

        // Ensure the Rya instance's details only contain the username of the user who installed the instance.
        final ImmutableList<String> expectedUsers = ImmutableList.<String>builder()
                .add(ADMIN_USER)
                .add(user)
                .build();

        final RyaDetails details = userAClient.getGetInstanceDetails().getDetails(getRyaInstanceName()).get();
        assertEquals(expectedUsers, details.getUsers());
    }

    private static AccumuloRdfConfiguration makeRyaConfig(
            final String ryaInstanceName,
            final String username,
            final String password,
            final String instanceName,
            final String zookeepers) {
        final AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
        conf.setTablePrefix(ryaInstanceName);
        // Accumulo connection information.
        conf.set(ConfigUtils.CLOUDBASE_USER, username);
        conf.set(ConfigUtils.CLOUDBASE_PASSWORD, password);
        conf.set(ConfigUtils.CLOUDBASE_INSTANCE, instanceName);
        conf.set(ConfigUtils.CLOUDBASE_ZOOKEEPERS, zookeepers);
        conf.set(ConfigUtils.CLOUDBASE_AUTHS, "");
        return conf;
    }
}