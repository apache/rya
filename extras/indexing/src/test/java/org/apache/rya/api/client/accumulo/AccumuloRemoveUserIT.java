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
import org.junit.Test;
import org.openrdf.model.ValueFactory;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailConnection;

import com.google.common.collect.ImmutableList;

/**
 * Integration tests the methods of {@link AccumuloRemoveUser}.
 */
public class AccumuloRemoveUserIT extends AccumuloITBase {

    /**
     * Ensure that when a user is removed from a Rya instance that its details are updated to no longer include the user.
     */
    @Test
    public void removedUserNotInDetails() throws Exception {
        final String adminUser = testInstance.createUniqueUser();
        final String user = testInstance.createUniqueUser();
        final SecurityOperations secOps = super.getConnector().securityOperations();

        // Create the user that will install the instance of Rya.
        secOps.createLocalUser(adminUser, new PasswordToken(adminUser));
        secOps.grantSystemPermission(adminUser, SystemPermission.CREATE_TABLE);


        final RyaClient userAClient = AccumuloRyaClientFactory.build(
                new AccumuloConnectionDetails(adminUser, adminUser.toCharArray(), getInstanceName(), getZookeepers()),
                super.getClusterInstance().getCluster().getConnector(adminUser, adminUser));

        // Create the user that will be added to the instance of Rya.
        secOps.createLocalUser(user, new PasswordToken(user));

        final RyaClient userBClient = AccumuloRyaClientFactory.build(
                new AccumuloConnectionDetails(user, user.toCharArray(), getInstanceName(), getZookeepers()),
                super.getClusterInstance().getCluster().getConnector(user, user));

        // Install the instance of Rya.
        userAClient.getInstall().install(getRyaInstanceName(), InstallConfiguration.builder().build());

        // Add userB.
        userAClient.getAddUser().get().addUser(getRyaInstanceName(), user);

        // Remove userA.
        userBClient.getRemoveUser().get().removeUser(getRyaInstanceName(), adminUser);

        // Ensure the Rya instance's details have been updated to include the added user.
        final ImmutableList<String> expectedUsers = ImmutableList.<String>builder()
                .add(user)
                .build();

        final RyaDetails details = userBClient.getGetInstanceDetails().getDetails(getRyaInstanceName()).get();
        assertEquals(expectedUsers, details.getUsers());
    }

    /**
     * Ensure a user that has been removed from the Rya instance can not interact with it.
     */
    @Test
    public void removedUserCanNotInsert() throws Exception {
        final String adminUser = testInstance.createUniqueUser();
        final String user = testInstance.createUniqueUser();
        final SecurityOperations secOps = super.getConnector().securityOperations();

        // Create the user that will install the instance of Rya.
        secOps.createLocalUser(adminUser, new PasswordToken(adminUser));
        secOps.grantSystemPermission(adminUser, SystemPermission.CREATE_TABLE);

        final RyaClient userAClient = AccumuloRyaClientFactory.build(
                new AccumuloConnectionDetails(adminUser, adminUser.toCharArray(), getInstanceName(), getZookeepers()),
                super.getClusterInstance().getCluster().getConnector(adminUser, adminUser));

        // Create the user that will be added to the instance of Rya.
        secOps.createLocalUser(user, new PasswordToken(user));

        final RyaClient userCClient = AccumuloRyaClientFactory.build(
                new AccumuloConnectionDetails(user, user.toCharArray(), getInstanceName(), getZookeepers()),
                super.getClusterInstance().getCluster().getConnector(user, user));

        // Install the instance of Rya.
        userAClient.getInstall().install(getRyaInstanceName(), InstallConfiguration.builder().build());

        // Add userC.
        userAClient.getAddUser().get().addUser(getRyaInstanceName(), user);

        // Remove userA.
        userCClient.getRemoveUser().get().removeUser(getRyaInstanceName(), adminUser);

        // Show that userA can not insert anything.
        boolean securityExceptionThrown = false;

        Sail sail = null;
        SailConnection sailConn = null;
        try {
            final AccumuloRdfConfiguration userAConf = makeRyaConfig(getRyaInstanceName(), adminUser, adminUser, getInstanceName(), getZookeepers());
            sail = RyaSailFactory.getInstance(userAConf);
            sailConn = sail.getConnection();

            final ValueFactory vf = sail.getValueFactory();
            sailConn.addStatement(vf.createURI("urn:Alice"), vf.createURI("urn:talksTo"), vf.createURI("urn:Bob"));

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