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
package org.apache.rya.shell;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;

import org.apache.rya.api.client.AddUser;
import org.apache.rya.api.client.CreatePCJ;
import org.apache.rya.api.client.CreatePCJ.ExportStrategy;
import org.apache.rya.api.client.CreatePeriodicPCJ;
import org.apache.rya.api.client.DeletePCJ;
import org.apache.rya.api.client.DeletePeriodicPCJ;
import org.apache.rya.api.client.GetInstanceDetails;
import org.apache.rya.api.client.Install;
import org.apache.rya.api.client.Install.DuplicateInstanceNameException;
import org.apache.rya.api.client.Install.InstallConfiguration;
import org.apache.rya.api.client.InstanceDoesNotExistException;
import org.apache.rya.api.client.ListInstances;
import org.apache.rya.api.client.RemoveUser;
import org.apache.rya.api.client.RyaClient;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.api.client.Uninstall;
import org.apache.rya.api.client.accumulo.AccumuloConnectionDetails;
import org.apache.rya.api.client.mongo.MongoConnectionDetails;
import org.apache.rya.api.instance.RyaDetails;
import org.apache.rya.api.instance.RyaDetails.EntityCentricIndexDetails;
import org.apache.rya.api.instance.RyaDetails.FreeTextIndexDetails;
import org.apache.rya.api.instance.RyaDetails.JoinSelectivityDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails.FluoDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails.PCJDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails.PCJDetails.PCJUpdateStrategy;
import org.apache.rya.api.instance.RyaDetails.ProspectorDetails;
import org.apache.rya.api.instance.RyaDetails.TemporalIndexDetails;
import org.apache.rya.shell.util.InstallPrompt;
import org.apache.rya.shell.util.SparqlPrompt;
import org.apache.rya.shell.util.UninstallPrompt;
import org.junit.Test;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Unit tests the methods of {@link RyaAdminCommands}.
 */
public class RyaAdminCommandsTest {

    @Test
    public void createPCJ() throws InstanceDoesNotExistException, RyaClientException, IOException {
        // Mock the object that performs the create operation.
        final String instanceName = "unitTest";
        final String sparql = "SELECT * WHERE { ?person <http://isA> ?noun }";
        final String pcjId = "123412342";
        final CreatePCJ mockCreatePCJ = mock(CreatePCJ.class);
        final Set<ExportStrategy> strategies = Sets.newHashSet(ExportStrategy.RYA);
        when(mockCreatePCJ.createPCJ( eq(instanceName), eq(sparql), eq(strategies) ) ).thenReturn( pcjId );

        final RyaClient mockCommands = mock(RyaClient.class);
        when(mockCommands.getCreatePCJ()).thenReturn( java.util.Optional.of(mockCreatePCJ) );

        final SharedShellState state = new SharedShellState();
        state.connectedToAccumulo(mock(AccumuloConnectionDetails.class), mockCommands);
        state.connectedToInstance(instanceName);

        final SparqlPrompt mockSparqlPrompt = mock(SparqlPrompt.class);
        when(mockSparqlPrompt.getSparql()).thenReturn(Optional.of(sparql));

        // Execute the command.
        final RyaAdminCommands commands = new RyaAdminCommands(state, mock(InstallPrompt.class), mockSparqlPrompt, mock(UninstallPrompt.class));
        final String message = commands.createPcj(true, false);

        // Verify the values that were provided to the command were passed through to CreatePCJ.
        verify(mockCreatePCJ).createPCJ(eq(instanceName), eq(sparql), eq(strategies));

        // Verify a message is returned that explains what was created.
        final String expected = "The PCJ has been created. Its ID is '123412342'.";
        assertEquals(expected, message);
    }

    @Test
    public void createPCJ_cancelledPrompt() throws InstanceDoesNotExistException, RyaClientException, IOException {
        // Mock the object that performs the create operation.
        final String instanceName = "unitTest";

        final RyaClient mockCommands = mock(RyaClient.class);

        final SharedShellState state = new SharedShellState();
        state.connectedToAccumulo(mock(AccumuloConnectionDetails.class), mockCommands);
        state.connectedToInstance(instanceName);

        final SparqlPrompt mockSparqlPrompt = mock(SparqlPrompt.class);
        when(mockSparqlPrompt.getSparql()).thenReturn(Optional.absent());

        // Execute the command.
        final RyaAdminCommands commands = new RyaAdminCommands(state, mock(InstallPrompt.class), mockSparqlPrompt, mock(UninstallPrompt.class));
        final String message = commands.createPcj(true, false);

        // Verify a message is returned that explains what was created.
        final String expected = "";
        assertEquals(expected, message);
    }

    @Test
    public void createPCJ_noExportStrategy() throws InstanceDoesNotExistException, RyaClientException, IOException {
        // Mock the object that performs the create operation.
        final String instanceName = "unitTest";

        final RyaClient mockCommands = mock(RyaClient.class);

        final SharedShellState state = new SharedShellState();
        state.connectedToAccumulo(mock(AccumuloConnectionDetails.class), mockCommands);
        state.connectedToInstance(instanceName);

        // Execute the command.
        final RyaAdminCommands commands = new RyaAdminCommands(state, mock(InstallPrompt.class), mock(SparqlPrompt.class), mock(UninstallPrompt.class));
        final String message = commands.createPcj(false, false);

        // Verify a message is returned that explains what was created.
        assertEquals("The user must specify at least one export strategy: (--exportToRya, --exportToKafka)", message);
    }

    @Test
    public void deletePCJ() throws InstanceDoesNotExistException, RyaClientException {
        // Mock the object that performs the delete operation.
        final DeletePCJ mockDeletePCJ = mock(DeletePCJ.class);

        final RyaClient mockCommands = mock(RyaClient.class);
        when(mockCommands.getDeletePCJ()).thenReturn( java.util.Optional.of(mockDeletePCJ) );

        final SharedShellState state = new SharedShellState();
        state.connectedToAccumulo(mock(AccumuloConnectionDetails.class), mockCommands);
        final String instanceName = "unitTests";
        state.connectedToInstance(instanceName);

        // Execute the command.
        final String pcjId = "123412342";

        final RyaAdminCommands commands = new RyaAdminCommands(state, mock(InstallPrompt.class), mock(SparqlPrompt.class), mock(UninstallPrompt.class));
        final String message = commands.deletePcj(pcjId);

        // Verify the values that were provided to the command were passed through to the DeletePCJ.
        verify(mockDeletePCJ).deletePCJ(eq(instanceName), eq(pcjId));

        // Verify a message is returned that explains what was deleted.
        final String expected = "The PCJ has been deleted.";
        assertEquals(expected, message);
    }

    @Test
    public void createPeriodicPCJ() throws InstanceDoesNotExistException, RyaClientException, IOException {
        // Mock the object that performs the create operation.
        final String instanceName = "unitTest";
        final String sparql = "SELECT * WHERE { ?person <http://isA> ?noun }";
        final String topic = "topic";
        final String brokers = "brokers";
        final String pcjId = "12341234";
        final CreatePeriodicPCJ mockCreatePCJ = mock(CreatePeriodicPCJ.class);
        when(mockCreatePCJ.createPeriodicPCJ( eq(instanceName), eq(sparql), eq(topic), eq(brokers) )).thenReturn( pcjId );

        final RyaClient mockCommands = mock(RyaClient.class);
        when(mockCommands.getCreatePeriodicPCJ()).thenReturn( java.util.Optional.of(mockCreatePCJ) );

        final SharedShellState state = new SharedShellState();
        state.connectedToAccumulo(mock(AccumuloConnectionDetails.class), mockCommands);
        state.connectedToInstance(instanceName);

        final SparqlPrompt mockSparqlPrompt = mock(SparqlPrompt.class);
        when(mockSparqlPrompt.getSparql()).thenReturn(Optional.of(sparql));

        // Execute the command.
        final RyaAdminCommands commands = new RyaAdminCommands(state, mock(InstallPrompt.class), mockSparqlPrompt, mock(UninstallPrompt.class));
        final String message = commands.createPeriodicPcj(topic, brokers);

        // Verify the values that were provided to the command were passed through to CreatePCJ.
        verify(mockCreatePCJ).createPeriodicPCJ(eq(instanceName), eq(sparql), eq(topic), eq(brokers));

        // Verify a message is returned that explains what was created.
        final String expected = "The Periodic PCJ has been created. Its ID is '12341234'.";
        assertEquals(expected, message);
    }

    @Test
    public void deletePeriodicPCJ() throws InstanceDoesNotExistException, RyaClientException {
        // Mock the object that performs the delete operation.
        final DeletePeriodicPCJ mockDeletePCJ = mock(DeletePeriodicPCJ.class);

        final RyaClient mockCommands = mock(RyaClient.class);
        when(mockCommands.getDeletePeriodicPCJ()).thenReturn( java.util.Optional.of(mockDeletePCJ) );

        final SharedShellState state = new SharedShellState();
        state.connectedToAccumulo(mock(AccumuloConnectionDetails.class), mockCommands);
        final String instanceName = "unitTests";
        state.connectedToInstance(instanceName);

        // Execute the command.
        final String pcjId = "123412342";
        final String topic = "topic";
        final String brokers = "brokers";

        final RyaAdminCommands commands = new RyaAdminCommands(state, mock(InstallPrompt.class), mock(SparqlPrompt.class), mock(UninstallPrompt.class));
        final String message = commands.deletePeriodicPcj(pcjId, topic, brokers);

        // Verify the values that were provided to the command were passed through to the DeletePCJ.
        verify(mockDeletePCJ).deletePeriodicPCJ(eq(instanceName), eq(pcjId), eq(topic), eq(brokers));

        // Verify a message is returned that explains what was deleted.
        final String expected = "The Periodic PCJ has been deleted.";
        assertEquals(expected, message);
    }


    @Test
    public void getInstanceDetails() throws InstanceDoesNotExistException, RyaClientException {
        // This test is failed if the default timezone was not EST, so now it's fixed at EST.
        // If you get assert mismatch of EST!=EDT, try the deprecated getTimeZone("EST") instead.
        TimeZone.setDefault(TimeZone.getTimeZone("America/New_York"));
        // Mock the object that performs the get operation.
        final GetInstanceDetails mockGetInstanceDetails = mock(GetInstanceDetails.class);
        final String instanceName = "test_instance";
        final RyaDetails details = RyaDetails.builder().setRyaInstanceName(instanceName)
                .setRyaVersion("1.2.3.4")
                .addUser("alice")
                .addUser("bob")
                .addUser("charlie")
                .setEntityCentricIndexDetails( new EntityCentricIndexDetails(true) )
              //RYA-215.setGeoIndexDetails( new GeoIndexDetails(true) )
                .setTemporalIndexDetails( new TemporalIndexDetails(true) )
                .setFreeTextDetails( new FreeTextIndexDetails(true) )
                .setPCJIndexDetails(
                        PCJIndexDetails.builder()
                            .setEnabled(true)
                            .setFluoDetails( new FluoDetails("test_instance_rya_pcj_updater") )
                            .addPCJDetails(
                                    PCJDetails.builder()
                                        .setId("pcj 1")
                                        .setUpdateStrategy(PCJUpdateStrategy.BATCH)
                                        .setLastUpdateTime( new Date(1252521351L) ))
                            .addPCJDetails(
                                    PCJDetails.builder()
                                        .setId("pcj 2")
                                        .setUpdateStrategy(PCJUpdateStrategy.INCREMENTAL)))
                .setProspectorDetails( new ProspectorDetails(Optional.of(new Date(12525211L))) )
                .setJoinSelectivityDetails( new JoinSelectivityDetails(Optional.of(new Date(125221351L))) )
                .build();

        when(mockGetInstanceDetails.getDetails(eq(instanceName))).thenReturn( Optional.of(details) );

        final RyaClient mockCommands = mock(RyaClient.class);
        when(mockCommands.getGetInstanceDetails()).thenReturn( mockGetInstanceDetails );

        final SharedShellState state = new SharedShellState();
        state.connectedToAccumulo(mock(AccumuloConnectionDetails.class), mockCommands);
        state.connectedToInstance(instanceName);

        // Execute the command.
        final RyaAdminCommands commands = new RyaAdminCommands(state, mock(InstallPrompt.class), mock(SparqlPrompt.class), mock(UninstallPrompt.class));
        final String message = commands.printInstanceDetails();

        // Verify the values that were provided to the command were passed through to the GetInstanceDetails.
        verify(mockGetInstanceDetails).getDetails(eq(instanceName));

        // Verify a message is returned that includes the details.
        final String expected =
                "General Metadata:\n" +
                "  Instance Name: test_instance\n" +
                "  RYA Version: 1.2.3.4\n" +
                "  Users: alice, bob, charlie\n" +
                "Secondary Indicies:\n" +
                "  Entity Centric Index:\n" +
                "    Enabled: true\n" +
              //RYA-215"  Geospatial Index:\n" +
            //RYA-215"    Enabled: true\n" +
                "  Free Text Index:\n" +
                "    Enabled: true\n" +
                "  Temporal Index:\n" +
                "    Enabled: true\n" +
                "  PCJ Index:\n" +
                "    Enabled: true\n" +
                "    Fluo App Name: test_instance_rya_pcj_updater\n" +
                "    PCJs:\n" +
                "      ID: pcj 1\n" +
                "        Update Strategy: BATCH\n" +
                "        Last Update Time: Thu Jan 15 06:55:21 EST 1970\n" +
                "      ID: pcj 2\n" +
                "        Update Strategy: INCREMENTAL\n" +
                "        Last Update Time: unavailable\n" +
                "Statistics:\n" +
                "  Prospector:\n" +
                "    Last Update Time: Wed Dec 31 22:28:45 EST 1969\n" +
                "  Join Selectivity:\n" +
                "    Last Updated Time: Fri Jan 02 05:47:01 EST 1970\n";
        assertEquals(expected, message);
    }

    @Test
    public void install() throws DuplicateInstanceNameException, RyaClientException, IOException {
        // Mock the object that performs the install operation.
        final Install mockInstall = mock(Install.class);

        final RyaClient mockCommands = mock(RyaClient.class);
        when(mockCommands.getInstall()).thenReturn( mockInstall );

        final SharedShellState state = new SharedShellState();
        state.connectedToAccumulo(mock(AccumuloConnectionDetails.class), mockCommands);

        // Execute the command.
        final String instanceName = "unitTests";
        final InstallConfiguration installConfig = InstallConfiguration.builder()
                .setEnableGeoIndex(true)
                .setEnablePcjIndex(true)
                .build();

        final InstallPrompt mockInstallPrompt = mock(InstallPrompt.class);
        when(mockInstallPrompt.promptInstanceName()).thenReturn( instanceName );
        when(mockInstallPrompt.promptInstallConfiguration(instanceName)).thenReturn( installConfig );
        when(mockInstallPrompt.promptVerified(eq(instanceName), eq(installConfig))).thenReturn(true);

        final RyaAdminCommands commands = new RyaAdminCommands(state, mockInstallPrompt, mock(SparqlPrompt.class), mock(UninstallPrompt.class));
        final String message = commands.install();

        // Verify the values that were provided to the command were passed through to the Install.
        verify(mockInstall).install(eq(instanceName), eq(installConfig));

        // Verify a message is returned that indicates the success of the operation.
        final String expected = "The Rya instance named 'unitTests' has been installed.";
        assertEquals(expected, message);
    }

    @Test
    public void installWithAccumuloParameters() throws DuplicateInstanceNameException, RyaClientException, IOException {
        // Mock the object that performs the install operation.
        final Install mockInstall = mock(Install.class);

        final RyaClient mockCommands = mock(RyaClient.class);
        when(mockCommands.getInstall()).thenReturn( mockInstall );

        final SharedShellState state = new SharedShellState();
        state.connectedToAccumulo(mock(AccumuloConnectionDetails.class), mockCommands);

        final String instanceName = "unitTests";
        final boolean enableTableHashPrefix = false;
        final boolean enableEntityCentricIndex = true;
        final boolean enableFreeTextIndex = false;
        final boolean enableTemporalIndex = false;
        final boolean enablePcjIndex = true;
        final String fluoPcjAppName = instanceName + "pcj_updater";

        // Execute the command.
        final InstallConfiguration installConfig = InstallConfiguration.builder()
                .setEnableTableHashPrefix(enableTableHashPrefix)
                .setEnableEntityCentricIndex(enableEntityCentricIndex)
                .setEnableFreeTextIndex(enableFreeTextIndex)
                .setEnableTemporalIndex(enableTemporalIndex)
                .setEnablePcjIndex(enablePcjIndex)
                .setFluoPcjAppName(fluoPcjAppName)
                .build();

        final InstallPrompt mockInstallPrompt = mock(InstallPrompt.class);
        when(mockInstallPrompt.promptInstanceName()).thenReturn( instanceName );
        when(mockInstallPrompt.promptInstallConfiguration(instanceName)).thenReturn( installConfig );
        when(mockInstallPrompt.promptVerified(eq(instanceName), eq(installConfig))).thenReturn(true);

        final RyaAdminCommands commands = new RyaAdminCommands(state, mockInstallPrompt, mock(SparqlPrompt.class), mock(UninstallPrompt.class));
        final String message = commands.installWithAccumuloParameters(instanceName, enableTableHashPrefix, enableEntityCentricIndex, enableFreeTextIndex, enableTemporalIndex, enablePcjIndex, fluoPcjAppName);

        // Verify the values that were provided to the command were passed through to the Install.
        verify(mockInstall).install(eq(instanceName), eq(installConfig));

        // Verify a message is returned that indicates the success of the operation.
        final String expected = "The Rya instance named 'unitTests' has been installed.";
        assertEquals(expected, message);
    }

    @Test
    public void installWithAccumuloParameters_userAbort() throws DuplicateInstanceNameException, RyaClientException, IOException {
        // Mock the object that performs the install operation.
        final Install mockInstall = mock(Install.class);

        final RyaClient mockCommands = mock(RyaClient.class);
        when(mockCommands.getInstall()).thenReturn( mockInstall );

        final SharedShellState state = new SharedShellState();
        state.connectedToAccumulo(mock(AccumuloConnectionDetails.class), mockCommands);

        final String instanceName = "unitTests";
        final boolean enableTableHashPrefix = false;
        final boolean enableEntityCentricIndex = true;
        final boolean enableFreeTextIndex = false;
        final boolean enableTemporalIndex = false;
        final boolean enablePcjIndex = true;
        final String fluoPcjAppName = instanceName + "pcj_updater";

        // Execute the command.
        final InstallConfiguration installConfig = InstallConfiguration.builder()
                .setEnableTableHashPrefix(enableTableHashPrefix)
                .setEnableEntityCentricIndex(enableEntityCentricIndex)
                .setEnableFreeTextIndex(enableFreeTextIndex)
                .setEnableTemporalIndex(enableTemporalIndex)
                .setEnablePcjIndex(enablePcjIndex)
                .setFluoPcjAppName(fluoPcjAppName)
                .build();

        final InstallPrompt mockInstallPrompt = mock(InstallPrompt.class);
        when(mockInstallPrompt.promptInstanceName()).thenReturn( instanceName );
        when(mockInstallPrompt.promptInstallConfiguration(instanceName)).thenReturn( installConfig );
        when(mockInstallPrompt.promptVerified(eq(instanceName), eq(installConfig))).thenReturn(false);

        final RyaAdminCommands commands = new RyaAdminCommands(state, mockInstallPrompt, mock(SparqlPrompt.class), mock(UninstallPrompt.class));
        final String message = commands.installWithAccumuloParameters(instanceName, enableTableHashPrefix, enableEntityCentricIndex, enableFreeTextIndex, enableTemporalIndex, enablePcjIndex, fluoPcjAppName);

        // Verify a message is returned that indicates the success of the operation.
        final String expected = "Skipping Installation.";
        assertEquals(expected, message);
    }

    @Test
    public void installWithMongoParameters() throws DuplicateInstanceNameException, RyaClientException, IOException {
        // Mock the object that performs the install operation.
        final Install mockInstall = mock(Install.class);

        final RyaClient mockCommands = mock(RyaClient.class);
        when(mockCommands.getInstall()).thenReturn( mockInstall );

        final SharedShellState state = new SharedShellState();
        state.connectedToMongo(mock(MongoConnectionDetails.class), mockCommands);

        final String instanceName = "unitTests";
        final boolean enableFreeTextIndex = false;
        final boolean enableTemporalIndex = false;

        // Execute the command.
        final InstallConfiguration installConfig = InstallConfiguration.builder()
                .setEnableFreeTextIndex(enableFreeTextIndex)
                .setEnableTemporalIndex(enableTemporalIndex)
                .build();

        final InstallPrompt mockInstallPrompt = mock(InstallPrompt.class);
        when(mockInstallPrompt.promptInstanceName()).thenReturn( instanceName );
        when(mockInstallPrompt.promptInstallConfiguration(instanceName)).thenReturn( installConfig );
        when(mockInstallPrompt.promptVerified(eq(instanceName), eq(installConfig))).thenReturn(true);

        final RyaAdminCommands commands = new RyaAdminCommands(state, mockInstallPrompt, mock(SparqlPrompt.class), mock(UninstallPrompt.class));
        final String message = commands.installWithMongoParameters(instanceName, enableFreeTextIndex, enableTemporalIndex);

        // Verify the values that were provided to the command were passed through to the Install.
        verify(mockInstall).install(eq(instanceName), eq(installConfig));

        // Verify a message is returned that indicates the success of the operation.
        final String expected = "The Rya instance named 'unitTests' has been installed.";
        assertEquals(expected, message);
    }

    @Test
    public void listInstances() throws RyaClientException, IOException {
        // Mock the object that performs the list operation.
        final ListInstances mockListInstances = mock(ListInstances.class);
        final List<String> instanceNames = Lists.newArrayList("a", "b", "c", "d");
        when(mockListInstances.listInstances()).thenReturn(instanceNames);

        final RyaClient mockCommands = mock(RyaClient.class);
        when(mockCommands.getListInstances()).thenReturn( mockListInstances );

        final SharedShellState state = new SharedShellState();
        state.connectedToAccumulo(mock(AccumuloConnectionDetails.class), mockCommands);
        state.connectedToInstance("b");

        // Execute the command.
        final RyaAdminCommands commands = new RyaAdminCommands(state, mock(InstallPrompt.class), mock(SparqlPrompt.class), mock(UninstallPrompt.class));
        final String message = commands.listInstances();

        // Verify a message is returned that lists the the instances.
        final String expected =
                "Rya instance names:\n" +
                "   a\n" +
                " * b\n" +
                "   c\n" +
                "   d\n";
        assertEquals(expected, message);
    }

    @Test
    public void addUser() throws Exception {
        // Mock the object that performs the Add User command.
        final AddUser mockAddUser = mock(AddUser.class);

        final RyaClient mockClient = mock(RyaClient.class);
        when(mockClient.getAddUser()).thenReturn( java.util.Optional.of(mockAddUser) );

        final SharedShellState state = new SharedShellState();
        state.connectedToAccumulo(mock(AccumuloConnectionDetails.class), mockClient);
        state.connectedToInstance("test_instance");

        // Execute the command.
        final RyaAdminCommands commands = new RyaAdminCommands(state, mock(InstallPrompt.class), mock(SparqlPrompt.class), mock(UninstallPrompt.class));
        commands.addUser("alice");

        // Verify the add request was forwarded to the client.
        verify(mockAddUser).addUser(eq("test_instance"), eq("alice"));
    }

    @Test
    public void removeUser() throws Exception {
        // Mock the object that performs the Add User command.
        final RemoveUser mockRemoveUser = mock(RemoveUser.class);

        final RyaClient mockClient = mock(RyaClient.class);
        when(mockClient.getRemoveUser()).thenReturn( java.util.Optional.of(mockRemoveUser) );

        final SharedShellState state = new SharedShellState();
        state.connectedToAccumulo(mock(AccumuloConnectionDetails.class), mockClient);
        state.connectedToInstance("test_instance");

        // Execute the command.
        final RyaAdminCommands commands = new RyaAdminCommands(state, mock(InstallPrompt.class), mock(SparqlPrompt.class), mock(UninstallPrompt.class));
        commands.removeUser("alice");

        // Verify the add request was forwarded to the client.
        verify(mockRemoveUser).removeUser(eq("test_instance"), eq("alice"));
    }

    @Test
    public void uninstall_yes() throws Exception {
        // Mock the object that performs the Uninstall command.
        final Uninstall mockUninstall = mock(Uninstall.class);

        // Mock a prompt that says the user does want to uninstall it.
        final UninstallPrompt uninstallPrompt = mock(UninstallPrompt.class);
        when(uninstallPrompt.promptAreYouSure( eq("test_instance") )).thenReturn(true);

        final RyaClient mockClient = mock(RyaClient.class);
        when(mockClient.getUninstall()).thenReturn( mockUninstall );

        final SharedShellState state = new SharedShellState();
        state.connectedToAccumulo(mock(AccumuloConnectionDetails.class), mockClient);
        state.connectedToInstance("test_instance");

        // Execute the command.
        final RyaAdminCommands commands = new RyaAdminCommands(state, mock(InstallPrompt.class), mock(SparqlPrompt.class), uninstallPrompt);
        commands.uninstall();

        // Verify the request was forwarded to the client.
        verify(mockUninstall).uninstall(eq("test_instance"));
    }

    @Test
    public void uninstall_no() throws Exception {
        // Mock the object that performs the Uninstall command.
        final Uninstall mockUninstall = mock(Uninstall.class);

        // Mock a prompt that says the user does want to uninstall it.
        final UninstallPrompt uninstallPrompt = mock(UninstallPrompt.class);
        when(uninstallPrompt.promptAreYouSure( eq("test_instance") )).thenReturn(false);

        final RyaClient mockClient = mock(RyaClient.class);
        when(mockClient.getUninstall()).thenReturn( mockUninstall );

        final SharedShellState state = new SharedShellState();
        state.connectedToAccumulo(mock(AccumuloConnectionDetails.class), mockClient);
        state.connectedToInstance("test_instance");

        // Execute the command.
        final RyaAdminCommands commands = new RyaAdminCommands(state, mock(InstallPrompt.class), mock(SparqlPrompt.class), uninstallPrompt);
        commands.uninstall();

        // Verify the request was forwarded to the client.
        verify(mockUninstall, never()).uninstall(eq("test_instance"));
    }
}
