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

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.rya.api.client.CreatePCJ.ExportStrategy;
import org.apache.rya.api.client.GetInstanceDetails;
import org.apache.rya.api.client.Install.DuplicateInstanceNameException;
import org.apache.rya.api.client.Install.InstallConfiguration;
import org.apache.rya.api.client.InstanceDoesNotExistException;
import org.apache.rya.api.client.RyaClient;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.api.instance.RyaDetails;
import org.apache.rya.shell.SharedShellState.ConnectionState;
import org.apache.rya.shell.SharedShellState.ShellState;
import org.apache.rya.shell.SharedShellState.StorageType;
import org.apache.rya.shell.util.InstallPrompt;
import org.apache.rya.shell.util.InstanceNamesFormatter;
import org.apache.rya.shell.util.RyaDetailsFormatter;
import org.apache.rya.shell.util.SparqlPrompt;
import org.apache.rya.shell.util.UninstallPrompt;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import com.google.common.base.Optional;

/**
 * Rya Shell commands that have to do with administrative tasks.
 */
@Component
public class RyaAdminCommands implements CommandMarker {

    public static final String CREATE_PCJ_CMD = "create-pcj";
    public static final String CREATE_PERIODIC_PCJ_CMD = "create-periodic-pcj";
    public static final String DELETE_PCJ_CMD = "delete-pcj";
    public static final String DELETE_PERIODIC_PCJ_CMD = "delete-periodic-pcj";
    public static final String LIST_INCREMENTAL_QUERIES = "list-incremental-queries";
    public static final String PRINT_INSTANCE_DETAILS_CMD = "print-instance-details";
    public static final String INSTALL_CMD = "install";
    public static final String INSTALL_ACCUMULO_PARAMETERS_CMD = "install-with-accumulo-parameters";
    public static final String INSTALL_MONGO_PARAMETERS_CMD = "install-with-mongo-parameters";
    public static final String LIST_INSTANCES_CMD = "list-instances";
    public static final String UNINSTALL_CMD = "uninstall";
    public static final String ADD_USER_CMD = "add-user";
    public static final String REMOVE_USER_CMD = "remove-user";

    private final SharedShellState state;
    private final InstallPrompt installPrompt;
    private final SparqlPrompt sparqlPrompt;
    private final UninstallPrompt uninstallPrompt;

    /**
     * Constructs an instance of {@link RyaAdminCommands}.
     *
     * @param state - Holds shared state between all of the command classes. (not null)
     * @param installPrompt - Prompts a user for installation details. (not null)
     * @param sparqlPrompt - Prompts a user for a SPARQL query. (not null)
     * @param uninstallPrompt - Prompts a user when uninstalling. (not null)
     */
    @Autowired
    public RyaAdminCommands(
            final SharedShellState state,
            final InstallPrompt installPrompt,
            final SparqlPrompt sparqlPrompt,
            final UninstallPrompt uninstallPrompt) {
        this.state = requireNonNull( state );
        this.installPrompt = requireNonNull(installPrompt);
        this.sparqlPrompt = requireNonNull(sparqlPrompt);
        this.uninstallPrompt = requireNonNull(uninstallPrompt);
    }

    /**
     * Enables commands that only become available once the Shell has been connected to a Rya Storage.
     */
    @CliAvailabilityIndicator({
        LIST_INSTANCES_CMD,
        INSTALL_CMD})
    public boolean areStorageCommandsAvailable() {
        switch(state.getShellState().getConnectionState()) {
            case CONNECTED_TO_STORAGE:
            case CONNECTED_TO_INSTANCE:
                return true;
            default:
                return false;
        }
    }

    /**
     * Enables commands that only become available once the Shell has been connected to an Accumulo Rya Storage.
     */
    @CliAvailabilityIndicator({
        INSTALL_ACCUMULO_PARAMETERS_CMD})
    public boolean areAccumuloStorageCommandsAvailable() {
        return isConnectedToStorageType(StorageType.ACCUMULO);
    }

    /**
     * Enables commands that only become available once the Shell has been connected to an MongoDB Rya Storage.
     */
    @CliAvailabilityIndicator({
        INSTALL_MONGO_PARAMETERS_CMD})
    public boolean areMongoStorageCommandsAvailable() {
        return isConnectedToStorageType(StorageType.MONGO);
    }

    private boolean isConnectedToStorageType(final StorageType type) {
        // Enabled if we are connected to the specified storage type.
        final Optional<StorageType> storageType = state.getShellState().getStorageType();
        if(storageType.isPresent()) {
            return areStorageCommandsAvailable() && storageType.get() == type;
        }

        // Otherwise disabled.
        return false;
    }

    /**
     * Enables commands that are always available once the Shell is connected to a Rya Instance.
     */
    @CliAvailabilityIndicator({
        PRINT_INSTANCE_DETAILS_CMD,
        UNINSTALL_CMD})
    public boolean areInstanceCommandsAvailable() {
        return state.getShellState().getConnectionState() == ConnectionState.CONNECTED_TO_INSTANCE;
    }

    /**
     * Enables commands that are available when connected to a Rya Instance that supports user management.
     */
    @CliAvailabilityIndicator({
        ADD_USER_CMD,
        REMOVE_USER_CMD})
    public boolean areUserCommandAvailable() {
        return areInstanceCommandsAvailable() && state.getShellState().getStorageType().get() == StorageType.ACCUMULO;
    }

    /**
     * Enables commands that are available when the Shell is connected to a Rya Instance that supports PCJ Indexing.
     */
    @CliAvailabilityIndicator({
        CREATE_PCJ_CMD,
        DELETE_PCJ_CMD,
        CREATE_PERIODIC_PCJ_CMD,
        DELETE_PERIODIC_PCJ_CMD,
        LIST_INCREMENTAL_QUERIES})
    public boolean arePCJCommandsAvailable() {
        // The PCJ commands are only available if the Shell is connected to an instance of Rya
        // that is new enough to use the RyaDetailsRepository and is configured to maintain PCJs.
        final ShellState shellState = state.getShellState();
        if(shellState.getConnectionState() == ConnectionState.CONNECTED_TO_INSTANCE &&
                shellState.getStorageType().get() == StorageType.ACCUMULO) {
            final GetInstanceDetails getInstanceDetails = shellState.getConnectedCommands().get().getGetInstanceDetails();
            final String ryaInstanceName = state.getShellState().getRyaInstanceName().get();
            try {
                final Optional<RyaDetails> instanceDetails = getInstanceDetails.getDetails( ryaInstanceName );
                if(instanceDetails.isPresent()) {
                    return instanceDetails.get().getPCJIndexDetails().isEnabled();
                }
            } catch (final RyaClientException e) {
                return false;
            }
        }
        return false;
    }

    @CliCommand(value = LIST_INSTANCES_CMD, help = "List the names of the installed Rya instances.")
    public String listInstances() {
        // Fetch the command that is connected to the store.
        final ShellState shellState = state.getShellState();
        final RyaClient commands = shellState.getConnectedCommands().get();
        final Optional<String> ryaInstance = shellState.getRyaInstanceName();

        try {
            // Sort the names alphabetically.
            final List<String> instanceNames = commands.getListInstances().listInstances();
            Collections.sort( instanceNames );

            final String report;
            final InstanceNamesFormatter formatter = new InstanceNamesFormatter();
            if(ryaInstance.isPresent()) {
                report = formatter.format(instanceNames, ryaInstance.get());
            } else {
                report = formatter.format(instanceNames);
            }
            return report;

        } catch (final RyaClientException e) {
            throw new RuntimeException("Can not list the Rya instances. Reason: " + e.getMessage(), e);
        }
    }

    @CliCommand(value = INSTALL_CMD, help = "Create a new instance of Rya interactively.")
    public String install() {
        // Fetch the commands that are connected to the store.
        final RyaClient commands = state.getShellState().getConnectedCommands().get();

        String instanceName = null;
        InstallConfiguration installConfig = null;
        try {
            boolean verified = false;
            while(!verified) {
                // Use the install prompt to fetch the user's installation options.
                instanceName = installPrompt.promptInstanceName();
                installConfig = installPrompt.promptInstallConfiguration(instanceName);

                // Verify the configuration is what the user actually wants to do.
                verified = installPrompt.promptVerified(instanceName, installConfig);
            }

            // Execute the command.
            commands.getInstall().install(instanceName, installConfig);
            return String.format("The Rya instance named '%s' has been installed.", instanceName);

        } catch(final DuplicateInstanceNameException e) {
            throw new RuntimeException(String.format("A Rya instance named '%s' already exists. Try again with a different name.", instanceName), e);
        } catch (final IOException | RyaClientException e) {
            throw new RuntimeException("Could not install a new instance of Rya. Reason: " + e.getMessage(), e);
        }
    }

    @CliCommand(value = INSTALL_ACCUMULO_PARAMETERS_CMD, help = "Create a new Accumulo instance of Rya with command line parameters.")
    public String installWithAccumuloParameters(
            @CliOption(key = {"instanceName"}, mandatory = true, help = "The name of the Rya instance to create.")
            final String instanceName,

            @CliOption(key = {"enableTableHashPrefix"}, mandatory = false, help = "Use Shard Balancing (improves streamed input write speeds).", unspecifiedDefaultValue = "false", specifiedDefaultValue = "true")
            final boolean enableTableHashPrefix,

            @CliOption(key = {"enableEntityCentricIndex"}, mandatory = false, help = "Use Entity Centric Indexing.", unspecifiedDefaultValue = "false", specifiedDefaultValue = "true")
            final boolean enableEntityCentricIndex,

            @CliOption(key = {"enableFreeTextIndex"}, mandatory = false, help = "Use Free Text Indexing.", unspecifiedDefaultValue = "false", specifiedDefaultValue = "true")
            final boolean enableFreeTextIndex,

            // TODO RYA-215
//            @CliOption(key = {"enableGeospatialIndex"}, mandatory = false, help = "Use Geospatial Indexing.", unspecifiedDefaultValue = "false", specifiedDefaultValue = "true")
//            final boolean enableGeospatialIndex,

            @CliOption(key = {"enableTemporalIndex"}, mandatory = false, help = "Use Temporal Indexing.", unspecifiedDefaultValue = "false", specifiedDefaultValue = "true")
            final boolean enableTemporalIndex,

            @CliOption(key = {"enablePcjIndex"}, mandatory = false, help = "Use Precomputed Join (PCJ) Indexing.", unspecifiedDefaultValue = "false", specifiedDefaultValue = "true")
            final boolean enablePcjIndex,

            @CliOption(key = {"fluoPcjAppName"}, mandatory = false, help = "Fluo Application Name for PCJ Index Updater (fluo app must be initialized and enablePcjIndex=true).")
            final String fluoPcjAppName
            ) {

        // Fetch the commands that are connected to the store.
        final RyaClient commands = state.getShellState().getConnectedCommands().get();

        try {
            final InstallConfiguration installConfig = InstallConfiguration.builder()
                    .setEnableTableHashPrefix(enableTableHashPrefix)
                    .setEnableEntityCentricIndex(enableEntityCentricIndex)
                    .setEnableFreeTextIndex(enableFreeTextIndex)
                    // TODO RYA-215
//                    .setEnableGeoIndex(enableGeospatialIndex)
                    .setEnableTemporalIndex(enableTemporalIndex)
                    .setEnablePcjIndex(enablePcjIndex)
                    .setFluoPcjAppName(fluoPcjAppName)
                    .build();

            // Verify the configuration is what the user actually wants to do.
            if (!installPrompt.promptVerified(instanceName, installConfig)) {
                return "Skipping Installation.";
            }

            // Execute the command.
            commands.getInstall().install(instanceName, installConfig);
            return String.format("The Rya instance named '%s' has been installed.", instanceName);

        } catch(final DuplicateInstanceNameException e) {
            throw new RuntimeException(String.format("A Rya instance named '%s' already exists. Try again with a different name.", instanceName), e);
        } catch (final IOException | RyaClientException e) {
            throw new RuntimeException("Could not install a new instance of Rya. Reason: " + e.getMessage(), e);
        }
    }

    @CliCommand(value = INSTALL_MONGO_PARAMETERS_CMD, help = "Create a new MongoDB instance of Rya with command line parameters.")
    public String installWithMongoParameters(
            @CliOption(key = {"instanceName"}, mandatory = true, help = "The name of the Rya instance to create.")
            final String instanceName,

            @CliOption(key = {"enableFreeTextIndex"}, mandatory = false, help = "Use Free Text Indexing.", unspecifiedDefaultValue = "false", specifiedDefaultValue = "true")
            final boolean enableFreeTextIndex,

            // TODO RYA-215
//            @CliOption(key = {"enableGeospatialIndex"}, mandatory = false, help = "Use Geospatial Indexing.", unspecifiedDefaultValue = "false", specifiedDefaultValue = "true")
//            final boolean enableGeospatialIndex,

            @CliOption(key = {"enableTemporalIndex"}, mandatory = false, help = "Use Temporal Indexing.", unspecifiedDefaultValue = "false", specifiedDefaultValue = "true")
            final boolean enableTemporalIndex) {

        // Fetch the commands that are connected to the store.
        final RyaClient commands = state.getShellState().getConnectedCommands().get();

        try {
            final InstallConfiguration installConfig = InstallConfiguration.builder()
                    .setEnableFreeTextIndex(enableFreeTextIndex)
                    // TODO RYA-215
//                    .setEnableGeoIndex(enableGeospatialIndex)
                    .setEnableTemporalIndex(enableTemporalIndex)
                    .build();

            // Verify the configuration is what the user actually wants to do.
            if (!installPrompt.promptVerified(instanceName, installConfig)) {
                return "Skipping Installation.";
            }

            // Execute the command.
            commands.getInstall().install(instanceName, installConfig);
            return String.format("The Rya instance named '%s' has been installed.", instanceName);

        } catch(final DuplicateInstanceNameException e) {
            throw new RuntimeException(String.format("A Rya instance named '%s' already exists. Try again with a different name.", instanceName), e);
        } catch (final IOException | RyaClientException e) {
            throw new RuntimeException("Could not install a new instance of Rya. Reason: " + e.getMessage(), e);
        }
    }

    @CliCommand(value = PRINT_INSTANCE_DETAILS_CMD, help = "Print information about how the Rya instance is configured.")
    public String printInstanceDetails() {
        // Fetch the command that is connected to the store.
        final ShellState shellState = state.getShellState();
        final RyaClient commands = shellState.getConnectedCommands().get();
        final String ryaInstance = shellState.getRyaInstanceName().get();

        try {
            final Optional<RyaDetails> details = commands.getGetInstanceDetails().getDetails(ryaInstance);
            if(details.isPresent()) {
                return new RyaDetailsFormatter().format(details.get());
            } else {
                return "This instance of Rya does not have a Rya Details table. Consider migrating to a newer version of Rya.";
            }
        } catch(final InstanceDoesNotExistException e) {
            throw new RuntimeException(String.format("A Rya instance named '%s' does not exist.", ryaInstance), e);
        } catch (final RyaClientException e) {
            throw new RuntimeException("Could not get the instance details. Reason: " + e.getMessage(), e);
        }
    }

    @CliCommand(value = CREATE_PCJ_CMD, help = "Creates and starts the maintenance of a new PCJ using a Fluo application.")
    public String createPcj(
            @CliOption(key = {"exportToRya"}, mandatory = false, help = "Indicates that results for the query should be exported to a Rya PCJ table.", unspecifiedDefaultValue = "false", specifiedDefaultValue = "true") final
            boolean exportToRya,
            @CliOption(key = {"exportToKafka"}, mandatory = false, help = "Indicates that results for the query should be exported to a Kafka Topic.", unspecifiedDefaultValue = "false", specifiedDefaultValue = "true") final
            boolean exportToKafka) {
        // Fetch the command that is connected to the store.
        final ShellState shellState = state.getShellState();
        final RyaClient commands = shellState.getConnectedCommands().get();
        final String ryaInstance = shellState.getRyaInstanceName().get();

        try {
            final Set<ExportStrategy> strategies = new HashSet<>();
            if(exportToRya) {
                strategies.add(ExportStrategy.RYA);
            }
            if(exportToKafka) {
                strategies.add(ExportStrategy.KAFKA);
            }
            if(strategies.isEmpty()) {
                return "The user must specify at least one export strategy: (--exportToRya, --exportToKafka)";
            }

            // Prompt the user for the SPARQL.
            final Optional<String> sparql = sparqlPrompt.getSparql();
            if (sparql.isPresent()) {
                // Execute the command.
                final String pcjId = commands.getCreatePCJ().get().createPCJ(ryaInstance, sparql.get(), strategies);
                // Return a message that indicates the ID of the newly created ID.
                return String.format("The PCJ has been created. Its ID is '%s'.", pcjId);
            } else {
                return ""; // user aborted the SPARQL prompt.
            }
        } catch (final InstanceDoesNotExistException e) {
            throw new RuntimeException(String.format("A Rya instance named '%s' does not exist.", ryaInstance), e);
        } catch (final IOException | RyaClientException e) {
            throw new RuntimeException("Could not create the PCJ. Provided reasons: " + e.getMessage(), e);
        }
    }

    @CliCommand(value = DELETE_PCJ_CMD, help = "Deletes and halts maintenance of a PCJ.")
    public String deletePcj(
            @CliOption(key = {"pcjId"}, mandatory = true, help = "The ID of the PCJ that will be deleted.")
            final String pcjId) {
        // Fetch the command that is connected to the store.
        final ShellState shellState = state.getShellState();
        final RyaClient commands = shellState.getConnectedCommands().get();
        final String ryaInstance = shellState.getRyaInstanceName().get();

        try {
            // Execute the command.
            commands.getDeletePCJ().get().deletePCJ(ryaInstance, pcjId);
            return "The PCJ has been deleted.";

        } catch (final InstanceDoesNotExistException e) {
            throw new RuntimeException(String.format("A Rya instance named '%s' does not exist.", ryaInstance), e);
        } catch (final RyaClientException e) {
            throw new RuntimeException("The PCJ could not be deleted. Provided reason: " + e.getMessage(), e);
        }
    }

    @CliCommand(value = CREATE_PERIODIC_PCJ_CMD, help = "Creates and starts the maintenance of a new Periodic PCJ and registers the associated Periodic Notification with Kafka.")
    public String createPeriodicPcj(
            @CliOption(key = {"topic"}, mandatory = true, help = "Kafka topic for registering new PeriodicNotifications.  This topic is monitored by the Periodic Notification Service.") final
            String topic,
            @CliOption(key = {"brokers"}, mandatory = true, help = "Comma delimited list of host/port pairs to establish the initial connection to the Kafka cluster.") final
            String brokers) {
        // Fetch the command that is connected to the store.
        final ShellState shellState = state.getShellState();
        final RyaClient commands = shellState.getConnectedCommands().get();
        final String ryaInstance = shellState.getRyaInstanceName().get();

        try {
            // Prompt the user for the SPARQL.
            final Optional<String> sparql = sparqlPrompt.getSparql();
            if (sparql.isPresent()) {
                // Execute the command.
                final String pcjId = commands.getCreatePeriodicPCJ().get().createPeriodicPCJ(ryaInstance, sparql.get(), topic, brokers);
                // Return a message that indicates the ID of the newly created ID.
                return String.format("The Periodic PCJ has been created. Its ID is '%s'.", pcjId);
            } else {
                return ""; // user aborted the SPARQL prompt.
            }
        } catch (final InstanceDoesNotExistException e) {
            throw new RuntimeException(String.format("A Rya instance named '%s' does not exist.", ryaInstance), e);
        } catch (final IOException | RyaClientException e) {
            throw new RuntimeException("Could not create the Periodic PCJ. Provided reasons: " + e.getMessage(), e);
        }
    }

    @CliCommand(value = DELETE_PERIODIC_PCJ_CMD, help = "Deletes and halts maintenance of a Periodic PCJ.")
    public String deletePeriodicPcj(
            @CliOption(key = {"pcjId"}, mandatory = true, help = "The ID of the PCJ that will be deleted.")
            final String pcjId,
            @CliOption(key = {"topic"}, mandatory = true, help = "Kafka topic for registering a delete notice to remove a PeriodicNotification from the Periodic Notification Service.")
            final String topic,
            @CliOption(key = {"brokers"}, mandatory = true, help = "Comma delimited list of host/port pairs to establish the initial connection to the Kafka cluster.")
            final String brokers
            ) {
        // Fetch the command that is connected to the store.
        final ShellState shellState = state.getShellState();
        final RyaClient commands = shellState.getConnectedCommands().get();
        final String ryaInstance = shellState.getRyaInstanceName().get();

        try {
            // Execute the command.
            commands.getDeletePeriodicPCJ().get().deletePeriodicPCJ(ryaInstance, pcjId, topic, brokers);
            return "The Periodic PCJ has been deleted.";

        } catch (final InstanceDoesNotExistException e) {
            throw new RuntimeException(String.format("A Rya instance named '%s' does not exist.", ryaInstance), e);
        } catch (final RyaClientException e) {
            throw new RuntimeException("The Periodic PCJ could not be deleted. Provided reason: " + e.getMessage(), e);
        }
    }


    @CliCommand(value = LIST_INCREMENTAL_QUERIES, help = "Lists relevant information about all SPARQL queries maintained by the Fluo application.")
    public String listFluoQueries() {
        // Fetch the command that is connected to the store.
        final ShellState shellState = state.getShellState();
        final RyaClient commands = shellState.getConnectedCommands().get();
        final String ryaInstance = shellState.getRyaInstanceName().get();

        try {
            return commands.getListIncrementalQueries().get().listIncrementalQueries(ryaInstance);
        } catch (final InstanceDoesNotExistException e) {
            throw new RuntimeException(String.format("A Rya instance named '%s' does not exist.", ryaInstance), e);
        } catch (final RyaClientException e) {
            throw new RuntimeException("Could not list incremental queries. Provided reasons: " + e.getMessage(), e);
        }
    }


    @CliCommand(value = ADD_USER_CMD, help = "Adds an authorized user to the Rya instance.")
    public void addUser(
            @CliOption(key = {"username"}, mandatory = true, help = "The username of the user that will be granted access.")
            final String username) {
        // Fetch the Rya client that is connected to the store.
        final ShellState shellState = state.getShellState();
        final RyaClient ryaClient = shellState.getConnectedCommands().get();
        final String ryaInstance = shellState.getRyaInstanceName().get();

        try {
            ryaClient.getAddUser().get().addUser(ryaInstance, username);
        } catch (final InstanceDoesNotExistException e) {
            throw new RuntimeException(String.format("A Rya instance named '%s' does not exist.", ryaInstance), e);
        } catch (final RyaClientException e) {
            throw new RuntimeException("The user's access could not be granted. Provided reason: " + e.getMessage(), e);
        }
    }

    @CliCommand(value = REMOVE_USER_CMD, help = "Removes an authorized user from the Rya instance.")
    public void removeUser(
            @CliOption(key = {"username"}, mandatory = true, help = "The username of the user whose access will be revoked.")
            final String username) {
        // Fetch the Rya client that is connected to the store.
        final ShellState shellState = state.getShellState();
        final RyaClient ryaClient = shellState.getConnectedCommands().get();
        final String ryaInstance = shellState.getRyaInstanceName().get();

        try {
            ryaClient.getRemoveUser().get().removeUser(ryaInstance, username);
        } catch (final InstanceDoesNotExistException e) {
            throw new RuntimeException(String.format("A Rya instance named '%s' does not exist.", ryaInstance), e);
        } catch (final RyaClientException e) {
            throw new RuntimeException("The user's access could not be revoked. Provided reason: " + e.getMessage(), e);
        }
    }

    @CliCommand(value = UNINSTALL_CMD, help = "Uninstall an instance of Rya.")
    public String uninstall() {
        // Fetch the command that is connected to the store.
        final ShellState shellState = state.getShellState();
        final RyaClient commands = shellState.getConnectedCommands().get();
        final String ryaInstanceName = shellState.getRyaInstanceName().get();

        try {
            // Make sure the user meant to uninstall the Rya instance.
            if(!uninstallPrompt.promptAreYouSure(ryaInstanceName)) {
                return "Cancelled.";
            }

            // Perform the uninstall.
            commands.getUninstall().uninstall(ryaInstanceName);

        } catch (final InstanceDoesNotExistException e) {
            throw new RuntimeException(String.format("A Rya instance named '%s' does not exist.", ryaInstanceName), e);
        } catch (final IOException | RyaClientException e) {
            throw new RuntimeException("The Rya instance could not be uninstalled. Provided reason: " + e.getMessage(), e);
        }

        return "The Rya instance named '" + ryaInstanceName +"' has been uninstalled.";
    }
}