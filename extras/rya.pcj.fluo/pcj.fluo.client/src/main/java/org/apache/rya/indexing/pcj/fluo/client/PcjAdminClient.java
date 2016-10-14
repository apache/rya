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
package org.apache.rya.indexing.pcj.fluo.client;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.rya.indexing.pcj.fluo.client.PcjAdminClientCommand.ArgumentsException;
import org.apache.rya.indexing.pcj.fluo.client.PcjAdminClientCommand.ExecutionException;
import org.apache.rya.indexing.pcj.fluo.client.command.CountUnprocessedStatementsCommand;
import org.apache.rya.indexing.pcj.fluo.client.command.ListQueriesCommand;
import org.apache.rya.indexing.pcj.fluo.client.command.LoadTriplesCommand;
import org.apache.rya.indexing.pcj.fluo.client.command.NewQueryCommand;
import org.apache.rya.indexing.pcj.fluo.client.command.QueryReportCommand;
import org.openrdf.repository.RepositoryException;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.rdftriplestore.RdfCloudTripleStore;
import org.apache.rya.rdftriplestore.RyaSailRepository;

/**
 * An application that helps Rya PCJ administrators interact with the cluster.
 */
@ParametersAreNonnullByDefault
public class PcjAdminClient {

    private static final Logger log = LogManager.getLogger(PcjAdminClient.class);

    private static final Path PROPERTIES_FILE = Paths.get("conf/tool.properties");

    /**
     * Maps from command strings to the object that performs the command.
     */
    private static final ImmutableMap<String, PcjAdminClientCommand> commands;
    static {
        final Set<Class<? extends PcjAdminClientCommand>> commandClasses = new HashSet<>();
        commandClasses.add(NewQueryCommand.class);
        commandClasses.add(LoadTriplesCommand.class);
        commandClasses.add(ListQueriesCommand.class);
        commandClasses.add(QueryReportCommand.class);
        commandClasses.add(CountUnprocessedStatementsCommand.class);

        final ImmutableMap.Builder<String, PcjAdminClientCommand> builder = ImmutableMap.builder();
        for(final Class<? extends PcjAdminClientCommand> commandClass : commandClasses) {
            try {
                final PcjAdminClientCommand command = commandClass.newInstance();
                builder.put(command.getCommand(), command);
            } catch (InstantiationException | IllegalAccessException e) {
                System.err.println("Could not run the application because a PcjCommand is missing its empty constructor.");
                e.printStackTrace();
            }
        }
        commands = builder.build();
    }

    /**
     * Describes how this application may be used on the command line.
     */
    private static final String usage = makeUsage(commands);

    public static void main(final String[] args) {
        log.trace("Starting up the PCJ Admin Client.");

        // If no command provided or the command isn't recognized, then print the usage.
        if(args.length == 0 || !commands.containsKey(args[0])) {
            System.out.println(usage);
            System.exit(-1);
        }

        // Load the properties file.
        final Properties props = new Properties();
        try (InputStream pin = Files.newInputStream(PROPERTIES_FILE)) {
            props.load( pin );
        } catch (final IOException e) {
            throw new RuntimeException("Could not load properties file: " + PROPERTIES_FILE, e);
        }

        // Fetch the command that will be executed.
        final String command = args[0];
        final String[] commandArgs = Arrays.copyOfRange(args, 1, args.length);
        final PcjAdminClientCommand pcjCommand = commands.get(command);

        RyaSailRepository rya = null;
        FluoClient fluo = null;
        try {
            // Connect to Accumulo, Rya, and Fluo.
            final PcjAdminClientProperties clientProps = new PcjAdminClientProperties(props);
            final Connector accumulo = createAccumuloConnector(clientProps);
            rya = makeRyaRepository(clientProps, accumulo);
            fluo = createFluoClient( clientProps );

            // Execute the command.
            pcjCommand.execute(accumulo, clientProps.getRyaTablePrefix(), rya, fluo, commandArgs);

        } catch (final AccumuloException | AccumuloSecurityException e) {
            System.err.println("Could not connect to the Accumulo instance that hosts the export PCJ tables.");
            e.printStackTrace();
            System.exit(-1);
        } catch(final RepositoryException e) {
            System.err.println("Could not connect to the Rya instance that hosts the historic RDF statements.");
            e.printStackTrace();
            System.exit(-1);
        } catch (final ArgumentsException e) {
            System.err.println( pcjCommand.getUsage() );
            System.exit(-1);
        } catch (final ExecutionException e) {
            System.err.println("Could not execute the command.");
            e.printStackTrace();
            System.exit(-1);
        } finally {
            log.trace("Shutting down the PCJ Admin Client.");

            if(rya != null) {
                try {
                    rya.shutDown();
                } catch (final RepositoryException e) {
                    System.err.println("Problem while shutting down the Rya connection.");
                    e.printStackTrace();
                }
            }

            if(fluo != null) {
                fluo.close();
            }
        }
    }

    private static String makeUsage(final ImmutableMap<String, PcjAdminClientCommand> commands) {
        final StringBuilder usage = new StringBuilder();
        usage.append("Usage: ").append(PcjAdminClient.class.getSimpleName()).append(" <command> (<argument> ... )\n");
        usage.append("\n");
        usage.append("Possible Commands:\n");

        // Sort and find the max width of the commands.
        final List<String> sortedCommandNames = Lists.newArrayList( commands.keySet() );
        Collections.sort(sortedCommandNames);

        int maxCommandLength = 0;
        for(final String commandName : sortedCommandNames) {
            maxCommandLength = commandName.length() > maxCommandLength ? commandName.length() : maxCommandLength;
        }

        // Add each command to the usage.
        final String commandFormat = "    %-" + (maxCommandLength) + "s - %s\n";
        for(final String commandName : sortedCommandNames) {
            final String commandDescription = commands.get(commandName).getDescription();
            usage.append( String.format(commandFormat, commandName, commandDescription) );
        }

        return usage.toString();
    }

    private static Connector createAccumuloConnector(final PcjAdminClientProperties clientProps) throws AccumuloException, AccumuloSecurityException {
        checkNotNull(clientProps);

        // Connect to the Zookeepers.
        final String instanceName = clientProps.getAccumuloInstance();
        final String zooServers = clientProps.getAccumuloZookeepers();
        final Instance inst = new ZooKeeperInstance(instanceName, zooServers);

        // Create a connector to the Accumulo that hosts the PCJ export tables.
        return inst.getConnector(clientProps.getAccumuloUsername(), new PasswordToken(clientProps.getAccumuloPassword()));
    }

    private static RyaSailRepository makeRyaRepository(final PcjAdminClientProperties clientProps, final Connector accumulo) throws RepositoryException {
        checkNotNull(clientProps);
        checkNotNull(accumulo);

        // Setup Rya configuration values.
        final AccumuloRdfConfiguration ryaConf = new AccumuloRdfConfiguration();
        ryaConf.setTablePrefix( clientProps.getRyaTablePrefix() );

        // Connect to the Rya repo.
        final AccumuloRyaDAO accumuloRyaDao = new AccumuloRyaDAO();
        accumuloRyaDao.setConnector(accumulo);
        accumuloRyaDao.setConf(ryaConf);

        final RdfCloudTripleStore ryaStore = new RdfCloudTripleStore();
        ryaStore.setRyaDAO(accumuloRyaDao);

        final RyaSailRepository ryaRepo = new RyaSailRepository(ryaStore);
        ryaRepo.initialize();
        return ryaRepo;
    }

    private static FluoClient createFluoClient(final PcjAdminClientProperties clientProps) {
        checkNotNull(clientProps);
        final FluoConfiguration fluoConfig = new FluoConfiguration();

        // Fluo configuration values.
        fluoConfig.setApplicationName( clientProps.getFluoAppName() );
        fluoConfig.setInstanceZookeepers( clientProps.getAccumuloZookeepers() +  "/fluo" );

        // Accumulo Connection Stuff.
        fluoConfig.setAccumuloZookeepers( clientProps.getAccumuloZookeepers() );
        fluoConfig.setAccumuloInstance( clientProps.getAccumuloInstance() );
        fluoConfig.setAccumuloUser( clientProps.getAccumuloUsername() );
        fluoConfig.setAccumuloPassword( clientProps.getAccumuloPassword() );

        // Connect the client.
        return FluoFactory.newClient(fluoConfig);
    }
}