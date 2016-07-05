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
package org.apache.rya.indexing.pcj.fluo.client.command;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.accumulo.core.client.Connector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.rya.indexing.pcj.fluo.api.GetPcjMetadata;
import org.apache.rya.indexing.pcj.fluo.api.GetPcjMetadata.NotInAccumuloException;
import org.apache.rya.indexing.pcj.fluo.api.GetPcjMetadata.NotInFluoException;
import org.apache.rya.indexing.pcj.fluo.client.PcjAdminClientCommand;
import org.apache.rya.indexing.pcj.fluo.client.util.PcjMetadataRenderer;
import org.apache.rya.indexing.pcj.storage.PcjMetadata;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

import io.fluo.api.client.FluoClient;
import mvm.rya.rdftriplestore.RyaSailRepository;

/**
 * A command that lists information about the queries that are being managed by the Fluo app.
 */
@ParametersAreNonnullByDefault
public class ListQueriesCommand implements PcjAdminClientCommand {
    private static final Logger log = LogManager.getLogger(ListQueriesCommand.class);

    /**
     * Command line parameters that are used by this command to configure itself.
     */
    private static final class Parameters {
        @Parameter(names = "--queryId", required = false, description = "Make this command only fetch the metadata for the specififed Query ID.")
        private String queryId;
    }

    @Override
    public String getCommand() {
        return "list-queries";
    }

    @Override
    public String getDescription() {
        return "View metadata about the queries that are loaded in the Fluo app";
    }

    @Override
    public String getUsage() {
        final JCommander parser = new JCommander(new Parameters());

        final StringBuilder usage = new StringBuilder();
        parser.usage(usage);
        return usage.toString();
    }

    @Override
    public void execute(final Connector accumulo, final String ryaTablePrefix, final RyaSailRepository rya, final FluoClient fluo, final String[] args) throws ArgumentsException, ExecutionException {
        checkNotNull(accumulo);
        checkNotNull(fluo);
        checkNotNull(args);

        log.trace("Executing the List Queries Command...");

        // Parse the command line arguments.
        final Parameters params = new Parameters();
        try {
            new JCommander(params, args);
        } catch(final ParameterException e) {
            throw new ArgumentsException("Could not list the queries because of invalid command line parameters.", e);
        }

        // Fetch the PCJ metadata that will be included in the report.
        final GetPcjMetadata getPcjMetadata = new GetPcjMetadata();
        final Map<String, PcjMetadata> metadata = new HashMap<String, PcjMetadata>();
        try {
            if(params.queryId != null) {
                log.trace("Fetch the PCJ Metadata from Accumulo for Query ID '" + params.queryId + "'.");
                metadata.put(params.queryId, getPcjMetadata.getMetadata(accumulo, fluo, params.queryId));
            } else {
                log.trace("Fetch the PCJ Metadata from Accumulo for all queries that are being updated by Fluo.");
                metadata.putAll( getPcjMetadata.getMetadata(accumulo, fluo) );
            }
        } catch (NotInFluoException | NotInAccumuloException e) {
            throw new ExecutionException("Could not fetch some of the metadata required to build the report.", e);
        }

        // Write the metadata to the console.
        log.trace("Rendering the queries report...");
        if(metadata.isEmpty()) {
            System.out.println("No queries are being tracked by Fluo.");
        } else {
            final PcjMetadataRenderer renderer = new PcjMetadataRenderer();
            try {
                final String report = renderer.render(metadata);
                System.out.println("The number of Queries that are being tracked by Fluo: " + metadata.size());
                System.out.println(report);
            } catch (final Exception e) {
                throw new ExecutionException("Unable to render the query metadata report for output.", e);
            }
        }

        log.trace("Finished executing the List Queries Command.");
    }
}