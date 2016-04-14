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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.accumulo.core.client.Connector;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.rya.indexing.pcj.fluo.api.CreatePcj;
import org.apache.rya.indexing.pcj.fluo.client.PcjAdminClientCommand;
import org.apache.rya.indexing.pcj.fluo.client.util.ParsedQueryRequest;
import org.apache.rya.indexing.pcj.storage.PcjException;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.sail.SailException;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

import io.fluo.api.client.FluoClient;
import mvm.rya.rdftriplestore.RyaSailRepository;

/**
 * A command that creates a creates a new PCJ in the Fluo app and loads historic
 * statement pattern matches for it.
 */
@ParametersAreNonnullByDefault
public class NewQueryCommand implements PcjAdminClientCommand {
    private static final Logger log = LogManager.getLogger(NewQueryCommand.class);

    /**
     * Command line parameters that are used by this command to configure itself.
     */
    private static final class Parameters {
        @Parameter(names = "--queryRequestFile", required = true, description = "The path to a file containing the SPARQL query that will be loaded into the Fluo app.")
        private String queryRequestFile;
    }

    @Override
    public String getCommand() {
        return "new-query";
    }

    @Override
    public String getDescription() {
        return "Add a SPARQL query to the Fluo app";
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

        log.trace("Executing the New Query Command...");

        // Parse the command line arguments.
        final Parameters params = new Parameters();
        try {
            new JCommander(params, args);
        } catch(final ParameterException e) {
            throw new ArgumentsException("Could not create a new query because of invalid command line parameters.", e);
        }

        // Load the request from the file into memory.
        log.trace("Loading the query found in file '" + params.queryRequestFile + "' into the client app.");
        ParsedQueryRequest request = null;
        try {
            final Path requestFile = Paths.get(params.queryRequestFile);
            final String requestText = IOUtils.toString( Files.newInputStream(requestFile) );
            request = ParsedQueryRequest.parse(requestText);
        } catch (final IOException e) {
            throw new ExecutionException("Could not load the query request into memory.", e);
        }

        // Load the query into the Fluo app.
        log.trace("SPARQL Query: " + request.getQuery());
        log.trace("Var Orders: " + request.getVarOrders());
        log.trace("Loading these values into the Fluo app.");
        final CreatePcj createPcj = new CreatePcj();
        try {
            createPcj.withRyaIntegration(fluo, ryaTablePrefix, rya, accumulo, request.getVarOrders(), request.getQuery());
        } catch (MalformedQueryException | SailException | QueryEvaluationException | PcjException e) {
            throw new ExecutionException("Could not create and load historic matches into the the Fluo app for the query.", e);
        }

        log.trace("Finished executing the New Query Command.");
    }
}