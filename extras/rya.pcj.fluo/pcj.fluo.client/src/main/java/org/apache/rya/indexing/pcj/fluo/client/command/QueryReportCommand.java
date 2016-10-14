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

import org.apache.accumulo.core.client.Connector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.rya.indexing.pcj.fluo.client.PcjAdminClientCommand;
import org.apache.rya.indexing.pcj.fluo.api.GetQueryReport;
import org.apache.rya.indexing.pcj.fluo.api.GetQueryReport.QueryReport;
import org.apache.rya.indexing.pcj.fluo.client.util.QueryReportRenderer;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

import org.apache.fluo.api.client.FluoClient;
import org.apache.rya.rdftriplestore.RyaSailRepository;

/**
 * TODO implement this.
 */
public class QueryReportCommand implements PcjAdminClientCommand {
    private static final Logger log = LogManager.getLogger(NewQueryCommand.class);

    /**
     * Command line parameters that are used by this command to configure itself.
     */
    private static final class Parameters {
        @Parameter(names = "--queryId", required = true, description = "The Query ID used to build the report.")
        private String queryId;
    }

    @Override
    public String getCommand() {
        return "query-report";
    }

    @Override
    public String getDescription() {
        return "Build a report that indicates a query's structure within the Fluo app as well as how many Binding Sets have been emitted for each of its nodes.";
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
        checkNotNull(ryaTablePrefix);
        checkNotNull(rya);
        checkNotNull(fluo);
        checkNotNull(args);

        log.trace("Executing the Get Query Report Command...");

        // Parse the command line arguments.
        final Parameters params = new Parameters();
        try {
            new JCommander(params, args);
        } catch(final ParameterException e) {
            throw new ArgumentsException("Could not create a new query because of invalid command line parameters.", e);
        }

        // Build the report using what is stored in Fluo.
        log.trace("Building the report for Query ID: " + params.queryId);
        final QueryReport queryReport = new GetQueryReport().getReport(fluo, params.queryId);
        log.trace("Report built.");

        // Format and print the report.
        try {
            final String reportString = new QueryReportRenderer().render(queryReport);
            System.out.println(reportString);
        } catch(final Exception e) {
            throw new ExecutionException("Unable to render the query metadata report for output.", e);
        }

        log.trace("Finished executing the Get Query Report Command.");
    }
}