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

import java.math.BigInteger;

import org.apache.accumulo.core.client.Connector;
import org.apache.log4j.Logger;
import org.apache.rya.indexing.pcj.fluo.api.CountStatements;
import org.apache.rya.indexing.pcj.fluo.client.PcjAdminClientCommand;

import org.apache.fluo.api.client.FluoClient;
import org.apache.rya.rdftriplestore.RyaSailRepository;

/**
 * A command that prints the number of RDF Statements that are loaded into the
 * Fluo application and have not been processed yet.
 */
public class CountUnprocessedStatementsCommand implements PcjAdminClientCommand {

    private static final Logger log = Logger.getLogger(CountUnprocessedStatementsCommand.class);

    @Override
    public String getCommand() {
        return "count-unprocessed-statements";
    }

    @Override
    public String getDescription() {
        return "Lists the number of Statements that have been inserted into the Fluo application that have not been processed yet.";
    }

    @Override
    public String getUsage() {
        return "There are no parameters associated with this command.";
    }

    @Override
    public void execute(final Connector accumulo, final String ryaTablePrefix, final RyaSailRepository rya, final FluoClient fluo, final String[] args) throws ArgumentsException, ExecutionException {
        checkNotNull(accumulo);
        checkNotNull(ryaTablePrefix);
        checkNotNull(rya);
        checkNotNull(fluo);
        checkNotNull(args);

        log.trace("Executing the Count Unprocessed Triples Command...");

        final BigInteger count = new CountStatements().countStatements(fluo);
        System.out.println("There are " + count.toString() + " unprocessed Statements stored in the Fluo app." );
    }
}