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
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.rya.indexing.pcj.fluo.api.InsertTriples;
import org.apache.rya.indexing.pcj.fluo.client.PcjAdminClientCommand;
import org.apache.rya.indexing.pcj.fluo.client.util.FluoLoader;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.ntriples.NTriplesParser;
import org.openrdf.rio.trig.TriGParser;
import org.openrdf.rio.turtle.TurtleParser;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

import org.apache.fluo.api.client.FluoClient;
import org.apache.rya.rdftriplestore.RyaSailRepository;

/**
 * A command that loads the contents of an NTriple file into the Fluo application.
 */
@ParametersAreNonnullByDefault
public class LoadTriplesCommand implements PcjAdminClientCommand {
    private static final Logger log = LogManager.getLogger(LoadTriplesCommand.class);

    /**
     * Command line parameters that are used by this command to configure itself.
     */
    private static final class Parameters {
        @Parameter(names = "--triplesFile", required = true, description = "The RDF file of statemetns to load into the Fluo app.")
        private String nTriplesFile;
    }

    @Override
    public String getCommand() {
        return "load-triples";
    }

    @Override
    public String getDescription() {
        return "Load RDF Triples into the Fluo app";
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

        log.trace("Executing the Load Triples Command...");

        // Parse the command line arguments.
        final Parameters params = new Parameters();
        try {
            new JCommander(params, args);
        } catch(final ParameterException e) {
            throw new ArgumentsException("Could not load the Triples file because of invalid command line parameters.", e);
        }

        // Iterate over the Statements that are in the input file and write them to Fluo.
        log.trace("Loading RDF Statements from the Triples file '" + params.nTriplesFile + "'.");
        final Path triplesPath = Paths.get( params.nTriplesFile );

        try {
            final RDFParser parser = makeParser(triplesPath);
            final FluoLoader loader = new FluoLoader(fluo, new InsertTriples());
            parser.setRDFHandler(loader);
            parser.parse(Files.newInputStream(triplesPath), triplesPath.toUri().toString());
        } catch (UnsupportedFormatException | RDFParseException | RDFHandlerException | IOException e) {
            throw new ExecutionException("Could not load the RDF file into the Fluo app.", e);
        }

        log.trace("Finished executing the Load Triples Command.");
    }

    private static RDFParser makeParser(final Path tripleFile) throws UnsupportedFormatException {
        checkNotNull(tripleFile);

        final String extension = FilenameUtils.getExtension( tripleFile.getFileName().toString() );
        switch(extension) {
            case "nt":
                return new NTriplesParser();
            case "ttl":
                return new TurtleParser();
            case "trig":
                return new TriGParser();
            default:
                throw new UnsupportedFormatException("RDF File with extension '" + extension + "' not supported.");
        }
    }

    private static final class UnsupportedFormatException extends Exception {
        private static final long serialVersionUID = 1L;

        public UnsupportedFormatException(final String message) {
            super(message);
        }
    }
}