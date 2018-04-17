/**
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
package org.apache.rya.kafka.connect.client.command;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.rya.kafka.connect.api.StatementsSerializer;
import org.apache.rya.kafka.connect.client.RyaKafkaClientCommand;
import org.apache.rya.rdftriplestore.utils.RdfFormatUtils;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.UnsupportedRDFormatException;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Writes {@link Statement}s to a Kafka topic using the Rya Kafka Connect Sink format.
 */
@DefaultAnnotation(NonNull.class)
public class WriteStatementsCommand implements RyaKafkaClientCommand {
    private static final Logger log = LoggerFactory.getLogger(WriteStatementsCommand.class);

    /**
     * Command line parameters that are used by this command to configure itself.
     */
    public static class WriteParameters extends KafkaParameters {
        @Parameter(names = {"--statementsFile", "-f"}, required = true, description = "The file of RDF statements to load into Rya Streams.")
        public String statementsFile;
    }

    @Override
    public String getCommand() {
        return "write";
    }

    @Override
    public String getDescription() {
        return "Writes Statements to the specified Kafka topic.";
    }

    @Override
    public boolean validArguments(final String[] args) {
        boolean valid = true;
        try {
            new JCommander(new WriteParameters(), args);
        } catch(final ParameterException e) {
            valid = false;
        }
        return valid;
    }

    /**
     * @return Describes what arguments may be provided to the command.
     */
    @Override
    public String getUsage() {
        final JCommander parser = new JCommander(new WriteParameters());

        final StringBuilder usage = new StringBuilder();
        parser.usage(usage);
        return usage.toString();
    }

    @Override
    public void execute(final String[] args) throws ArgumentsException, ExecutionException {
        requireNonNull(args);

        // Parse the command line arguments.
        final WriteParameters params = new WriteParameters();
        try {
            new JCommander(params, args);
        } catch(final ParameterException e) {
            throw new ArgumentsException("Could not stream the query's results because of invalid command line parameters.", e);
        }

        // Verify the configured statements file path.
        final Path statementsPath = Paths.get(params.statementsFile);
        if(!statementsPath.toFile().exists()) {
            throw new ArgumentsException("Could not load statements at path '" + statementsPath + "' because that " +
                    "file does not exist. Make sure you've entered the correct path.");
        }

        // Create an RDF Parser whose format is derived from the statementPath's file extension.
        final String filename = statementsPath.getFileName().toString();
        final RDFFormat format = RdfFormatUtils.forFileName(filename);
        if (format == null) {
            throw new UnsupportedRDFormatException("Unknown RDF format for the file: " + filename);
        }
        final RDFParser parser = Rio.createParser(format);

        // Set up the producer.
        try(Producer<String, Set<Statement>> producer = makeProducer(params)) {
            // Set a handler that writes the statements to the specified kafka topic. It writes batches of 5 Statements.
            parser.setRDFHandler(new AbstractRDFHandler() {

                private Set<Statement> batch = new HashSet<>(5);

                @Override
                public void startRDF() throws RDFHandlerException {
                    log.trace("Starting loading statements.");
                }

                @Override
                public void handleStatement(final Statement stmnt) throws RDFHandlerException {
                    log.trace("Adding statement.");
                    batch.add(stmnt);

                    if(batch.size() == 5) {
                        flushBatch();
                    }
                }

                @Override
                public void endRDF() throws RDFHandlerException {
                    if(!batch.isEmpty()) {
                        flushBatch();
                    }
                    log.trace("Done.");
                }

                private void flushBatch() {
                    log.trace("Flushing batch of size " + batch.size());
                    producer.send(new ProducerRecord<>(params.topic, null, batch));
                    batch = new HashSet<>(5);
                    producer.flush();
                }
            });

            // Do the parse and load.
            try {
                parser.parse(Files.newInputStream(statementsPath), "");
            } catch (RDFParseException | RDFHandlerException | IOException e) {
                throw new ExecutionException("Could not load the RDF file's Statements into the Kafka topic.", e);
            }
        }
    }

    private static Producer<String, Set<Statement>> makeProducer(final KafkaParameters params) {
        requireNonNull(params);
        final Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, params.bootstrapServers);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StatementsSerializer.class.getName());
        return new KafkaProducer<>(props);
    }
}