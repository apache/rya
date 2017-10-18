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
package org.apache.rya.streams.kafka.interactor;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.rya.api.model.VisibilityStatement;
import org.apache.rya.rdftriplestore.utils.RdfFormatUtils;
import org.apache.rya.streams.api.exception.RyaStreamsException;
import org.apache.rya.streams.api.interactor.LoadStatements;
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

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Loads {@link VisibilityStatement}s from an RDF file into a kafka topic.
 */
@DefaultAnnotation(NonNull.class)
public class KafkaLoadStatements implements LoadStatements {
    private static final Logger log = LoggerFactory.getLogger(KafkaLoadStatements.class);

    private final String topic;
    private final Producer<?, VisibilityStatement> producer;

    /**
     * Creates a new {@link KafkaLoadStatements}.
     *
     * @param topic - The Kafka topic to load statements into. (not null)
     * @param producer - The {@link Producer} connected to Kafka. (not null)
     */
    public KafkaLoadStatements(final String topic, final Producer<?, VisibilityStatement> producer) {
        this.topic = requireNonNull(topic);
        this.producer = requireNonNull(producer);
    }

    @Override
    public void fromFile(final Path statementsPath, final String visibilities) throws RyaStreamsException {
        requireNonNull(statementsPath);
        requireNonNull(visibilities);

        if(!statementsPath.toFile().exists()) {
            throw new RyaStreamsException("Could not load statements at path '" + statementsPath + "' because that " +
                    "does not exist. Make sure you've entered the correct path.");
        }

        // Create an RDF Parser whose format is derived from the statementPath's file extension.
        final String filename = statementsPath.getFileName().toString();
        final RDFFormat format = RdfFormatUtils.forFileName(filename);
        if (format == null) {
            throw new UnsupportedRDFormatException("Unknown RDF format for the file: " + filename);
        }
        final RDFParser parser = Rio.createParser(format);

        // Set a handler that writes the statements to the specified kafka topic.
        parser.setRDFHandler(new AbstractRDFHandler() {
            @Override
            public void startRDF() throws RDFHandlerException {
                log.trace("Starting loading statements.");
            }

            @Override
            public void handleStatement(final Statement stmnt) throws RDFHandlerException {
                final VisibilityStatement visiStatement = new VisibilityStatement(stmnt, visibilities);
                producer.send(new ProducerRecord<>(topic, visiStatement));
            }

            @Override
            public void endRDF() throws RDFHandlerException {
                producer.flush();
                log.trace("Done.");
            }
        });

        // Do the parse and load.
        try {
            parser.parse(Files.newInputStream(statementsPath), "");
        } catch (RDFParseException | RDFHandlerException | IOException e) {
            throw new RyaStreamsException("Could not load the RDF file's Statements into Rya Streams.", e);
        }
    }

    @Override
    public void fromCollection(final Collection<VisibilityStatement> statements) throws RyaStreamsException {
        requireNonNull(statements);

        for(final VisibilityStatement statement : statements) {
            producer.send(new ProducerRecord<>(topic, statement));
        }
        producer.flush();
    }
}