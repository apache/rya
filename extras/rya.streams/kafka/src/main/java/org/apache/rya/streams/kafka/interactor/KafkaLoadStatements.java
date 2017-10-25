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

import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.rya.api.model.VisibilityStatement;
import org.apache.rya.streams.api.interactor.LoadStatements;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.Rio;
import org.openrdf.rio.helpers.RDFHandlerBase;
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

    private static final String KAFKA_CLIENT = "Load Statements";
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
    public void load(final Path statementsPath, final String visibilities) throws Exception {
        requireNonNull(statementsPath);
        requireNonNull(visibilities);

        final RDFParser parser = Rio.createParser(RDFFormat.forFileName(statementsPath.getFileName().toString()));
        parser.setRDFHandler(new RDFHandlerBase() {
            @Override
            public void startRDF() throws RDFHandlerException {
                log.trace("starting loading statements.");
            }

            @Override
            public void handleStatement(final Statement stmnt) throws RDFHandlerException {
                final VisibilityStatement visiStatement = new VisibilityStatement(stmnt, visibilities);
                producer.send(new ProducerRecord<>(topic, visiStatement));
            }

            @Override
            public void endRDF() throws RDFHandlerException {
                producer.flush();
                log.trace("done.");
            }
        });
        parser.parse(Files.newInputStream(statementsPath), "");
    }
}
