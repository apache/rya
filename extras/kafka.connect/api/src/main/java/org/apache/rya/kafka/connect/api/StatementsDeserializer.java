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
package org.apache.rya.kafka.connect.api;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.serialization.Deserializer;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.binary.BinaryRDFParserFactory;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A Kafka {@link Deserializer} that is able to deserialize an RDF4J Rio Binary format serialized
 * set of {@link Statement}s.
 */
@DefaultAnnotation(NonNull.class)
public class StatementsDeserializer implements Deserializer<Set<Statement>> {
    private static final Logger log = LoggerFactory.getLogger(StatementsDeserializer.class);

    private static final BinaryRDFParserFactory PARSER_FACTORY = new BinaryRDFParserFactory();

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        // Nothing to do.
    }

    @Override
    public Set<Statement> deserialize(final String topic, final byte[] data) {
        if(data == null || data.length == 0) {
            // Return null because that is the contract of this method.
            return null;
        }

        try {
            final RDFParser parser = PARSER_FACTORY.getParser();
            final Set<Statement> statements = new HashSet<>();

            parser.setRDFHandler(new AbstractRDFHandler() {
                @Override
                public void handleStatement(final Statement statement) throws RDFHandlerException {
                    log.debug("Statement: " + statement);
                    statements.add( statement );
                }
            });

            parser.parse(new ByteArrayInputStream(data), null);
            return statements;

        } catch(final RDFParseException | RDFHandlerException | IOException e) {
            log.error("Could not deserialize a Set of VisibilityStatement objects using the RDF4J Rio Binary format.", e);
            return null;
        }
    }

    @Override
    public void close() {
        // Nothing to do.
    }
}