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

import java.io.ByteArrayOutputStream;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.serialization.Serializer;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.binary.BinaryRDFWriterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A Kafka {@link Serializer} that is able to serialize a set of {@link Statement}s
 * using the RDF4J Rio Binary format.
 */
@DefaultAnnotation(NonNull.class)
public class StatementsSerializer implements Serializer<Set<Statement>> {
    private static final Logger log = LoggerFactory.getLogger(StatementsSerializer.class);

    private static final BinaryRDFWriterFactory WRITER_FACTORY = new BinaryRDFWriterFactory();

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        // Nothing to do.
    }

    @Override
    public byte[] serialize(final String topic, final Set<Statement> data) {
        if(data == null) {
            // Returning null because that is the contract of this method.
            return null;
        }

        // Write the statements using a Binary RDF Writer.
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final RDFWriter writer = WRITER_FACTORY.getWriter(baos);
        writer.startRDF();

        for(final Statement stmt : data) {
            // Write the statement.
            log.debug("Writing Statement: " + stmt);
            writer.handleStatement(stmt);
        }
        writer.endRDF();

        // Return the byte[] version of the data.
        return baos.toByteArray();
    }

    @Override
    public void close() {
        // Nothing to do.
    }
}