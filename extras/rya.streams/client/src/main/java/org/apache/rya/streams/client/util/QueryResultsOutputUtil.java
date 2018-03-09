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
package org.apache.rya.streams.client.util;

import static java.util.Objects.requireNonNull;

import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.api.model.VisibilityStatement;
import org.apache.rya.streams.api.entity.QueryResultStream;
import org.apache.rya.streams.api.exception.RyaStreamsException;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.resultio.sparqljson.SPARQLResultsJSONWriter;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.Rio;
import org.openrdf.rio.WriterConfig;
import org.openrdf.rio.helpers.BasicWriterSettings;

import com.google.common.collect.Lists;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A utility that writes {@link QueryResultStream} results to an {@link OutputStream}.
 */
@DefaultAnnotation(NonNull.class)
public class QueryResultsOutputUtil {

    /**
     * Private constructor to prevent instantiation.
     */
    private QueryResultsOutputUtil() { }

    /**
     * Writes the results of a {@link QueryResultStream} to the output stream as NTriples until the
     * shutdown signal is set.
     *
     * @param out - The stream the NTriples data will be written to. (not null)
     * @param resultsStream - The results stream that will be polled for results to
     *   write to {@code out}. (not null)
     * @param shutdownSignal - Setting this signal will cause the thread that
     *   is processing this function to finish and leave. (not null)
     * @throws RDFHandlerException A problem was encountered while
     *   writing the NTriples to the output stream.
     * @throws IllegalStateException The {@code resultsStream} is closed.
     * @throws RyaStreamsException Could not fetch the next set of results.
     */
    public static void toNtriplesFile(
            final OutputStream out,
            final QueryResultStream<VisibilityStatement> resultsStream,
            final AtomicBoolean shutdownSignal) throws RDFHandlerException, IllegalStateException, RyaStreamsException {
        requireNonNull(out);
        requireNonNull(resultsStream);
        requireNonNull(shutdownSignal);

        final RDFWriter writer = Rio.createWriter(RDFFormat.NTRIPLES, out);
        writer.startRDF();

        while(!shutdownSignal.get()) {
            final Iterable<VisibilityStatement> it = resultsStream.poll(1000);
            for(final VisibilityStatement result : it) {
                writer.handleStatement(result);
            }
        }

        writer.endRDF();
    }

    /**
     * Writes the results of a {@link QueryResultStream} to the output stream as JSON until the
     * shutdown signal is set.
     *
     * @param out - The stream the JSON will be written to. (not null)
     * @param query - The parsed SPARQL Query whose results are being output. This
     *   object is used to figure out which bindings may appear. (not null)
     * @param resultsStream - The results stream that will be polled for results to
     *   write to {@code out}. (not null)
     * @param shutdownSignal - Setting this signal will cause the thread that
     *   is processing this function to finish and leave. (not null)
     * @throws TupleQueryResultHandlerException A problem was encountered while
     *   writing the JSON to the output stream.
     * @throws IllegalStateException The {@code resultsStream} is closed.
     * @throws RyaStreamsException Could not fetch the next set of results.
     */
    public static void toBindingSetJSONFile(
            final OutputStream out,
            final TupleExpr query,
            final QueryResultStream<VisibilityBindingSet> resultsStream,
            final AtomicBoolean shutdownSignal) throws TupleQueryResultHandlerException, IllegalStateException, RyaStreamsException {
        requireNonNull(out);
        requireNonNull(query);
        requireNonNull(resultsStream);
        requireNonNull(shutdownSignal);

        // Create a writer that does not pretty print.
        final SPARQLResultsJSONWriter writer = new SPARQLResultsJSONWriter(out);
        final WriterConfig config = writer.getWriterConfig();
        config.set(BasicWriterSettings.PRETTY_PRINT, false);

        // Start the JSON and enumerate the possible binding names.
        writer.startQueryResult( Lists.newArrayList(query.getBindingNames()) );

        while(!shutdownSignal.get()) {
            for(final VisibilityBindingSet result : resultsStream.poll(1000)) {
                writer.handleSolution(result);
            }
        }

        writer.endQueryResult();
    }
}