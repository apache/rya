/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.benchmark.periodic;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * Generates sets of statements used for benchmarking
 */
public class BenchmarkStatementGenerator {

    private static final Logger logger = LoggerFactory.getLogger(BenchmarkStatementGenerator.class);

    private static final ValueFactory VF = SimpleValueFactory.getInstance();
    private final DatatypeFactory dtf;

    public BenchmarkStatementGenerator() throws DatatypeConfigurationException {
        dtf = DatatypeFactory.newInstance();
    }

    /**
     * Generates (numObservationsPerType x numTypes) statements of the form:
     *
     * <pre>
     * urn:obs_n uri:hasTime zonedTime
     * urn:obs_n uri:hasObsType typePrefix_m
     * </pre>
     *
     * Where the n in urn:obs_n is the ith value in 0 to (numObservationsPerType x numTypes) with an offset specified by
     * observationOffset, and where m in typePrefix_m is the jth value in 0 to numTypes.
     *
     * @param numObservationsPerType - The quantity of observations per type to generate.
     * @param numTypes - The number of types to generate observations for.
     * @param typePrefix - The prefix to be used for the type literal in the statement.
     * @param observationOffset - The offset to be used for determining the value of n in the above statements.
     * @param zonedTime - The time to be used for all observations generated.
     * @return A new list of all generated Statements.
     */
    public List<Statement> generate(final long numObservationsPerType, final int numTypes, final String typePrefix, final long observationOffset, final ZonedDateTime zonedTime) {
        final String time = zonedTime.format(DateTimeFormatter.ISO_INSTANT);
        final Literal litTime = VF.createLiteral(dtf.newXMLGregorianCalendar(time));
        final List<Statement> statements = Lists.newArrayList();

        for (long i = 0; i < numObservationsPerType; i++) {
            for(int j = 0; j < numTypes; j++) {
                final long observationId = observationOffset + i*numTypes + j;
                //final String obsId = "urn:obs_" + Long.toHexString(observationId)  + "_" + observationId;
                //final String obsId = "urn:obs_" + observationId;
                final String obsId = "urn:obs_" + String.format("%020d", observationId);
                final String type = typePrefix + j;
                //logger.info(obsId + " " + type + " " + litTime);
                statements.add(VF.createStatement(VF.createIRI(obsId), VF.createIRI("uri:hasTime"), litTime));
                statements.add(VF.createStatement(VF.createIRI(obsId), VF.createIRI("uri:hasObsType"), VF.createLiteral(type)));
            }
        }

        return statements;
    }
}
