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

import com.beust.jcommander.Parameter;
import com.google.common.base.Objects;

public class BenchmarkOptions {
    @Parameter(names = { "-ii", "--ingest-iterations" }, description = "Number of ingest iterations.  Total data published is -i x -obs x -t", required = true)
    private int numIterations;

    @Parameter(names = { "-iobs", "--ingest-observations-per-type" }, description = "Observations per Type per Iteration to generate.", required = true)
    private int observationsPerTypePerIteration;

    @Parameter(names = { "-it", "--ingest-types" }, description = "The number of unique types to generate.", required = true)
    private int numTypes;

    @Parameter(names = { "-itp", "--ingest-type-prefix" }, description = "The prefix to use for a type, for example 'car_'", required = true)
    private String typePrefix;

    @Parameter(names = { "-ip", "--ingest-period-sec" }, description = "The period, in seconds between ingests of the data generated for one iteration.", required = true)
    private int ingestPeriodSeconds;

    @Parameter(names = { "-rp", "--report-period-sec" }, description = "The period, in seconds between persisting reports of the current state.", required = true)
    private int resultPeriodSeconds;

    public int getNumIterations() {
        return numIterations;
    }

    public int getObservationsPerTypePerIteration() {
        return observationsPerTypePerIteration;
    }

    public int getNumTypes() {
        return numTypes;
    }

    public String getTypePrefix() {
        return typePrefix;
    }

    public int getIngestPeriodSeconds() {
        return ingestPeriodSeconds;
    }

    public int getResultPeriodSeconds() {
        return resultPeriodSeconds;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("numIterations", numIterations)
                .add("observationsPerTypePerIteration", observationsPerTypePerIteration)
                .add("numTypes", numTypes)
                .add("typePrefix", typePrefix)
                .add("ingestPeriodSeconds", ingestPeriodSeconds)
                .add("resultPeriodSeconds", resultPeriodSeconds)
                .toString();
    }
}
