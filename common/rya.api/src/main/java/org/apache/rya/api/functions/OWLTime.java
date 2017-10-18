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
package org.apache.rya.api.functions;

import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Constants for OWL-Time primitives in the OWL-Time namespace.
 *
 */
public class OWLTime {

    private static final ValueFactory vf = SimpleValueFactory.getInstance();

    /**
     * Indicates namespace of OWL-Time ontology
     */
    public static final String NAMESPACE = "http://www.w3.org/2006/time#";
    /**
     * Seconds class of type DurationDescription in OWL-Time ontology
     */
    public static final IRI SECONDS_URI = vf.createIRI(NAMESPACE, "seconds");
    /**
     * Minutes class of type DurationDescription in OWL-Time ontology
     */
    public static final IRI MINUTES_URI = vf.createIRI(NAMESPACE, "minutes");
    /**
     * Hours class of type DurationDescription in OWL-Time ontology
     */
    public static final IRI HOURS_URI = vf.createIRI(NAMESPACE, "hours");
    /**
     * Days class of type DurationDescription in OWL-Time ontology
     */
    public static final IRI DAYS_URI = vf.createIRI(NAMESPACE, "days");
    /**
     * Weeks class of type DurationDescription in OWL-Time ontology
     */
    public static final IRI WEEKS_URI = vf.createIRI(NAMESPACE, "weeks");

    private static final Map<IRI, ChronoUnit> DURATION_MAP = new HashMap<>();

    static {
        DURATION_MAP.put(SECONDS_URI, ChronoUnit.SECONDS);
        DURATION_MAP.put(MINUTES_URI, ChronoUnit.MINUTES);
        DURATION_MAP.put(HOURS_URI, ChronoUnit.HOURS);
        DURATION_MAP.put(DAYS_URI, ChronoUnit.DAYS);
        DURATION_MAP.put(WEEKS_URI, ChronoUnit.WEEKS);
    }

    /**
     * Verifies whether URI is a valid OWL-Time URI that is supported by this class.
     * @param durationURI - OWLTime URI indicating the time unit (not null)
     * @return - {@code true} if this URI indicates a supported OWLTime time unit
     */
    public static boolean isValidDurationType(IRI durationURI) {
        checkNotNull(durationURI);
        return DURATION_MAP.containsKey(durationURI);
    }

    /**
     * Returns the duration in milliseconds
     *
     * @param duration - amount of time in the units indicated by the provided {@link OWLTime} URI
     * @param uri - OWLTime URI indicating the time unit of duration (not null)
     * @return - the amount of time in milliseconds
     * @throws IllegalArgumentException if provided {@link IRI} is not a valid, supported OWL-Time time unit.
     */
    public static long getMillis(int duration, IRI uri) throws IllegalArgumentException {
        Optional<ChronoUnit> unit = getChronoUnitFromURI(uri);
        checkArgument(unit.isPresent(),
                String.format("URI %s does not indicate a valid OWLTime time unit.  URI must of be of type %s, %s, %s, %s, or %s .", uri,
                        SECONDS_URI, MINUTES_URI, HOURS_URI, DAYS_URI, WEEKS_URI));
        return duration * unit.get().getDuration().toMillis();
    }

    /**
     * Converts the {@link OWLTime} URI time unit to a {@link ChronoUnit} time unit
     *
     * @param durationURI - OWLTime time unit URI (not null)
     * @return - corresponding ChronoUnit time unit
     */
    public static Optional<ChronoUnit> getChronoUnitFromURI(IRI durationURI) {
        return Optional.ofNullable(DURATION_MAP.get(durationURI));
    }
}
