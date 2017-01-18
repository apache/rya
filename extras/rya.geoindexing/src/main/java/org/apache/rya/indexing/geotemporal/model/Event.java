/**
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
package org.apache.rya.indexing.geotemporal.model;

import static java.util.Objects.requireNonNull;

import java.util.Objects;
import java.util.Optional;

import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.indexing.TemporalInstant;
import org.apache.rya.indexing.TemporalInterval;
import org.apache.rya.indexing.geotemporal.GeoTemporalIndexer;

import com.vividsolutions.jts.geom.Geometry;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Query object for a {@link GeoTemporalIndexer}.
 * Defines a {@link Geometry}, either a {@link TemporalInstant} or
 * {@link TemporalInterval}, and a triple Subject.
 */
public class Event {
    private final Optional<Geometry> geometry;
    private final Optional<TemporalInstant> instant;
    private final Optional<TemporalInterval> interval;
    private final RyaURI subject;

    private final boolean isInstant;

    /**
     * Creates a new {@link Event} query object with a {@link TemporalInstant}.
     * @param geo - The {@link Geometry} to use when querying.
     * @param instant - The {@link TemporalInstant} to use when querying.
     * @param subject - The Subject that both statements must have when querying.
     */
    private Event(final Geometry geo, final TemporalInstant instant, final RyaURI subject) {
        this.subject = requireNonNull(subject);

        //these fields are nullable since they are filled field by field.
        this.instant = Optional.ofNullable(instant);
        geometry = Optional.ofNullable(geo);
        isInstant = true;
        interval = Optional.empty();
    }

    /**
     * Creates a new {@link Event} query object with a {@link TemporalInterval}.
     * @param geo - The {@link Geometry} to use when querying.
     * @param interval - The {@link TemporalInterval} to use when querying.
     * @param subject - The Subject that both statements must have when querying.
     */
    private Event(final Geometry geo, final TemporalInterval interval, final RyaURI subject) {
        this.subject = requireNonNull(subject);

        //these fields are nullable since they are filled field by field.
        this.interval = Optional.ofNullable(interval);
        geometry = Optional.ofNullable(geo);
        isInstant = false;
        instant = Optional.empty();
    }

    /**
     * @return Whether or not the query object uses a {@link TemporalInstant}.
     */
    public boolean isInstant() {
        return isInstant;
    }

    /**
     * @return The {@link Geometry} to use when querying.
     */
    public Optional<Geometry> getGeometry() {
        return geometry;
    }

    /**
     * @return The {@link TemporalInstant} to use when querying.
     */
    public Optional<TemporalInstant> getInstant() {
        return instant;
    }

    /**
     * @return The {@link TemporalInterval} to use when querying.
     */
    public Optional<TemporalInterval> getInterval() {
        return interval;
    }

    /**
     * @return The statement subject.
     */
    public RyaURI getSubject() {
        return subject;
    }

    @Override
    public int hashCode() {
        if(isInstant) {
            return Objects.hash(subject, geometry, instant);
        } else {
            return Objects.hash(subject, geometry, interval);
        }
    }

    @Override
    public boolean equals(final Object o) {
        if(this == o) {
            return true;
        }
        if(o instanceof Event) {
            final Event event = (Event) o;
            return Objects.equals(subject, event.subject) &&
                    Objects.equals(isInstant, event.isInstant) &&
                    (isInstant ? Objects.equals(instant, event.instant) : Objects.equals(interval, event.interval));
        }
        return false;
    }

    public static Builder builder(final Event event) {
        final Builder builder = new Builder()
            .setSubject(event.getSubject());
        if(event.getGeometry().isPresent()) {
            builder.setGeometry(event.getGeometry().get());
        }
        if(event.isInstant()) {
            if(event.getInstant().isPresent()) {
                builder.setTemporalInstant(event.getInstant().get());
            }
        } else {
            if(event.getInterval().isPresent()) {
                builder.setTemporalInterval(event.getInterval().get());
            }
        }
        return builder;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builds instances of {@link Event}.
     */
    @DefaultAnnotation(NonNull.class)
    public static class Builder {
        private RyaURI subject;
        private Geometry geo;
        private TemporalInstant instant;
        private TemporalInterval interval;

        /**
         * Sets the {@link RyaURI} subject.
         * @param subject - The subject to key on the event.
         */
        public Builder setSubject(final RyaURI subject) {
            this.subject = subject;
            return this;
        }

        /**
         * Sets the {@link Geometry}.
         * @param geo - The geometry.
         */
        public Builder setGeometry(final Geometry geo) {
            this.geo = geo;
            return this;
        }

        /**
         * Sets the {@link TemporalInterval}.
         * @param interval - The interval.
         */
        public Builder setTemporalInterval(final TemporalInterval interval) {
            this.interval = interval;
            return this;
        }

        /**
         * Sets the {@link TemporalInstant}.
         * @param instant - The instant.
         */
        public Builder setTemporalInstant(final TemporalInstant instant) {
            this.instant = instant;
            return this;
        }

        /**
         * @return The new {@link Event}.
         */
        public Event build() {
            if(instant == null) {
                return new Event(geo, interval, subject);
            } else {
                return new Event(geo, instant, subject);
            }
        }
    }
}
