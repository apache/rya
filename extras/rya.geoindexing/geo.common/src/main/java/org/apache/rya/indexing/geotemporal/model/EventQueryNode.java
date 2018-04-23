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
package org.apache.rya.indexing.geotemporal.model;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.indexing.IndexingExpr;
import org.apache.rya.indexing.TemporalInstant;
import org.apache.rya.indexing.TemporalInstantRfc3339;
import org.apache.rya.indexing.entity.query.EntityQueryNode;
import org.apache.rya.indexing.geotemporal.storage.EventStorage;
import org.apache.rya.indexing.mongodb.update.RyaObjectStorage.ObjectStorageException;
import org.apache.rya.rdftriplestore.evaluation.ExternalBatchingIterator;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.FunctionCall;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.ExternalSet;
import org.eclipse.rdf4j.query.algebra.evaluation.iterator.CollectionIteration;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.vividsolutions.jts.geom.Geometry;

public class EventQueryNode extends ExternalSet implements ExternalBatchingIterator {
    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    private final Collection<FunctionCall> usedFilters;
    private final Collection<IndexingExpr> geoFilters;
    private final Collection<IndexingExpr> temporalFilters;

    private final StatementPattern geoPattern;
    private final StatementPattern temporalPattern;

    //Information about the subject of the patterns.
    private final boolean subjectIsConstant;
    private final Optional<String> subjectVar;
    //not final because if the subject is a variable and the evaluate() is
    //  provided a binding set that contains the subject, this optional is used.
    private Optional<String> subjectConstant;

    //since and EventQueryNode exists in a single segment, all binding names are garunteed to be assured.
    private final Set<String> bindingNames;

    private Collection<StatementPattern> patterns;

    private final EventStorage eventStore;

    /**
     * Constructs an instance of {@link EventQueryNode}.
     * @param usedFilters
     *
     * @param type - The type of {@link Event} this node matches. (not null)
     * @param patterns - The query StatementPatterns that are solved using an
     *   Event of the Type. (not null)
     * @param entities - The {@link EventStorage} that will be searched to match
     *   {@link BindingSet}s when evaluating a query. (not null)
     */
    private EventQueryNode(final EventStorage eventStore, final StatementPattern geoPattern, final StatementPattern temporalPattern, final Collection<IndexingExpr> geoFilters, final Collection<IndexingExpr> temporalFilters, final Collection<FunctionCall> usedFilters) throws IllegalStateException {
        this.geoPattern = requireNonNull(geoPattern);
        this.temporalPattern = requireNonNull(temporalPattern);
        this.geoFilters = requireNonNull(geoFilters);
        this.temporalFilters = requireNonNull(temporalFilters);
        this.eventStore = requireNonNull(eventStore);
        this.usedFilters = requireNonNull(usedFilters);
        bindingNames = new HashSet<>();

        // Subject based preconditions.
        verifySameSubjects(getPatterns());
        // Predicate based preconditions.
        verifyAllPredicatesAreConstants(getPatterns());

        // The Subject may either be constant or a variable.
        final Var subject = patterns.iterator().next().getSubjectVar();
        subjectIsConstant = subject.isConstant();
        if(subjectIsConstant) {
            subjectConstant = Optional.of( subject.getValue().toString() );
            subjectVar = Optional.empty();
        } else {
            subjectConstant = Optional.empty();
            subjectVar = Optional.of( subject.getName() );
        }
    }

    @Override
    public Set<String> getBindingNames() {
        return bindingNames;
    }

    @Override
    public Set<String> getAssuredBindingNames() {
        return bindingNames;
    }

    /**
     * Verify the Subject for all of the patterns is the same.
     *
     * @param patterns - The patterns to check.
     * @throws IllegalStateException If all of the Subjects are not the same.
     */
    private static void verifySameSubjects(final Collection<StatementPattern> patterns) throws IllegalStateException {
        requireNonNull(patterns);

        final Iterator<StatementPattern> it = patterns.iterator();
        final Var subject = it.next().getSubjectVar();

        while(it.hasNext()) {
            final StatementPattern pattern = it.next();
            if(!pattern.getSubjectVar().equals(subject)) {
                throw new IllegalStateException("At least one of the patterns has a different subject from the others. " +
                        "All subjects must be the same.");
            }
        }
    }

    /**
     * Verifies all of the Statement Patterns have Constants for their predicates.
     *
     * @param patterns - The patterns to check. (not null)
     * @throws IllegalStateException A pattern has a variable predicate.
     */
    private static void verifyAllPredicatesAreConstants(final Collection<StatementPattern> patterns) throws IllegalStateException {
        requireNonNull(patterns);

        for(final StatementPattern pattern : patterns) {
            if(!pattern.getPredicateVar().isConstant()) {
                throw new IllegalStateException("The Predicate of a Statement Pattern must be constant. Pattern: " + pattern);
            }
        }
    }

    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(final BindingSet bindings) throws QueryEvaluationException {
        final List<BindingSet> list = new ArrayList<>();
        try {
            final Collection<Event> searchEvents;
            final String subj;
            //if the provided binding set has the subject already, set it to the constant subject.
            if(!subjectConstant.isPresent() && bindings.hasBinding(subjectVar.get())) {
                subjectConstant = Optional.of(bindings.getValue(subjectVar.get()).stringValue());
            } else if(bindings.size() != 0) {
                list.add(bindings);
            }

            // If the subject needs to be filled in, check if the subject variable is in the binding set.
            if(subjectConstant.isPresent()) {
                // if it is, fetch that value and then fetch the entity for the subject.
                subj = subjectConstant.get();
                searchEvents = eventStore.search(Optional.of(new RyaIRI(subj)), Optional.of(geoFilters), Optional.of(temporalFilters));
            } else {
                searchEvents = eventStore.search(Optional.empty(), Optional.of(geoFilters), Optional.of(temporalFilters));
            }

            for(final Event event : searchEvents) {
                final MapBindingSet resultSet = new MapBindingSet();
                if(event.getGeometry().isPresent()) {
                    final Geometry geo = event.getGeometry().get();
                    final Value geoValue = VF.createLiteral(geo.toText());
                    final Var geoObj = geoPattern.getObjectVar();
                    resultSet.addBinding(geoObj.getName(), geoValue);
                }

                final Value temporalValue;
                if(event.isInstant() && event.getInstant().isPresent()) {
                    final Optional<TemporalInstant> opt = event.getInstant();
                    DateTime dt = opt.get().getAsDateTime();
                    dt = dt.toDateTime(DateTimeZone.UTC);
                    final String str = dt.toString(TemporalInstantRfc3339.FORMATTER);
                    temporalValue = VF.createLiteral(str);
                } else if(event.getInterval().isPresent()) {
                    temporalValue = VF.createLiteral(event.getInterval().get().getAsPair());
                } else {
                    temporalValue = null;
                }

                if(temporalValue != null) {
                    final Var temporalObj = temporalPattern.getObjectVar();
                    resultSet.addBinding(temporalObj.getName(), temporalValue);
                }
                list.add(resultSet);
            }
        } catch (final ObjectStorageException e) {
            throw new QueryEvaluationException("Failed to evaluate the binding set", e);
        }
        return new CollectionIteration<>(list);
    }

    public Collection<IndexingExpr> getGeoFilters() {
        return geoFilters;
    }

    public Collection<IndexingExpr> getTemporalFilters() {
        return temporalFilters;
    }

    public Collection<FunctionCall> getFilters() {
        return usedFilters;
    }

    public Collection<StatementPattern> getPatterns() {
        if(patterns == null) {
            patterns = new ArrayList<>();
            patterns.add(geoPattern);
            patterns.add(temporalPattern);
        }
        return patterns;
    }

    @Override
    public int hashCode() {
        return Objects.hash(subjectIsConstant,
                subjectVar,
                geoFilters,
                temporalFilters,
                geoPattern,
                temporalPattern,
                bindingNames,
                eventStore);
    }

    @Override
    public boolean equals(final Object other) {
        if(other instanceof EventQueryNode) {
            final EventQueryNode otherNode = (EventQueryNode)other;
            return new EqualsBuilder()
                .append(subjectIsConstant, otherNode.subjectIsConstant)
                .append(subjectVar, otherNode.subjectVar)
                .append(geoFilters, otherNode.geoFilters)
                .append(geoPattern, otherNode.geoPattern)
                .append(temporalFilters, otherNode.temporalFilters)
                .append(temporalPattern, otherNode.temporalPattern)
                .append(bindingNames, otherNode.bindingNames)
                .append(subjectConstant, otherNode.subjectConstant)
                .isEquals();
        }
        return false;
    }

    @Override
    public EventQueryNode clone() {
        return new EventQueryNode(eventStore, geoPattern, temporalPattern, geoFilters, temporalFilters, usedFilters);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("Geo Pattern: " + geoPattern.toString());
        sb.append("\n--Geo Filters--\n");
        for(final IndexingExpr filter : geoFilters) {
            sb.append(filter.toString());
            sb.append("\n");
        }
        sb.append("\n-------------------\n");
        sb.append("Temporal Pattern: " + temporalPattern.toString());
        sb.append("\n--Temporal Filters--\n");
        for(final IndexingExpr filter : temporalFilters) {
            sb.append(filter.toString());
            sb.append("\n");
        }
        return sb.toString();
    }

    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(final Collection<BindingSet> bindingset)
            throws QueryEvaluationException {
        return null;
    }

    /**
     * Builder for {@link EventQueryNode}s.
     */
    public static class EventQueryNodeBuilder {
        private EventStorage store;
        private StatementPattern geoPattern;
        private StatementPattern temporalPattern;
        private Collection<IndexingExpr> geoFilters;
        private Collection<IndexingExpr> temporalFilters;
        private Collection<FunctionCall> usedFilters;

        /**
         * @param store - The {@link EventStorage} to use in the {@link EntityQueryNode}
         * @return - The Builder.
         */
        public EventQueryNodeBuilder setStorage(final EventStorage store) {
            this.store = store;
            return this;
        }

        /**
         * @param geoPattern - The geo {@link StatementPattern} to use in the {@link EntityQueryNode}
         * @return - The Builder.
         */
        public EventQueryNodeBuilder setGeoPattern(final StatementPattern geoPattern) {
            this.geoPattern = geoPattern;
            return this;
        }

        /**
         * @param temporalPattern - The temporal {@link StatementPattern} to use in the {@link EntityQueryNode}
         * @return - The Builder.
         */
        public EventQueryNodeBuilder setTemporalPattern(final StatementPattern temporalPattern) {
            this.temporalPattern = temporalPattern;
            return this;
        }

        /**
         * @param geoFilters - The geo filter(s) {@link IndexingExpr} to use in the {@link EntityQueryNode}
         * @return - The Builder.
         */
        public EventQueryNodeBuilder setGeoFilters(final Collection<IndexingExpr> geoFilters) {
            this.geoFilters = geoFilters;
            return this;
        }

        /**
         * @param temporalFilters - The temporal filter(s) {@link IndexingExpr} to use in the {@link EntityQueryNode}
         * @return - The Builder.
         */
        public EventQueryNodeBuilder setTemporalFilters(final Collection<IndexingExpr> temporalFilters) {
            this.temporalFilters = temporalFilters;
            return this;
        }

        /**
         * @param usedFilters - The filter(s) used by the {@link EntityQueryNode}
         * @return - The Builder.
         */
        public EventQueryNodeBuilder setUsedFilters(final Collection<FunctionCall> usedFilters) {
            this.usedFilters = usedFilters;
            return this;
        }

        /**
         * @return The {@link EntityQueryNode} built by the builder.
         */
        public EventQueryNode build() {
            return new EventQueryNode(store, geoPattern, temporalPattern, geoFilters, temporalFilters, usedFilters);
        }
    }
}
