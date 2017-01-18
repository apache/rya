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

import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.indexing.IndexingExpr;
import org.apache.rya.indexing.TemporalInstantRfc3339;
import org.apache.rya.indexing.geotemporal.storage.EventStorage;
import org.apache.rya.indexing.mongodb.update.RyaObjectStorage.ObjectStorageException;
import org.apache.rya.rdftriplestore.evaluation.ExternalBatchingIterator;
import org.openrdf.model.Value;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.FunctionCall;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.evaluation.impl.ExternalSet;
import org.openrdf.query.algebra.evaluation.iterator.CollectionIteration;
import org.openrdf.query.impl.MapBindingSet;

import com.vividsolutions.jts.geom.Geometry;

import info.aduna.iteration.CloseableIteration;

public class EventQueryNode extends ExternalSet implements ExternalBatchingIterator {
    private final Collection<FunctionCall> usedFilters;
    private final Collection<IndexingExpr> geoFilters;
    private final Collection<IndexingExpr> temporalFilters;

    private final StatementPattern geoPattern;
    private final StatementPattern temporalPattern;

    //Information about the subject of the patterns.
    private final boolean subjectIsConstant;
    private final Optional<String> subjectConstant;
    private final Optional<String> subjectVar;

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
    public EventQueryNode(final EventStorage eventStore, final StatementPattern geoPattern, final StatementPattern temporalPattern, final Collection<IndexingExpr> geoFilters, final Collection<IndexingExpr> temporalFilters, final Collection<FunctionCall> usedFilters) throws IllegalStateException {
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
            // If the subject needs to be filled in, check if the subject variable is in the binding set.
            if(subjectIsConstant) {
                // if it is, fetch that value and then fetch the entity for the subject.
                subj = subjectConstant.get();
                searchEvents = eventStore.search(Optional.of(new RyaURI(subj)), Optional.of(geoFilters), Optional.of(temporalFilters));
            } else {
                searchEvents = eventStore.search(Optional.empty(), Optional.of(geoFilters), Optional.of(temporalFilters));
            }

            for(final Event event : searchEvents) {
                final MapBindingSet resultSet = new MapBindingSet();
                if(event.getGeometry().isPresent()) {
                    final Geometry geo = event.getGeometry().get();
                    final Value geoValue = ValueFactoryImpl.getInstance().createLiteral(geo.toText());
                    final Var geoObj = geoPattern.getObjectVar();
                    resultSet.addBinding(geoObj.getName(), geoValue);
                }

                final Value temporalValue;
                if(event.isInstant() && event.getInstant().isPresent()) {
                    temporalValue = ValueFactoryImpl.getInstance().createLiteral(event.getInstant().get().getAsDateTime().toString(TemporalInstantRfc3339.FORMATTER));
                } else if(event.getInterval().isPresent()) {
                    temporalValue = ValueFactoryImpl.getInstance().createLiteral(event.getInterval().get().getAsPair());
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
        if(bindings.size() != 0) {
            list.add(bindings);
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

            return  Objects.equals(subjectIsConstant, otherNode.subjectIsConstant) &&
                    Objects.equals(subjectVar, otherNode.subjectVar) &&
                    Objects.equals(geoFilters, otherNode.geoFilters) &&
                    Objects.equals(geoPattern, otherNode.geoPattern) &&
                    Objects.equals(temporalFilters, otherNode.temporalFilters) &&
                    Objects.equals(temporalPattern, otherNode.temporalPattern) &&
                    Objects.equals(bindingNames, otherNode.bindingNames) &&
                    Objects.equals(subjectConstant, otherNode.subjectConstant);
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
}
