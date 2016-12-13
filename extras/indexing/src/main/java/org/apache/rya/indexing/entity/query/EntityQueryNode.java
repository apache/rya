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
package org.apache.rya.indexing.entity.query;

import static java.util.Objects.requireNonNull;

import java.util.Collection;
import java.util.Iterator;
import java.util.Optional;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.rya.indexing.entity.model.Type;
import org.apache.rya.indexing.entity.storage.EntityStorage;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.evaluation.impl.ExternalSet;

import com.google.common.collect.ImmutableMap;

import info.aduna.iteration.CloseableIteration;
import mvm.rya.api.domain.RyaURI;
import mvm.rya.rdftriplestore.evaluation.ExternalBatchingIterator;

/**
 * TODO impl, test, doc
 */
@ParametersAreNonnullByDefault
public class EntityQueryNode extends ExternalSet implements ExternalBatchingIterator {

    /**
     * The RyaURI that when used as the Predicate of a Statement Pattern indicates the Type of the Entities.
     */
    private static final RyaURI TYPE_ID_URI = new RyaURI(RDF.TYPE.toString());

    // Provided at construction time.
    private final Type type;
    private final Collection<StatementPattern> patterns;
    private final EntityStorage entities;

    // Information about the subject of the patterns.
    private final boolean subjectIsConstant;
    private final Optional<String> subjectConstant;
    private final Optional<String> subjectVar;

    // Information about the objects of the patterns.

    // XXX what does this map? property name -> binding variable?
    // for any property of the entity that has a variable, have to fill it in?
    private final ImmutableMap<RyaURI, String> objectVariables;


    /**
     * Constructs an instance of {@link EntityQueryNode}.
     *
     * @param type - The type of {@link Entity} this node matches. (not null)
     * @param patterns - The query StatementPatterns that are solved using an
     *   Entity of the Type. (not null)
     * @param entities - The {@link EntityStorage} that will be searched to match
     *   {@link BindingSet}s when evaluating a query. (not null)
     */
    public EntityQueryNode(final Type type, final Collection<StatementPattern> patterns, final EntityStorage entities) throws IllegalStateException {
        this.type = requireNonNull(type);
        this.patterns = requireNonNull(patterns);
        this.entities = requireNonNull(entities);

        // Subject based preconditions.
        verifySameSubjects(patterns);

        // Predicate based preconditions.
        verifyAllPredicatesAreConstants(patterns);
        verifyHasCorrectTypePattern(type, patterns);
        verifyAllPredicatesPartOfType(type, patterns);

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

        // TODO Also, map each variable that is in an Object spot each variable can be mapped to a property name as well
        // Processing note:
        // Any constant that appears in the Object portion of the SP will be used to make sure they match.

        objectVariables = null;
    }

    /**
     * Verify the Subject for all of the patterns is the same.
     *
     * @param patterns - The patterns to check.
     * @throws IllegalStateException If all of the Subjects are not the same.
     */
    private static void verifySameSubjects(Collection<StatementPattern> patterns) throws IllegalStateException {
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
    private static void verifyAllPredicatesAreConstants(Collection<StatementPattern> patterns) throws IllegalStateException {
        requireNonNull(patterns);

        for(final StatementPattern pattern : patterns) {
            if(!pattern.getPredicateVar().isConstant()) {
                throw new IllegalStateException("The Predicate of a Statement Pattern must be constant. Pattern: " + pattern);
            }
        }
    }

    /**
     * Verifies a single Statement Pattern defines the Type of Entity this query node matches.
     *
     * @param type - The expected Type. (not null)
     * @param patterns - The patterns to check. (not null)
     * @throws IllegalStateException No Type or the wrong Type is specified by the patterns.
     */
    private static void verifyHasCorrectTypePattern(Type type, Collection<StatementPattern> patterns) throws IllegalStateException {
        requireNonNull(type);
        requireNonNull(patterns);

        boolean typeFound = false;

        for(final StatementPattern pattern : patterns) {
            final RyaURI predicate = new RyaURI(pattern.getPredicateVar().getValue().toString());

            if(predicate.equals(TYPE_ID_URI)) {
                final RyaURI typeId = new RyaURI( pattern.getObjectVar().getValue().stringValue() );
                if(typeId.equals(type.getId())) {
                    typeFound = true;
                } else {
                    throw new IllegalStateException("Statement Pattern encountred for a Type that does not match the expected Type." +
                            " Expected Type = '" + type.getId().getData() + "' Found Type = '" + typeId.getData() + "'");
                }
            }
        }

        if(!typeFound) {
            throw new IllegalStateException("The collection of Statement Patterns that this node matches must define which Type they match.");
        }
    }

    /**
     * Verify all of the patterns have predicates that match one of the Type's property names.
     *
     * @param type - The Type the patterns match. (not null)
     * @param patterns - The patterns to check.
     * @throws IllegalStateException If any of the non-type defining Statement Patterns
     *   contain a predicate that does not match one of the Type's property names.
     */
    private static void verifyAllPredicatesPartOfType(Type type, Collection<StatementPattern> patterns) throws IllegalStateException {
        requireNonNull(type);
        requireNonNull(patterns);

        for(final StatementPattern pattern : patterns) {
            // Skip TYPE patterns.
            final RyaURI predicate = new RyaURI( pattern.getPredicateVar().getValue().toString() );
            if(predicate.equals(TYPE_ID_URI)) {
                continue;
            }

            if(!type.getPropertyNames().contains(predicate)) {
                throw new IllegalStateException("The Predicate of a Statement Pattern must be a property name for the Type. " +
                        "Type ID: '" + type.getId().getData() + "' Pattern: " + pattern);
            }
        }
    }






    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(Collection<BindingSet> bindingSets) throws QueryEvaluationException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(BindingSet bindingSet) throws QueryEvaluationException {
        requireNonNull(bindingSet);

        // ... ok, so if the subject needs to be filled in, then we need to see if the subject variable is in the binding set.
        // if it is, fetch that value and then fetch the entity for the subject.

        // if it isn't, fetch the entity for the constant?


        // RETURN AN EMPTY ITERATION IF IT CAN NOT FILL IT IN!

        // for all variables in the OBJECT portion of the SPs, fill 'em in using the entity that is stored in the index.

        // x = alice's SSN
        // y = blue  <-- how do i know this is for urn:eye property? FROM THE STATEMENT PATTERN. look for the ?y in the SP, and that has the property name in it.

        // TODO Auto-generated method stub
        return null;
    }
}