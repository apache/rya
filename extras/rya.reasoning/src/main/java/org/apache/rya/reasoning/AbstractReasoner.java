package org.apache.rya.reasoning;

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

import java.util.HashSet;
import java.util.Set;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

/**
 * Abstract class for reasoning in the neighborhood of a particular resource.
 * Contains general methods for collecting facts and derivations, and
 * determining whether those facts are potentially new.
 *
 * As reasoning rules are applied, facts and inconsistencies are collected and
 * stored in this object. (This allows some simple logic to reduce redundant
 * fact generation.) The caller can use getFacts and getInconsistencies to
 * retrieve them when needed. (This clears the buffered facts in the reasoner
 * object.)
 */
public abstract class AbstractReasoner {
    /**
     * Reasoning is done with respect to this particular node.
     */
    protected final Resource node;

    /**
     * All schema (TBox/RBox) data is accessed through this.
     */
    protected final Schema schema;

    /**
     * The current iteration in the overall reasoning algorithm.
     */
    protected final int currentIteration;

    /**
     * A newly derived fact should have at least one source which is at least
     * this recent.
     */
    final int minIteration;

    /**
     * If the global schema has ever been updated during reasoning iteration,
     * this will be the iteration number of that update (otherwise 0).
     */
    final int lastSchemaChange;

    // Newly derived information
    Set<Fact> newFacts = new HashSet<>();
    Set<Derivation> inconsistencies = new HashSet<>();

    /**
     * Constructor.
     * @param   node    Conduct reasoning about/around this node
     * @param   schema  Global schema (class/property) information
     * @param   t       Iteration # of new triples
     * @param   tSchema Iteration # of latest schema update. If the schema has
     *                  not been changed, we can ignore many triples that we
     *                  expect to already have used.
     */
    public AbstractReasoner(Resource node, Schema schema, int t, int tSchema) {
        this.node = node;
        this.schema = schema;
        this.currentIteration = t;
        this.lastSchemaChange = tSchema;
        if (tSchema < (t - 1)) {
            this.minIteration = 0;
        }
        else {
            this.minIteration = t - 1;
        }
    }

    /**
     * Store inconsistency if it really is new.
     * @return  Whether it really needed to be stored
     */
    protected boolean collectInconsistency(Derivation inconsistency) {
        // If at least one of its sources can indeed be used for derivations,
        // store this fact
        for (Fact source : inconsistency.getSources()) {
            if (frontier(source)) {
                return inconsistencies.add(inconsistency);
            }
        }
        return false;
    }

    /**
     * Store fact if it really is new.
     * @return  Whether it really needed to be stored
     */
    protected boolean collect(Fact fact) {
        // If this fact was just generated, at least one of its sources can
        // indeed be used for derivations, and the sources don't include it
        // itself, store this fact
        if (fact.getIteration() == currentIteration && !fact.isCycle()) {
            Derivation d = fact.getDerivation();
            for (Fact source : d.getSources()) {
                if (frontier(source)) {
                    return newFacts.add(fact);
                }
            }
        }
        return false;
    }

    /**
     * Whether any new facts have been derived and not yet retrieved.
     */
    public boolean hasNewFacts() {
        return !newFacts.isEmpty();
    }

    /**
     * Whether any inconsistencies have been derived and not retrieved.
     */
    public boolean hasInconsistencies() {
        return !inconsistencies.isEmpty();
    }

    /**
     * Return the latest facts and set current new results to empty.
     */
    public Set<Fact> getFacts() {
        Set<Fact> results = newFacts;
        newFacts = new HashSet<Fact>();
        return results;
    }

    /**
     * Return the latest inconsistencies and set inconsistencies to empty.
     */
    public Set<Derivation> getInconsistencies() {
        Set<Derivation> results = inconsistencies;
        inconsistencies = new HashSet<Derivation>();
        return results;
    }

    /**
     * Create a Fact representing a triple inferred by this reasoner.
     * @param   rule    The specific rule rule that yielded the inference
     * @param   source  One (might be the only) fact used in the derivation
     */
    protected Fact triple(Resource s, URI p, Value o, OwlRule rule,
            Fact source) {
        Fact fact = new Fact(s, p, o, this.currentIteration,
            rule, this.node);
        fact.addSource(source);
        return fact;
    }

    /**
     * Create a Derivation representing an inconsistency found by this reasoner.
     * @param   rule    The specific rule rule that yielded the inconsistency
     * @param   source  One (might be the only) fact used in the derivation
     */
    protected Derivation inconsistency(OwlRule rule, Fact source) {
        Derivation d = new Derivation(this.currentIteration, rule, this.node);
        d.addSource(source);
        return d;
    }

    /**
     * Determine whether a fact is on the frontier of knowledge so far, meaning
     * it is new enough to imply further unknown information. If false, we can
     * expect inferences based on this fact to have already been made in the
     * step during which it was initially derived. Any interesting fact coming
     * from this reasoner should have at least one of its direct sources on
     * the frontier. Considers only age, not semantics.
     *
     * Three cases that indicate this is on the frontier:
     * 1) We are looking at all the data (minIteration==0, either because this
     *      is the first pass through the data or because we reset on updating
     *      the schema).
     * 2) This fact was generated (by this reasoner) during this iteration, so
     *      it hasn't been seen before.
     * 3) It was generated by another node's reasoner since minIteration,
     *      meaning it hasn't been seen yet by a reasoner for this node.
     *
     * In any case, inconsistencies are not used to derive anything, so they
     * always return false.
     */
    protected boolean frontier(Fact fact) {
        int t = fact.getIteration();
        Resource dNode = null;
        if (fact.isInference()) {
            dNode = fact.getDerivation().getNode();
        }
        return (minIteration == 0) || (t == currentIteration)
            || (t >= minIteration && !this.node.equals(dNode));
    }

    /**
     * Get some summary information for logging/debugging.
     */
    public String getDiagnostics() {
        StringBuilder sb = new StringBuilder();
        sb.append(newFacts.size()).append(" new facts buffered");
        sb.append(inconsistencies.size()).append(" new inconsistencies buffered");
        return sb.toString();
    }

    /**
     * Get the number of inputs cached.
     */
    public int getNumStored() { return 0; }

    /**
     * Get the node this reasoner reasons around.
     */
    public Resource getNode() { return node; }
}
