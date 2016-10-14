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

import java.util.LinkedList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;

/**
 * Keep track of a single node's types and do reasoning about its types.
 */
public class TypeReasoner extends AbstractReasoner {
    // This node's types, whether asserted or derived
    Map<Resource, Fact> knownTypes = new HashMap<>();

    // Inferences waiting for particular types to be discovered
    Map<Resource, List<Fact>> possibleInferences = new HashMap<>();
    Map<Resource, List<Derivation>> possibleInconsistencies = new HashMap<>();

    /**
     * Constructor.
     * @param   node    Conduct reasoning about/around this node
     * @param   schema  Global schema (class/property) information
     * @param   t       Iteration # of new triples
     * @param   tSchema Iteration # of latest schema change
     */
    public TypeReasoner(Resource node, Schema schema, int t, int tSchema) {
        super(node, schema, t, tSchema);
    }

    /**
     * Process a type (class) assignment from the input. It may have been
     * inferred during a previous iteration.
     * @param typeFact  An assertion about one of this node's types
     */
    void processType(Fact typeFact) {
        Resource type = (Resource) typeFact.getObject();
        boolean newType = !knownTypes.containsKey(type);
        int t = typeFact.getIteration();
        // Save the type in memory unless older knowledge takes precedence
        if (newType || t < knownTypes.get(type).getIteration()) {
            knownTypes.put(type, typeFact);
            // Perform further inference
            typeInference(typeFact);
        }
    }

    /**
     * Produce and process a type derivation from this iteration.
     * TODO: how to implement rules that would produce "literal rdf:type ?x"
     * @param type      The type itself
     * @param rule      The generating rule
     * @param source    The source of the derivation
     */
    void processType(Resource type, OwlRule rule, Fact source) {
        processType(triple(node, RDF.TYPE, type, rule, source));
    }

    /**
     * Infer additional information from a type assertion.
     */
    void typeInference(Fact typeFact) {
        Resource type = (Resource) typeFact.getObject();
        OwlClass c = schema.getClass(type);
        // RL rule cls-nothing2: Inconsistent if type owl:Nothing
        if (OWL.NOTHING.equals(type) && frontier(typeFact)) {
            // Skip if this isn't a new fact
            collectInconsistency(inconsistency(OwlRule.CLS_NOTHING2, typeFact));
        }
        // RL rule cax-dw: type shouldn't be disjointWith a previous type
        Set<Resource> disjoint = c.getDisjointClasses();
        disjoint.retainAll(knownTypes.keySet());
        for (Resource other : disjoint) {
            Fact otherTypeFact = knownTypes.get(other);
            Derivation inc = inconsistency(OwlRule.CAX_DW, typeFact);
            inc.addSource(otherTypeFact);
            collectInconsistency(inc);
        }
        // RL rule cls-com: type shouldn't be complementOf a previous type
        Set<Resource> complementary = c.getComplementaryClasses();
        complementary.retainAll(knownTypes.keySet());
        for (Resource other : complementary) {
            Fact otherTypeFact = knownTypes.get(other);
            Derivation inc = inconsistency(OwlRule.CLS_COM, typeFact);
            inc.addSource(otherTypeFact);
            collectInconsistency(inc);
        }
        // RL rule cax-sco: subClassOf semantics (derive superclasses)
        if (!typeFact.hasRule(OwlRule.CAX_SCO)
            && frontier(typeFact)) {
            // Skip if typeFact itself came from this rule, and/or if typeFact
            // generally shouldn't be the sole source of new information
            for (Resource supertype : c.getSuperClasses()) {
                // If the supertype isn't trivial, assert it
                if (!supertype.equals(type)
                        && !(supertype.equals(OWL.THING))) {
                    processType(supertype, OwlRule.CAX_SCO, typeFact);
                }
            }
        }
        // Apply property restriction rules:
        for (URI prop : c.getOnProperty()) {
            // RL rule cls-hv1: if type is an owl:hasValue restriction
            for (Value val : c.hasValue()) {
                collect(triple(node, prop, val, OwlRule.CLS_HV1, typeFact));
            }
        }
        // Derive any facts whose explicit condition is this type assignment
        if (possibleInferences.containsKey(type)) {
            for (Fact fact : possibleInferences.get(type)) {
                Fact join = fact.clone();
                join.addSource(typeFact);
                collect(join);
            }
        }
        // Derive any inconsistencies whose explicit condition is this type
        if (possibleInconsistencies.containsKey(type)) {
            for (Derivation d : possibleInconsistencies.get(type)) {
                Derivation inc = d.clone();
                inc.addSource(typeFact);
                collectInconsistency(inc);
            }
        }
    }

    /**
     * Assert an arbitrary fact if and when this node is determined to have
     * a particular type. Facilitates join rules specifically concerning type.
     */
    void onType(Resource type, Fact fact) {
        if (!possibleInferences.containsKey(type)) {
            possibleInferences.put(type, new LinkedList<Fact>());
        }
        possibleInferences.get(type).add(fact);
        // If we already know the type, assert the fact right away.
        if (knownTypes.containsKey(type)) {
            Fact join = fact.clone();
            join.addSource(knownTypes.get(type));
            collect(join);
        }
    }

    /**
     * Assert an inconsistency if and when this node is determined to have
     * a particular type. Facilitates join rules specifically concerning type.
     */
    void inconsistentOnType(Resource type, Derivation fact) {
        if (!possibleInconsistencies.containsKey(type)) {
            possibleInconsistencies.put(type, new LinkedList<Derivation>());
        }
        possibleInconsistencies.get(type).add(fact);
        // If we already know the type, assert the fact right away.
        if (knownTypes.containsKey(type)) {
            Derivation d = fact.clone();
            d.addSource(knownTypes.get(type));
            collectInconsistency(d);
        }
    }

    /**
     * Collect all the type knowledge into the output, if it represents new
     * information.
     */
    void collectTypes() {
        for (Resource type : knownTypes.keySet()) {
            collect(knownTypes.get(type));
        }
    }

    /**
     * Get info about types derived and potential inferences.
     */
    @Override
    public String getDiagnostics() {
        int total = 0;
        int incTotal = 0;
        for (Resource uri : possibleInferences.keySet()) {
            total += possibleInferences.get(uri).size();
        }
        for (Resource uri : possibleInconsistencies.keySet()) {
            incTotal += possibleInconsistencies.get(uri).size();
        }
        StringBuilder sb = new StringBuilder();
        sb.append(knownTypes.size()).append(" total types known\n");
        sb.append("Watching for ").append(possibleInferences.size());
        sb.append(" distinct types to trigger any of ").append(total);
        sb.append(" possible inferences");
        sb.append("Watching for ").append(possibleInconsistencies.size());
        sb.append(" distinct types to trigger any of ").append(incTotal);
        sb.append(" possible inconsistencies");
        return sb.toString();
    }

    /**
     * Get the total number of input facts cached.
     */
    @Override
    public int getNumStored() {
        int total = knownTypes.size();
        for (List<Fact> l : possibleInferences.values()) {
            total += l.size();
        }
        for (List<Derivation> l : possibleInconsistencies.values()) {
            total += l.size();
        }
        return total;
    }
}
