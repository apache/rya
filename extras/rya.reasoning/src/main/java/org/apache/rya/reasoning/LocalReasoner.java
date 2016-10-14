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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;

/**
 * Perform reasoning with respect to a particular node, given a global schema.
 * Assumes that incoming input triples will be provided first (i.e. triples
 * where this node is the object) in order to perform some joins.
 * <p>
 * Rules implemented so far:
 * <p>
 * Simple rules implemented here:
 * <ul>
 * <li>prp-dom:         domain: infer subject's type from predicate's domain
 * <li>prp-rng:         range: infer object's type from predicate's range
 * <li>prp-irp:         If p is irreflexive, (x p x) is inconsistent
 * <li>prp-symp:        If p is symmetric, (x p y) implies (y p x)
 * <li>prp-spo1:        subPropertyOf semantics: inherit superproperties
 * <li>prp-inv1:        If p1 inverseOf p2, (x p1 y) implies (y p2 x)
 * <li>prp-inv2:        If p1 inverseOf p2, (x p2 y) implies (y p1 x)
 * <li>cls-svf2:        If [x someValuesFrom owl:Thing onProperty p],
 *                          (u p v) implies (u type x)
 * <li>cls-hv2:         If [x hasValue v onProperty p], (u p v) implies
 *                          (u type x)
 * </ul>
 * <p>
 * Join rules implemented here:
 * <ul>
 * <li>prp-asyp:        If p is asymmetric, (x p y) and (y p x) is inconsistent
 * <li>prp-trp:         If p is transitive, (x p y) and (y p z) implies (x p z)
 * <li>prp-pdw:         If p is disjoint with q, (x p y) and (x q y) is
 *                          inconsistent
 * <li>cls-svf1:        If [x someValuesFrom y onProperty p],
 *                          (u p v) and (v type y) imply (u type x)
 * <li>cls-avf:         If [x allValuesFrom y onProperty p],
 *                          (u p v) and (u type x) imply (v type y)
 * <li>cls-maxc1:       (x p y) is inconsistent if p has maxCardinality 0
 * <li>cls-maxqc2:      ...or if p has maxQualifiedCardinality 0 on owl:Thing
 * </ul>
 * <p>
 * Simple and join rules handled in {@link TypeReasoner}:
 * <ul>
 * <li>cax-sco:         subClassOf semantics: inherit supertypes
 * <li>cls-hv1:         If [x hasValue v onProperty p], (u type x) implies
 *                          (u p v)
 * <li>cls-nothing2:    type owl:Nothing is automatically inconsistent
 * <li>cls-com:         If c1 has complement c2, having both types is
 *                          inconsistent
 * <li>cax-dw:          If c1 and c2 are disjoint, having both types is
 *                          inconsistent
 * </ul>
 */
public class LocalReasoner extends AbstractReasoner {
    public enum Relevance {
        NONE, SUBJECT, OBJECT, BOTH;
        static Relevance get(boolean s, boolean o) {
            if (s && o) return BOTH;
            else if (s) return SUBJECT;
            else if (o) return OBJECT;
            else return NONE;
        }
        public boolean subject() { return this == SUBJECT || this == BOTH; }
        public boolean object() { return this == OBJECT || this == BOTH; }
    }

    /**
     * Determine whether a fact is a triple which might be used by a local
     * reasoner for its subject and/or object.
     * @param   fact  Fact to be evaluated
     * @param   schema  Global schema
     * @return  Relevance to subject and/or object. Relevance means that it's a
     *          triple that *could* be used in reasoning. It may only be useful
     *          when combined with other facts, which may or may not exist
     *          somewhere, so this doesn't guarantee that information will be
     *          derived.
     */
    public static Relevance relevantFact(Fact fact, Schema schema) {
        // If this is schema information, we know it's already
        // contained in the schema object.
        if (Schema.isSchemaTriple(fact.getTriple())) {
            return Relevance.NONE;
        }
        // Otherwise, consider the semantics of the statement:
        Resource subject = fact.getSubject();
        URI predURI = fact.getPredicate();
        Value object = fact.getObject();
        boolean relevantToSubject = false;
        boolean relevantToObject = false;
        // Literals don't get reasoners, so determine whether object is a uri:
        boolean literalObject = object instanceof Literal;

        // Type statements could be relevant to the subject, if the schema gives
        // them any meaning:
        if (predURI.equals(RDF.TYPE)) {
            // Assume the object is a valid URI
            Resource typeURI = (Resource) fact.getObject();
            if (typeURI.equals(OWL.NOTHING)
                || schema.hasClass(typeURI)) {
                relevantToSubject = true;
            }
        }

        // If the schema knows about the property:
        if (schema.hasProperty(predURI)) {
            OwlProperty prop = schema.getProperty(predURI);

            // Relevant to both:
                    // Any statement with an asymmetric property
            if (prop.isAsymmetric()
                    // Any statement with a transitive property
                    || prop.isTransitive()
                    // Statements involving restricted properties
                    || !prop.getRestrictions().isEmpty()) {
                relevantToSubject = true;
                relevantToObject = !literalObject;
            }

            // Relevant to subject:
            if (!relevantToSubject && // skip these checks if it already is
                    // Any statement whose property has a domain.
                    (!prop.getDomain().isEmpty()
                    // Choose to apply superproperties here
                    // (every property is its own superproperty; ignore that)
                    || prop.getSuperProperties().size() > 1
                    // Choose to apply disjoint properties here
                    || !prop.getDisjointProperties().isEmpty())) {
                relevantToSubject = true;
            }

            // Relevant to object if the object is not a literal and one other
            // condition matches:
            if (!literalObject && !relevantToObject &&
                    // Any statement whose property has a defined range
                    (!prop.getRange().isEmpty()
                    // Choose to apply inverse rule in the object's reasoner
                    || !prop.getInverseProperties().isEmpty()
                    // Choose to apply symmetry in the object's reasoner
                    || prop.isSymmetric()
                    // Choose to check irreflexivity in the object's reasoner
                    || prop.isIrreflexive() && subject.equals(object))) {
                relevantToObject = true;
            }
        }
        return Relevance.get(relevantToSubject, relevantToObject);
    }

    /**
     * Determine whether a fact is a triple which might be used in some join
     * rule for its subject and/or object.
     */
    public static Relevance relevantJoinRule(Fact fact, Schema schema) {
        // If this is schema information, we know it's already
        // contained in the schema object.
        if (Schema.isSchemaTriple(fact.getTriple())) {
            return Relevance.NONE;
        }
        // Otherwise, consider the semantics of the statement:
        URI predURI = fact.getPredicate();
        Value object = fact.getObject();
        boolean relevantToSubject = false;
        boolean relevantToObject = false;
        // Literals don't get reasoners, so determine whether object is a uri:
        boolean literalObject = object instanceof Literal;

        // Type statements can be joined if...
        if (predURI.equals(RDF.TYPE)) {
            Resource typeURI = (Resource) fact.getObject();
            if (schema.hasClass(typeURI)) {
                OwlClass c = schema.getClass(typeURI);
                // 1. the type is a property restriction
                if (!c.getOnProperty().isEmpty()
                // 2. the type is relevant to a property restriction
                    || !c.getSvfRestrictions().isEmpty()
                    || !c.getAvfRestrictions().isEmpty()
                    || !c.getQCRestrictions().isEmpty()
                // 3. the type has complementary/disjoint types
                    || !c.getDisjointClasses().isEmpty()
                    || !c.getComplementaryClasses().isEmpty()) {
                    relevantToSubject = true;
                }
            }
        }

        // If the schema knows about the property:
        if (schema.hasProperty(predURI)) {
            OwlProperty prop = schema.getProperty(predURI);
            // transitivity: relevant to both
            if (prop.isTransitive()) {
                relevantToSubject = true;
                relevantToObject = !literalObject;
            }
            else {
                // disjoint properties: relevant to subject
                if (!prop.getDisjointProperties().isEmpty()) {
                    relevantToSubject = true;
                }
                // Property restrictions: possibly relevant to either
                for (Resource rURI : prop.getRestrictions()) {
                    OwlClass r = schema.getClass(rURI);
                    // allValuesFrom requires a join on the subject
                    // (if <subject type rURI>, infer object's type)
                    if (!r.allValuesFrom().isEmpty()) {
                        relevantToSubject = true;
                    }
                    // someValuesFrom requires a join on the object
                    // (if the object is the appropriate type, infer rURI)
                    // max cardinality requires a join on the subject
                    if (!literalObject &&
                        (r.getMaxCardinality() >= 0
                        || r.getMaxQualifiedCardinality() >= 0
                        || !r.someValuesFrom().isEmpty())) {
                        relevantToObject = true;
                    }
                    if (relevantToSubject
                        && (relevantToObject || literalObject)) {
                        break;
                    }
                }
            }
        }
        return Relevance.get(relevantToSubject, relevantToObject);
    }

    /**
     * Use the semantics of a fact to determine whether it could be relevant
     * to future reasoners, or whether we should assume we've extracted all
     * the information implied by the fact during this iteration. Considers
     * semantics but not age.
     * @return true if this fact might still be used later.
     */
    boolean relevantToFuture(Fact fact) {
        // If it's a join rule, it needs to be kept no matter what
        if (relevantJoinRule(fact, schema) != Relevance.NONE) {
            return true;
        }
        // Otherwise, it can be skipped under certain circumstances.
        Relevance general = relevantFact(fact, schema);
        Resource s = fact.getSubject();
        Value o = fact.getObject();
        // Exception: if subject==object, recursive derivation is limited, so
        // we can't make assumptions about what's already been done.
        if (!s.equals(o)) {
            // Otherwise, if this is a reasoner for the subject, and the fact
            // is only relevant to the subject, we can assume this reasoner
            // did all the reasoning we needed to.
            if (general == Relevance.SUBJECT && node.equals(s)) {
                return false;
            }
            // Same reasoning for the object:
            if (general == Relevance.OBJECT && node.equals(o)) {
                return false;
            }
        }
        // If we can't skip it, return true if it's ever relevant
        return general != Relevance.NONE;
    }

    // Many rules derive types; keep track of them with a TypeReasoner
    TypeReasoner types;

    // Keep track of statements whose properties might make them relevant later
    Map<URI, List<Fact>> transitiveIncoming = new HashMap<>();
    Map<URI, List<Fact>> asymmetricIncoming = new HashMap<>();
    Map<URI, List<Fact>> disjointOutgoing = new HashMap<>();

    // Only combine transitive paths of a certain size, based on the current
    // iteration, to avoid duplicate derivations and unnecessary memory use.
    int minTransitiveLeft;
    int minTransitiveRight;

    /**
     * Constructor.
     * @param   node    Conduct reasoning about/around this node
     * @param   schema  Global schema (class/property) information
     * @param   t       Current iteration; any new facts will be generated with
     *                  this number
     * @param   tSchema Iteration of latest schema update (0 if original
     *                  schema is unchanged)
     */
    public LocalReasoner(Resource node, Schema schema, int t, int tSchema) {
        super(node, schema, t, tSchema);
        types = new TypeReasoner(node, schema, t, tSchema);
        // "Smart TC": combine incoming paths of length 2^(n-1) with outgoing
        // paths of any length.
        int n = t - tSchema; // count iterations since any schema change
        minTransitiveLeft = (int) Math.pow(2, n-1);
        minTransitiveRight = 1;
    }

    /**
     * Read in a fact involving this node and make any inferences we can.
     * Assumes that incoming triples are received before outgoing triples.
     * Recursively call processFact on new triples until nothing else is
     * derived.
     * @param   fact  Contains a triple assumed to be relevant to this reasoner
     */
    public void processFact(Fact fact) {
        Resource subject = fact.getSubject();
        URI pred = fact.getPredicate();
        Value object = fact.getObject();
        // Whether this is a recursive call on a fact that's just been inferred
        boolean recursive = fact.getIteration() == currentIteration;
        // Figure out what kind of edge this is with respect to this node
        boolean incoming = object.equals(node);
        boolean outgoing = subject.equals(node);
        // Avoid some derivation chains on recursive calls to avoid cycles
        boolean skipReflexive = incoming && outgoing && recursive;
        // Perform reasoning (incoming before outgoing, so reflexive edges are
        // handled in the right order)
        if (incoming && !skipReflexive) {
            processIncoming(fact);
        }
        if (outgoing) {
            if (pred.equals(RDF.TYPE)) {
                types.processType(fact);
            }
            else {
                processOutgoing(fact);
            }
        }
        // If newly-learned facts cause further derivations, apply them recursively
        Set<Fact> resultsSoFar = getFacts();
        for (Fact newFact : resultsSoFar) {
            processFact(newFact);
        }
        newFacts.addAll(resultsSoFar);
    }

    /**
     * Process a triple in which this node is the subject.
     */
    private void processOutgoing(Fact fact) {
        URI predURI = fact.getPredicate();
        Value object = fact.getObject();
        OwlProperty prop = schema.getProperty(predURI);
        Set<Resource> restrictions = prop.getRestrictions();
        Set<URI> disjointProps = prop.getDisjointProperties();
        // RL rule prp-dom: Apply domain(s), if appropriate
        for (Resource type : prop.getDomain()) {
            types.processType(type, OwlRule.PRP_DOM, fact);
        }
        // RL rule prp-spo1: assert superproperties
        // Assume we have the full property hierarchy in the schema, so that
        // if the input fact was derived using this rule, we must have  gotten
        // all the superproperties and don't need to apply them again.
        if (!fact.hasRule(OwlRule.PRP_SPO1)) {
            for (URI superProp : prop.getSuperProperties()) {
                // (everything is its own superproperty)
                if (superProp.equals(predURI)) {
                    continue;
                }
                collect(triple(node, superProp, object, OwlRule.PRP_SPO1, fact));
            }
        }
        // RL rule prp-pdw: Check if this conflicts with any disjoint properties
        if (!disjointProps.isEmpty()) {
            for (URI disjointProp : disjointProps) {
                if (disjointOutgoing.containsKey(disjointProp)) {
                    for (Fact other : disjointOutgoing.get(disjointProp)) {
                        if (object.equals(other.getObject())) {
                            Derivation pdwFact = inconsistency(OwlRule.PRP_PDW, fact);
                            pdwFact.addSource(other);
                            collectInconsistency(pdwFact);
                        }
                    }
                }
            }
            if (!disjointOutgoing.containsKey(predURI)) {
                disjointOutgoing.put(predURI, new LinkedList<Fact>());
            }
            disjointOutgoing.get(predURI).add(fact);
        }
        // Property restriction rules:
        for (Resource rNode : restrictions) {
            OwlClass restriction = schema.getClass(rNode);
            // RL rule cls-svf2: if (?x owl:someValuesFrom owl:Thing)
            if (restriction.someValuesFrom().contains(OWL.THING)) {
                // If there are any property restrictions stating that class
                // x is equivalent to having someValuesFrom owl:Thing for this
                // property, then this node is a member of type x
                types.processType(rNode, OwlRule.CLS_SVF2, fact);
            }
            // RL rule cls-hv2: if (?x owl:hasValue <object>)
            if (restriction.hasValue().contains(object)) {
                //... then node (subject) satisfies/belongs to x
                types.processType(rNode, OwlRule.CLS_HV2, fact);
            }
            // RL rule cls-avf: if x=[allValuesFrom ?c onProperty ?p]:
            for (Resource c : restriction.allValuesFrom()) {
                // If/when we learn this node is supposed to satisfy this
                // restriction, and if object is a resource, assert
                // (object type c).
                if (object instanceof Resource) {
                    types.onType(rNode, triple((Resource) object, RDF.TYPE,
                        c, OwlRule.CLS_AVF, fact));
                }
            }
            // RL rule cls-maxc1: if x=[maxCardinality 0], subject can't be x
            if (restriction.getMaxCardinality() == 0) {
                types.inconsistentOnType(rNode,
                    inconsistency(OwlRule.CLS_MAXC1, fact));
            }
            // RL rule cls-maxqc2: x=[maxQualifiedCardinality 0 on owl:Thing]
            // (same as maxCardinality 0)
            if (restriction.getMaxQualifiedCardinality() == 0
                && restriction.onClass().contains(OWL.THING)) {
                types.inconsistentOnType(rNode,
                    inconsistency(OwlRule.CLS_MAXQC2, fact));
            }
        }
        // RL rule prp-trp (part 1/2): Apply against incoming statements
        // with the same predicate (skip if this node is both the subject and
        // object) .
        // Assumes that input is sorted with incoming coming first, so we don't
        // need to store this triple after joining.
        if (prop.isTransitive() && !object.equals(node)
            && checkTransitivityOutgoing(fact)) {
            if (transitiveIncoming.containsKey(predURI)) {
                for (Fact other : transitiveIncoming.get(predURI)) {
                    Resource otherSubject = other.getSubject();
                    Fact trpFact = triple(otherSubject, predURI, object,
                        OwlRule.PRP_TRP, fact);
                    trpFact.addSource(other);
                    collect(trpFact);
                }
            }
        }
        // RL rule prp-asyp (part 2/2): Check against incoming statements with
        // the same predicate. Don't store this one since we assume input is
        // sorted by the direction of the edge.
        if (prop.isAsymmetric() && asymmetricIncoming.containsKey(predURI)) {
            for (Fact other : asymmetricIncoming.get(predURI)) {
                if (object.equals(other.getSubject())) {
                    Derivation asypFact = inconsistency(OwlRule.PRP_ASYP, fact);
                    asypFact.addSource(other);
                    collectInconsistency(asypFact);
                }
            }
        }
    }

    /**
     * Process a triple in which this node is the object.
     */
    private void processIncoming(Fact fact) {
        Resource subject = fact.getSubject();
        URI predURI = fact.getPredicate();
        OwlProperty prop = schema.getProperty(predURI);
        // RL rule prp-rng: Apply range(s), if appropriate
        for (Resource type : prop.getRange()) {
            types.processType(type, OwlRule.PRP_RNG, fact);
        }
        // RL rules prp-inv1, prp-inv2: assert any inverse properties
        for (URI inverseProp : prop.getInverseProperties()) {
            collect(triple(node, inverseProp, subject, OwlRule.PRP_INV, fact));
        }
        // RL rule prp-symp: Assert the symmetric statement if appropriate
        if (prop.isSymmetric()
            && !fact.hasRule(OwlRule.PRP_SYMP)
            && !subject.equals(node)) {
            collect(triple(node, predURI, subject, OwlRule.PRP_SYMP, fact));
        }
        // RL rule prp-irp: (x p x) is inconsistent if p is irreflexive
        if (prop.isIrreflexive() && subject.equals(node)) {
            collectInconsistency(inconsistency(OwlRule.PRP_IRP, fact));
        }
        // RL rule prp-trp (part 1/2): We assume triples are sorted with
        // incoming first, so store this triple in case it needs to be joined
        // with any later outgoing triples with the same property.
        if (prop.isTransitive() && !subject.equals(node)
            && checkTransitivityIncoming(fact)) {
            if (!transitiveIncoming.containsKey(predURI)) {
                transitiveIncoming.put(predURI, new LinkedList<Fact>());
            }
            transitiveIncoming.get(predURI).add(fact);
        }
        // RL rule prp-asyp (part 1/2): Store this incoming edge so we can
        // compare later outgoing edges against it. (Assume sorted input.)
        if (prop.isAsymmetric()) {
            if (!asymmetricIncoming.containsKey(predURI)) {
                asymmetricIncoming.put(predURI, new LinkedList<Fact>());
            }
            asymmetricIncoming.get(predURI).add(fact);
        }
        for (Resource rNode : prop.getRestrictions()) {
            OwlClass restriction = schema.getClass(rNode);
            // RL rule cls-svf1: Check for a someValuesFrom restriction
            Set<Resource> valuesFrom = restriction.someValuesFrom();
            // type owl:Thing would be checked by cls-svf2
            valuesFrom.remove(OWL.THING);
            for (Resource commonType : valuesFrom) {
                // If we learn the type, assert the other node's membership in rNode
                types.onType(commonType, triple(subject, RDF.TYPE,
                    rNode, OwlRule.CLS_SVF1, fact));
            }
        }
    }

    /**
     * Collect all the derived type information in a single ResultSet.
     */
    public void getTypes() {
        types.collectTypes();
        newFacts.addAll(types.getFacts());
    }

    /**
     * Determine whether an inconsistency has been found.
     */
    @Override
    public boolean hasInconsistencies() {
        return types.hasInconsistencies() || !inconsistencies.isEmpty();
    }

    /**
     * Get results, including those stored in the TypeReasoner.
     */
    @Override
    public Set<Fact> getFacts() {
        Set<Fact> results = types.getFacts();
        if (hasNewFacts()) {
            results.addAll(newFacts);
            newFacts.clear();
        }
        // If we can mark some facts as not needing to be processed during the
        // next step, do so:
        for (Fact result : results) {
            result.setUseful(relevantToFuture(result));
        }
        return results;
    }

    /**
     * Get inconsistencies, including those stored in the TypeReasoner.
     */
    @Override
    public Set<Derivation> getInconsistencies() {
        Set<Derivation> results = types.getInconsistencies();
        if (hasInconsistencies()) {
            results.addAll(inconsistencies);
            inconsistencies.clear();
        }
        return results;
    }

    /**
     * Get some diagnostic info for logging purposes.
     */
    @Override
    public String getDiagnostics() {
        // Count the different types of triples in memory:
        int sumIncomingAsymmetric = 0;
        int sumOutgoingDisjoint = 0;
        int sumIncomingTransitive = 0;
        for (List<Fact> l : asymmetricIncoming.values()) {
            sumIncomingAsymmetric += l.size();;
        }
        for (List<Fact> l : disjointOutgoing.values()) {
            sumOutgoingDisjoint += l.size();;
        }
        int maxTransitiveSpan = (int) Math.pow(2, currentIteration);
        int[] distribution = new int[maxTransitiveSpan+1];
        for (int i = 0; i <= maxTransitiveSpan; i++) {
            distribution[i] = 0;
        }
        for (List<Fact> l : transitiveIncoming.values()) {
            for (Fact fact : l) {
                sumIncomingTransitive++;
                int x = fact.span();
                if (x > 0 && x <= maxTransitiveSpan) {
                    distribution[x]++;
                }
                else {
                    distribution[0]++;
                }
            }
        }

        // Collect totals:
        StringBuilder sb = new StringBuilder();
        sb.append("Node: ").append(node.stringValue()).append("\n");
        sb.append(newFacts.size()).append(" new triples stored\n");
        sb.append(sumIncomingAsymmetric).append(" stored incoming triples w/ ");
        sb.append(asymmetricIncoming.size()).append(" asymmetric properties\n");
        sb.append(sumOutgoingDisjoint).append(" stored outgoing triples w/ ");
        sb.append(disjointOutgoing.size()).append(" disjoint properties\n");
        sb.append(sumIncomingTransitive).append(" stored incoming triples w/ ");
        sb.append(transitiveIncoming.size()).append(" transitive properties\n");
        sb.append("Span of stored transitive triples:\n");
        for (int i = 0; i <= maxTransitiveSpan; i++) {
            sb.append("    ").append(i).append(": ").append(distribution[i]);
            sb.append("\n");
        }
        sb.append(types.getDiagnostics());
        return sb.toString();
    }

    /**
     * Get the total number of input facts cached.
     */
    @Override
    public int getNumStored() {
        int total = 0;
        for (List<Fact> l : asymmetricIncoming.values()) {
            total += l.size();;
        }
        for (List<Fact> l : disjointOutgoing.values()) {
            total += l.size();;
        }
        for (List<Fact> l : transitiveIncoming.values()) {
            total += l.size();
        }
        return total + types.getNumStored();
    }

    /**
     * Determine whether transitivity should be applied to an incoming triple.
     * Decide based on the distance of the relationship. For any length l, there
     * should be a unique split l1 and l2 such that any connection of length l
     * will be made only by combining a left-hand/incoming connection of length
     * l1 and a right-hand/outgoing connection of length l2.
     */
    boolean checkTransitivityIncoming(Fact fact) {
        return fact.span() >= minTransitiveLeft;
    }

    /**
     * Determine whether transitivity should be applied to an outgoing triple.
     * Decide based on the distance of the relationship. For any length l, there
     * should be a unique split l1 and l2 such that any connection of length l
     * will be made only by combining a left-hand/incoming connection of length
     * l1 and a right-hand/outgoing connection of length l2.
     */
    boolean checkTransitivityOutgoing(Fact fact) {
        return fact.span() >= minTransitiveRight;
    }
}
