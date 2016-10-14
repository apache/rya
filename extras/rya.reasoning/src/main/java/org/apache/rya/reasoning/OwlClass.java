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

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.OWL;

/**
 * Contains all the schema information we might need about a class.
 * <p>
 * Rules applied dynamically by getter methods:
 * <ul>
 * <li>
 * scm-cls  States that every class is its own subclass and equivalent class,
 *          and is a subclass of owl:Thing. These facts are included by
 *          getSuperClasses and getEquivalentClasses. (It also states that
 *          owl:Nothing is a subclass of every class).
 * <li>
 * scm-eqc1 States that if two classes are equivalent, they are also
 *          subclasses. Equivalence is represented using subclass relations
 *          internally.
 * <li>
 * scm-eqc2 States that if two classes are each other's subclasses, they are
 *          also equivalent classes.
 * </ul>
 * Rules applied by explicitly calling methods once all data has been read in:
 * <ul>
 * <p>
 * <li>
 * scm-sco  States that subClassOf is transitive. Computed by
 *          computeSuperClasses().
 * </ul>
 */
public class OwlClass implements Serializable {
    private static final long serialVersionUID = 1L;
    private Resource uri;

    // Relations to other classes:
    private Set<OwlClass> superClasses = new HashSet<>();
    private Set<OwlClass> disjointClasses = new HashSet<>();
    private Set<OwlClass> complementaryClasses = new HashSet<>();

    // If this class is referenced by property restrictions:
    private Set<OwlClass> svfRestrictions = new HashSet<>();
    private Set<OwlClass> avfRestrictions = new HashSet<>();
    private Set<OwlClass> qcRestrictions = new HashSet<>();

    // If this is a property restriction, Which propert(y/ies) does it apply to?
    Set<OwlProperty> properties = new HashSet<OwlProperty>();
    // These represent the semantics of the restriction:
    Set<OwlClass> svfClasses = new HashSet<>();
    Set<OwlClass> avfClasses = new HashSet<>();
    Set<OwlClass> qcClasses = new HashSet<>();
    Set<Value> values = new HashSet<>();
    int maxCardinality = -1;
    int maxQualifiedCardinality = -1;

    OwlClass(Resource uri) {
        this.uri = uri;
    }

    public Resource getURI() { return uri; }
    public void setURI(Resource uri) { this.uri = uri; }

    /**
     * Add a superclass
     */
    boolean addSuperClass(OwlClass c) {
        return superClasses.add(c);
    }

    /**
     * Add an equivalent class
     * RL rule scm-eqc1: Store equivalence as mutual subClassOf relations
     */
    boolean addEquivalentClass(OwlClass c) {
        boolean change = superClasses.add(c);
        change = c.superClasses.add(this) || change;
        return change;
    }
    /**
     * Add a disjoint class
     */
    boolean addDisjoint(OwlClass c) { return disjointClasses.add(c); }
    /**
     * Add a complementary class
     */
    boolean addComplement(OwlClass c) { return complementaryClasses.add(c); }
    /**
     * Add a someValuesFrom restriction on this class
     */
    boolean addSvfRestriction(OwlClass r) { return svfRestrictions.add(r); }
    /**
     * Add an allValuesFrom restriction on this class
     */
    boolean addAvfRestriction(OwlClass r) { return avfRestrictions.add(r); }
    /**
     * Add a qualified cardinality restriction on this class
     */
    boolean addQCRestriction(OwlClass r) { return qcRestrictions.add(r); }

    /**
     * Apply RL rule scm-sco: subClassOf transitivity.
     * Follows subClassOf chains to compute all the ancestor classes. Assumes
     * the hierarchy is small enough that a simple BFS is fine.
     * @return  Whether new information was discovered
     */
    boolean computeSuperClasses() {
        Set<OwlClass> ancestors = new HashSet<OwlClass>();
        Set<OwlClass> frontier = new HashSet<OwlClass>(superClasses);
        while (!frontier.isEmpty()) {
            Set<OwlClass> next = new HashSet<OwlClass>();
            for (OwlClass ancestor : frontier) {
                // This node is an ancestor; it's parents should be explored next
                ancestors.add(ancestor);
                next.addAll(ancestor.superClasses);
            }
            // Don't revisit nodes
            next.removeAll(ancestors);
            frontier = next;
        }
        boolean newInfo = !ancestors.equals(superClasses);
        superClasses = ancestors;
        return newInfo;
    }

    /**
     * Add all of this class' ancestors to the domain of a given property.
     */
    void inheritDomains(OwlProperty prop) {
        for (OwlClass superclass : this.superClasses) {
            prop.addDomain(superclass);
        }
    }

    /**
     * Add all of this class' ancestors to the range of a given property.
     */
    void inheritRanges(OwlProperty prop) {
        for (OwlClass superclass : this.superClasses) {
            prop.addRange(superclass);
        }
    }

    /**
     * Add "onProperty p" information, and tell p to point back here.
     */
    public boolean addProperty(OwlProperty property) {
        property.addRestriction(this);
        return properties.add(property);
    }

    /**
     * Add "someValuesFrom c" information, and tell c to point back here.
     */
    public boolean addSvf(OwlClass targetClass) {
        targetClass.addSvfRestriction(this);
        return svfClasses.add(targetClass);
    }

    /**
     * Add "allValuesFrom c" information, and tell c to point back here.
     */
    public boolean addAvf(OwlClass targetClass) {
        targetClass.addAvfRestriction(this);
        return avfClasses.add(targetClass);
    }

    /**
     * Add "onClass c" information, and tell c to point back here.
     */
    public boolean addClass(OwlClass targetClass) {
        targetClass.addQCRestriction(this);
        return qcClasses.add(targetClass);
    }

    /**
     * Add "hasValue v" information.
     */
    public boolean addValue(Value v) {
        return values.add(v);
    }

    /**
     * Set a maxCardinality.
     */
    public boolean setMaxCardinality(Value v) {
        int mc = Integer.parseInt(v.stringValue());
        if (maxCardinality < 0 || mc < maxCardinality) {
            maxCardinality = mc;
            return true;
        }
        return false;
    }

    /**
     * Set maxQualifiedCardinality.
     */
    public boolean setMaxQualifiedCardinality(Value v) {
        int mqc = Integer.parseInt(v.stringValue());
        if (maxQualifiedCardinality < 0 || mqc < maxQualifiedCardinality) {
            maxQualifiedCardinality = mqc;
            return true;
        }
        return false;
    }


    /**
     * Get all the superclasses of this subclass. Includes self and owl:Thing.
     */
    public Set<Resource> getSuperClasses() {
        Set<Resource> ancestors = new HashSet<>();
        for (OwlClass ancestor : superClasses) {
            ancestors.add(ancestor.uri);
        }
        // RL rule scm-cls: Every class is a subclass of itself and owl:Thing
        ancestors.add(this.uri);
        ancestors.add(OWL.THING);
        return ancestors;
    }

    /**
     * Get all the equivalent classes, meaning those who are both subclasses
     * and superclasses of this one.
     */
    public Set<Resource> getEquivalentClasses() {
        // RL rule scm-eqc2: mutual subClassOf relations imply equivalentClass
        Set<Resource> equivalents = new HashSet<>();
        for (OwlClass other : superClasses) {
            if (other.superClasses.contains(this)) {
                equivalents.add(other.uri);
            }
        }
        // RL rule scm-cls: Every class is its own equivalent
        equivalents.add(this.uri);
        return equivalents;
    }

    /**
     * Get all classes declared disjoint with this one.
     */
    public Set<Resource> getDisjointClasses() {
        Set<Resource> disjoint = new HashSet<>();
        for (OwlClass other : disjointClasses) {
            disjoint.add(other.uri);
        }
        return disjoint;
    }

    /**
     * Get all classes declared complement to a specific class.
     */
    public Set<Resource> getComplementaryClasses() {
        Set<Resource> complements = new HashSet<>();
        for (OwlClass other : complementaryClasses) {
            complements.add(other.uri);
        }
        return complements;
    }

    /**
     * Get all someValuesFrom restrictions on this class
     */
    public Set<Resource> getSvfRestrictions() {
        Set<Resource> restrictions = new HashSet<>();
        for (OwlClass r : svfRestrictions) {
            restrictions.add(r.getURI());
        }
        return restrictions;
    }

    /**
     * Get all allValuesFrom restrictions on this class
     */
    public Set<Resource> getAvfRestrictions() {
        Set<Resource> restrictions = new HashSet<>();
        for (OwlClass r : avfRestrictions) {
            restrictions.add(r.getURI());
        }
        return restrictions;
    }

    /**
     * Get all onClass (qualified cardinality) restrictions on this class
     */
    public Set<Resource> getQCRestrictions() {
        Set<Resource> restrictions = new HashSet<>();
        for (OwlClass r : qcRestrictions) {
            restrictions.add(r.getURI());
        }
        return restrictions;
    }

    /**
     * Get the onProperty relation(s) for this property restriction.
     */
    public Set<URI> getOnProperty() {
        Set<URI> onp = new HashSet<>();
        for (OwlProperty prop : properties) {
            onp.add(prop.getURI());
        }
        return onp;
    }

    /**
     * Get all someValuesFrom relations for this property restriction.
     */
    public Set<Resource> someValuesFrom() {
        Set<Resource> targets = new HashSet<>();
        for (OwlClass c : svfClasses) {
            targets.add(c.getURI());
        }
        return targets;
    }

    /**
     * Get all allValuesFrom relations for this property restriction.
     */
    public Set<Resource> allValuesFrom() {
        Set<Resource> targets = new HashSet<>();
        for (OwlClass c : avfClasses) {
            targets.add(c.getURI());
        }
        return targets;
    }

    /**
     * Get all onClass relations for this property restriction.
     */
    public Set<Resource> onClass() {
        Set<Resource> targets = new HashSet<>();
        for (OwlClass c : qcClasses) {
            targets.add(c.getURI());
        }
        return targets;
    }

    /**
     * Get all hasValue relations for this property restriction.
     */
    public Set<Value> hasValue() { return new HashSet<Value>(values); }

    /**
     * Get the maxCardinality. Negative means it was not specified, otherwise
     * returns the lowest value ever given.
     */
    public int getMaxCardinality() { return maxCardinality; }

    /**
     * Get the maxQualifiedCardinality. Negative means it was not specified,
     * otherwise returns the lowest value ever given.
     */
    public int getMaxQualifiedCardinality() { return maxQualifiedCardinality; }

    /**
     * Compares someValuesFrom target classes, and returns true if this one's
     * is a subclass of the other's.
     */
    boolean svfSubClass(OwlClass other) {
        for (OwlClass svfClass : this.svfClasses) {
            Set<Resource> intersection = svfClass.getSuperClasses();
            intersection.retainAll(other.someValuesFrom());
            if (!intersection.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Compares someValuesFrom target classes, and returns true if this one's
     * is a subclass of the other's.
     */
    boolean avfSubClass(OwlClass other) {
        for (OwlClass avfClass : this.avfClasses) {
            Set<Resource> intersection = avfClass.getSuperClasses();
            intersection.retainAll(other.allValuesFrom());
            if (!intersection.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Compares onProperty properties, and returns true if this one's
     * is a subproperty of the other's.
     */
    boolean onSubProperty(OwlClass other) {
        Set<URI> otherProp = other.getOnProperty();
        for (OwlProperty prop : this.properties) {
            Set<URI> intersection = prop.getSuperProperties();
            intersection.retainAll(otherProp);
            if (!intersection.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Apply property restriction rules that involve the same class/value
     * and properties related by subPropertyOf.
     * RL rule scm-hv: [subprop hasValue] subClassOf [superprop hasValue]
     * RL rule scm-svf2: [subprop someValuesFrom x] subClassOf [superprop someValuesFrom x]
     * RL rule scm-avf2: [superprop allValuesFrom x] subClassOf [subprop allValuesFrom x]
     * Doesn't check subproperty transitivity.
     * @return  Whether any information was generated
     */
    boolean compareRestrictions(OwlClass other) {
        boolean newInfo = false;
        // These rules apply iff the restrictions are on subprop and superprop
        if (this.onSubProperty(other)) {
            // scm-hv
            Set<Value> sharedValues = new HashSet<>(this.values);
            sharedValues.retainAll(other.values);
            if (!sharedValues.isEmpty()) {
                newInfo = this.addSuperClass(other) || newInfo;
            }
            else {
                // scm-svf2
                // (same result as scm-hv, no need to derive twice)
                Set<OwlClass> sharedSvf = new HashSet<>(this.svfClasses);
                sharedSvf.retainAll(other.svfClasses);
                if (!sharedSvf.isEmpty()) {
                    newInfo = this.addSuperClass(other) || newInfo;
                }
            }
            // scm-avf2
            // (applies in the other direction)
            Set<OwlClass> sharedAvf = new HashSet<>(this.avfClasses);
            sharedAvf.retainAll(other.avfClasses);
            if (!sharedAvf.isEmpty()) {
                newInfo = other.addSuperClass(this) || newInfo;
            }
        }
        return newInfo;
    }

    /**
     * Get all superclasses of the target(s) of any someValuesFrom relations
     * this class has, including the target class itself and any transitive
     * superclasses.
     */
    Set<Resource> getSvfSuperClasses() {
        Set<Resource> svfSuperClasses = new HashSet<>();
        for (OwlClass svfClass : svfClasses) {
            svfSuperClasses.addAll(svfClass.getSuperClasses());
        }
        return svfSuperClasses;
    }

    /**
     * Get all superclasses of the target(s) of any allValuesFrom relations
     * this class has, including the target class itself and any transitive
     * superclasses.
     */
    Set<Resource> getAvfSuperClasses() {
        Set<Resource> avfSuperClasses = new HashSet<>();
        for (OwlClass avfClass : avfClasses) {
            avfSuperClasses.addAll(avfClass.getSuperClasses());
        }
        return avfSuperClasses;
    }
}
