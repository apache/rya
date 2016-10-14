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

/**
 * Contains all the schema information we might need about a property.
 *
 * Rules implemented dynamically by getter methods:
 *
 * scm-op   States that every object property is its own subproperty and
 *          equivalent property.
 * scm-dp   States that every datatype property is its own subproperty and
 *          equivalent property.
 * scm-eqp1 States that if two properties are equivalent, they are also
 *          subproperties. Equivalence is represented this way internally.
 * scm-eqp2 States that if two properties are each other's subproperties,
 *          they are also equivalent properties.
 *
 * Rules implemented explicitly by calling methods once all the necessary data
 * has been read in:
 *
 * scm-spo  States that subPropertyOf is transitive; computed by
 *          computeSuperProperties().
 * scm-dom1 States that a property with domain c also has as domain any
 *          of c's superclasses. Computed in inheritDomainRange().
 * scm-dom2 States that a property inherits its superproperties' domains.
 *          Computed in inheritDomainRange().
 * scm-rng1 States that a property with range c also has as domain any
 *          of c's superclasses. Computed in inheritDomainRange().
 * scm-rng2 States that a property inherits its superproperties' ranges.
 *          Computed in inheritDomainRange().
 * svm-svf1 States that property restriction c1 is a subclass of another c2 if
 *          they are someValuesFrom restrictions on the same property where
 *          c1's target class is a subclass of c2's target class.
 *          Computed in compareRestrictions(), depends on subclass info.
 * svm-avf1 States that property restriction c1 is a subclass of another c2 if
 *          they are allValuesFrom restrictions on the same property where
 *          c1's target class is a subclass of c2's target class.
 *          Computed in compareRestrictions(), depends on subclass info.
 */
public class OwlProperty implements Serializable {
    private static final long serialVersionUID = 1L;

    private URI uri;

    // Boolean qualities the property might have
    private boolean transitive = false;
    private boolean symmetric = false;
    private boolean asymmetric = false;
    private boolean functional = false;
    private boolean inverseFunctional = false;
    private boolean irreflexive = false;

    // Relations to other properties
    private Set<OwlProperty> superProperties = new HashSet<OwlProperty>();
    private Set<OwlProperty> disjointProperties = new HashSet<OwlProperty>();
    private Set<OwlProperty> inverseProperties = new HashSet<OwlProperty>();

    // Relations to classes
    private Set<OwlClass> domain = new HashSet<OwlClass>();
    private Set<OwlClass> range = new HashSet<OwlClass>();

    // Restrictions on this property
    private Set<OwlClass> restrictions = new HashSet<OwlClass>();

    OwlProperty(URI uri) {
        this.uri = uri;
    }

    boolean addSuperProperty(OwlProperty p) {
        return superProperties.add(p);
    }

    /**
     * Add an equivalient property
     * RL rule scm-eqp1: Store equivalence as mutual subPropertyOf relations
     */
    boolean addEquivalentProperty(OwlProperty p) {
        boolean change = this.superProperties.add(p);
        change = p.superProperties.add(this) || change;
        return change;
    }

    boolean addDisjoint(OwlProperty p) { return disjointProperties.add(p); }
    boolean addInverse(OwlProperty p) { return inverseProperties.add(p); }
    boolean addDomain(OwlClass c) {
        return domain.add(c);
    }
    boolean addRange(OwlClass c) {
        return range.add(c);
    }
    boolean addRestriction(OwlClass r) { return restrictions.add(r); }

    public void setURI(URI uri) { this.uri = uri; }
    void setTransitive() { transitive = true; }
    void setSymmetric() { symmetric = true; }
    void setAsymmetric() { asymmetric = true; }
    void setFunctional() { functional = true; }
    void setInverseFunctional() { inverseFunctional = true; }
    void setIrreflexive() { irreflexive = true; }

    public URI getURI() { return uri; }
    public boolean isTransitive() { return transitive; }
    public boolean isSymmetric() { return symmetric; }
    public boolean isAsymmetric() { return asymmetric; }
    public boolean isFunctional() { return functional; }
    public boolean isInverseFunctional() { return inverseFunctional; }
    public boolean isIrreflexive() { return irreflexive; }

    /**
     * Apply RL rule scm-spo: subPropertyOf transitivity.
     * Follows subPropertyOf chains to compute all the ancestor properties.
     * Assumes the hierarchy is small enough that a simple BFS is fine.
     * @return  Whether new information was discovered
     */
    boolean computeSuperProperties() {
        Set<OwlProperty> ancestors = new HashSet<OwlProperty>();
        Set<OwlProperty> frontier = new HashSet<OwlProperty>(superProperties);
        while (!frontier.isEmpty()) {
            Set<OwlProperty> next = new HashSet<OwlProperty>();
            for (OwlProperty ancestor : frontier) {
                // This node is an ancestor; it's parents should be explored next
                ancestors.add(ancestor);
                next.addAll(ancestor.superProperties);
            }
            // Don't revisit nodes
            next.removeAll(ancestors);
            frontier = next;
        }
        boolean newInfo = !ancestors.equals(superProperties);
        superProperties = ancestors;
        return newInfo;
    }

    /**
     * Infer domain and range from parents' domains and ranges. Only looks at
     * those superproperties that are explicitly known; computeSuperProperties
     * should generally be called first to get the full closure.
     */
    void inheritDomainRange() {
        //RL rule scm-dom2: (p2 domain c) && (p1 subPropertyOf p2) -> (p1 domain c)
        //RL rule scm-rng2: (p2 range c) && (p1 subPropertyOf p2) -> (p1 range c)
        for (OwlProperty ancestorProperty : superProperties) {
            for (OwlClass c : ancestorProperty.domain) {
                addDomain(c);
            }
            for (OwlClass c : ancestorProperty.range) {
                addRange(c);
            }
        }
        //RL rule scm-dom1: (p domain c1) && (c1 subClassOf c2) -> (p domain c2)
        for (OwlClass domainClass : new HashSet<OwlClass>(this.domain)) {
            domainClass.inheritDomains(this);
        }
        //RL rule scm-rng1: (p range c1) && (c1 subClassOf c2) -> (p range c2)
        for (OwlClass rangeClass : new HashSet<OwlClass>(this.range)) {
            rangeClass.inheritRanges(this);
        }
    }

    /**
     * Get all the superproperties of this subproperty.
     */
    public Set<URI> getSuperProperties() {
        Set<URI> ancestors = new HashSet<>();
        for (OwlProperty ancestor : superProperties) {
            ancestors.add(ancestor.uri);
        }
        // RL rules scm-op & scm-dp: Every property is a subproperty of itself
        ancestors.add(this.uri);
        return ancestors;
    }

    /**
     * Get all the equivalent properties for this property.
     * Apply RL rules scm-op and scm-dp: Every property is its own equivalent.
     */
    public Set<URI> getEquivalentProperties() {
        Set<URI> equivalents = new HashSet<>();
        for (OwlProperty other : superProperties) {
            if (other.superProperties.contains(this)) {
                equivalents.add(other.uri);
            }
        }
        // RL rules scm-op & scm-dp: Every property is equivalent to itself
        equivalents.add(this.uri);
        return equivalents;
    }

    /**
     * Get all properties declared disjoint with this one.
     */
    public Set<URI> getDisjointProperties() {
        Set<URI> disjoint = new HashSet<>();
        for (OwlProperty other : disjointProperties) {
            disjoint.add(other.uri);
        }
        return disjoint;
    }

    /**
     * Get all properties declared inverse of this one.
     */
    public Set<URI> getInverseProperties() {
        Set<URI> inverse = new HashSet<>();
        for (OwlProperty other : inverseProperties) {
            inverse.add(other.uri);
        }
        return inverse;
    }

    /**
     * Get the domain (set of class URIs/Resources).
     */
    public Set<Resource> getDomain() {
        Set<Resource> domain = new HashSet<>();
        for (OwlClass d : this.domain) {
            domain.add(d.getURI());
        }
        return domain;
    }

    /**
     * Get the range (set of class URIs/Resources).
     */
    public Set<Resource> getRange() {
        Set<Resource> range = new HashSet<>();
        for (OwlClass r : this.range) {
            range.add(r.getURI());
        }
        return range;
    }

    /**
     * Get the property restrictions relevant to this property.
     */
    public Set<Resource> getRestrictions() {
        Set<Resource> restrictions = new HashSet<>();
        for (OwlClass pr : this.restrictions) {
            restrictions.add(pr.getURI());
        }
        return restrictions;
    }

    /**
     * Apply property restriction rules that involve the same property.
     * RL rule scm-svf1: [someValuesFrom sub] subClassOf [someValuesFrom super]
     * RL rule scm-avf1: [allValuesFrom sub] subClassOf [allValuesFrom super]
     * Doesn't check subclass transitivity.
     * @return  Whether any information was generated
     */
    public boolean compareRestrictions() {
        boolean newInfo = false;
        for (OwlClass c1 : restrictions) {
            // Get all superclasses of this restriction's avf target class(es)
            // and avf target classes.
            Set<Resource> avfSuperClasses = c1.getAvfSuperClasses();
            Set<Resource> svfSuperClasses = c1.getSvfSuperClasses();
            if (avfSuperClasses.isEmpty() && svfSuperClasses.isEmpty()) {
                continue;
            }
            // Compare each other restriction's avf and svf targets to the
            // appropriate set of superclasses.
            for (OwlClass c2 : restrictions) {
                //svm-svf1, svm-avf1
                if (c1 != c2) {
                    Set<Resource> avf2 = c2.allValuesFrom();
                    avf2.retainAll(avfSuperClasses);
                    if (avf2.isEmpty()) {
                        Set<Resource> svf2 = c2.someValuesFrom();
                        svf2.retainAll(svfSuperClasses);
                        if (svf2.isEmpty()) {
                            continue;
                        }
                    }
                    // If c2's target is one of the parent classes of c1's
                    // target, then c2 is a more general version of c1 and
                    // therefore a superclass of c1.
                    newInfo = c1.addSuperClass(c2) || newInfo;
                }
            }
        }
        return newInfo;
    }
}
