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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.model.vocabulary.OWL;

/**
 * Hold on to facts about the schema (TBox/RBox) and perform what reasoning we
 * can without instance data.
 * <p>
 * The Schema object, together with the OwlClass and OwlProperty objects it
 * keeps track of, is responsible for schema reasoning, or the "Semantics of
 * Schema Vocabulary" rules from the OWL RL/RDF specificiation. Some rules are
 * handled dynamically, while the rest must be computed by calling closure()
 * once the schema data has been read in.
 * <p>
 * Schema rules implemented in {@link OwlClass}:
 *      scm-cls, scm-eqc1, scm-eqc2, scm-sco,
 *      scm-hv, scm-svf2, scm-avf2
 * <p>
 * Schema rules implemented in {@link OwlProperty}:
 *      scm-op, scm-dp, scm-eqp1, scm-eqp2, scm-spo, scm-dom1, scm-dom2,
 *      scm-rng1, scm-rng2, scm-svf1, scm-avf1
 * <p>
 * TODO: scm-cls officially states owl:Nothing is a subclass of every class.
 *  Do we need to explicitly do something with this fact?
 */
public class Schema {
    // Statements using these predicates are automatically relevant schema
    // information.
    private static final Set<URI> schemaPredicates = new HashSet<>();
    private static final URI[] schemaPredicateURIs = {
        RDFS.SUBCLASSOF,
        RDFS.SUBPROPERTYOF,
        RDFS.DOMAIN,
        RDFS.RANGE,
        OWL.EQUIVALENTCLASS,
        OWL.EQUIVALENTPROPERTY,
        OWL.INVERSEOF,
        OWL.DISJOINTWITH,
        OWL.COMPLEMENTOF,
        OWL.ONPROPERTY,
        OWL.SOMEVALUESFROM,
        OWL.ALLVALUESFROM,
        OWL.HASVALUE,
        OWL.MAXCARDINALITY,
        OWL2.MAXQUALIFIEDCARDINALITY,
        OWL2.PROPERTYDISJOINTWITH,
        OWL2.ONCLASS
    };

    // The fact that something is one of these types is schema information.
    private static final Set<Resource> schemaTypes = new HashSet<>();
    private static final Resource[] schemaTypeURIs = {
        OWL.TRANSITIVEPROPERTY,
        OWL2.IRREFLEXIVEPROPERTY,
        OWL.SYMMETRICPROPERTY,
        OWL2.ASYMMETRICPROPERTY,
        OWL.FUNCTIONALPROPERTY,
        OWL.INVERSEFUNCTIONALPROPERTY
    };

    static {
        for (URI uri : schemaPredicateURIs) {
            schemaPredicates.add(uri);
        }
        for (Resource uri : schemaTypeURIs) {
            schemaTypes.add(uri);
        }
    }

    /**
     * Does this triple/statement encode potentially relevant schema
     * information?
     */
    public static boolean isSchemaTriple(Statement triple) {
        URI pred = triple.getPredicate();
        // Triples with certain predicates are schema triples,
        if (schemaPredicates.contains(pred)) {
            return true;
        }
        // And certain type assertions are schema triples.
        else if (pred.equals(RDF.TYPE)) {
            if (schemaTypes.contains(triple.getObject())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Map URIs to schema information about a property
     */
    protected Map<URI, OwlProperty> properties = new HashMap<>();

    /**
     * Map Resources to schema information about a class/restriction
     */
    protected Map<Resource, OwlClass> classes = new HashMap<>();

    /**
     * Get schema information for a class, for reading and writing.
     * Instantiates OwlClass if it doesn't yet exist.
     */
    public OwlClass getClass(Resource c) {
        if (!classes.containsKey(c)) {
            classes.put(c, new OwlClass(c));
        }
        return classes.get(c);
    }

    /**
     * Get schema information for a class, for reading and writing.
     * Assumes this Value refers to a class Resource.
     */
    public OwlClass getClass(Value c) {
        return getClass((Resource) c);
    }

    /**
     * Get schema information for a property, for reading and writing.
     * Instantiates OwlProperty if it doesn't yet exist.
     */
    public OwlProperty getProperty(URI p) {
        if (!properties.containsKey(p)) {
            properties.put(p, new OwlProperty(p));
        }
        return properties.get(p);
    }

    /**
     * Get schema information for a property, for reading and writing.
     * Assumes this Value refers to a property URI.
     */
    public OwlProperty getProperty(Value p) {
        return getProperty((URI) p);
    }

    /**
     * Return whether this resource corresponds to a property.
     */
    public boolean hasProperty(URI r) {
        return properties.containsKey(r);
    }

    /**
     * Return whether this resource corresponds to a class.
     */
    public boolean hasClass(Resource r) {
        return classes.containsKey(r);
    }

    /**
     * Return whether this resource corresponds to a property restriction.
     */
    public boolean hasRestriction(Resource r) {
        return classes.containsKey(r) && !classes.get(r).getOnProperty().isEmpty();
    }

    public Schema() {
    }

    /**
     * Incorporate a new triple into the schema.
     */
    public void processTriple(Statement triple) {
        Resource s = triple.getSubject();
        URI p = triple.getPredicate();
        Value o = triple.getObject();
        if (isSchemaTriple(triple)) {
            // For a type statement to be schema information, it must yield
            // some boolean information about a property.
            if (p.equals(RDF.TYPE)) {
                if (schemaTypes.contains(o)) {
                    addPropertyType((URI) s, (Resource) o);
                }
            }

            // Domain/range
            else if (p.equals(RDFS.DOMAIN)) {
                // Don't add trivial domain owl:Thing
                if (!o.equals(OWL.THING)) {
                    getProperty(s).addDomain(getClass(o));
                }
            }
            else if (p.equals(RDFS.RANGE)) {
                // Don't add trivial range owl:Thing
                if (!o.equals(OWL.THING)) {
                    getProperty(s).addRange(getClass(o));
                }
            }

            // Sub/super relations
            else if (p.equals(RDFS.SUBCLASSOF)) {
                // Everything is a subclass of owl#Thing, we don't need to
                // store that information
                if (!o.equals(OWL.THING)) {
                    getClass(s).addSuperClass(getClass(o));
                }
            }
            else if (p.equals(RDFS.SUBPROPERTYOF)) {
                getProperty(s).addSuperProperty(getProperty(o));
            }

            // Equivalence relations
            else if (p.equals(OWL.EQUIVALENTCLASS)) {
                getClass(s).addEquivalentClass(getClass(o));
            }
            else if (p.equals(OWL.EQUIVALENTPROPERTY)) {
                getProperty(s).addEquivalentProperty(getProperty(o));
            }

            // Inverse properties
            else if (p.equals(OWL.INVERSEOF)) {
                getProperty(s).addInverse(getProperty(o));
                getProperty(o).addInverse(getProperty(s));
            }

            // Complementary classes
            else if (p.equals(OWL.COMPLEMENTOF)) {
                getClass(s).addComplement(getClass(o));
                getClass(o).addComplement(getClass(s));
            }

            // Disjoint classes and properties
            else if (p.equals(OWL.DISJOINTWITH)) {
                getClass(s).addDisjoint(getClass(o));
                getClass(o).addDisjoint(getClass(s));
            }
            else if (p.equals(OWL2.PROPERTYDISJOINTWITH)) {
                getProperty(s).addDisjoint(getProperty(o));
                getProperty(o).addDisjoint(getProperty(s));
            }

            // Property restriction info
            else if (p.equals(OWL.ONPROPERTY)) {
                getClass(s).addProperty(getProperty(o));
            }
            else if (p.equals(OWL.SOMEVALUESFROM)) {
                getClass(s).addSvf(getClass(o));
            }
            else if (p.equals(OWL.ALLVALUESFROM)) {
                getClass(s).addAvf(getClass(o));
            }
            else if (p.equals(OWL2.ONCLASS)) {
                getClass(s).addClass(getClass(o));
            }
            else if (p.equals(OWL.HASVALUE)) {
                getClass(s).addValue(o);
            }
            else if (p.equals(OWL.MAXCARDINALITY)) {
                getClass(s).setMaxCardinality(o);
            }
            else if (p.equals(OWL2.MAXQUALIFIEDCARDINALITY)) {
                getClass(s).setMaxQualifiedCardinality(o);
            }
        }
    }

    /**
     * Add a particular characteristic to a property.
     */
    private void addPropertyType(URI p, Resource t) {
        OwlProperty prop = getProperty(p);
        if (t.equals(OWL.TRANSITIVEPROPERTY)) {
            prop.setTransitive();
        }
        else if (t.equals(OWL.SYMMETRICPROPERTY)) {
            prop.setSymmetric();
        }
        else if (t.equals(OWL2.ASYMMETRICPROPERTY)) {
            prop.setAsymmetric();
        }
        else if (t.equals(OWL.FUNCTIONALPROPERTY)) {
            prop.setFunctional();
        }
        else if (t.equals(OWL.INVERSEFUNCTIONALPROPERTY)) {
            prop.setInverseFunctional();
        }
        else if (t.equals(OWL2.IRREFLEXIVEPROPERTY)) {
            prop.setIrreflexive();
        }
    }

    /**
     * Perform schema-level reasoning to compute the closure of statements
     * already represented in this schema. This includes things like subClassOf
     * transitivity and applying domain/range to subclasses.
     */
    public void closure() {
        // RL rule scm-spo: subPropertyOf transitivity
        // (takes in subproperty info; yields subproperty info)
        for (OwlProperty subprop : properties.values()) {
            subprop.computeSuperProperties();
        }

        // RL rules scm-hv, scm-svf2, scm-avf2: restrictions & subproperties
        // (take in subproperty info & prop. restrictions; yield subclass info)
        for (OwlClass c1 : classes.values()) {
            for (OwlClass c2 : classes.values()) {
                c1.compareRestrictions(c2);
            }
        }

        // The following two steps can affect each other, so repeat the block
        // as many times as necessary.
        boolean repeat;
        do {
            // RL rule scm-sco: subClassOf transitivity
            // (takes in subclass info; yields subclass info)
            // (This traverses the complete hierarchy, so we don't need to loop
            // again if changes are only made in this step)
            for (OwlClass subclass : classes.values()) {
                subclass.computeSuperClasses();
            }
            // RL rules scm-svf1, scm-avf1: property restrictions & subclasses
            // (take in subclass info & prop. restrictions; yield subclass info)
            // (If changes are made here, loop through both steps again)
            repeat = false;
            for (OwlProperty prop : properties.values()) {
                repeat = prop.compareRestrictions() || repeat;
            }
        } while (repeat);

        // Apply RL rules scm-dom1, scm-rng1, scm-dom2, scm-rng2:
        // (take in subclass/subproperty & domain/range; yield domain/range)
        for (OwlProperty prop : properties.values()) {
            prop.inheritDomainRange();
        }
    }

    /**
     * Determine whether a fact is contained in the Schema object
     * relationships or implied by schema rules.
     * @return  True if this schema contains the semantics of the triple
     */
    public boolean containsTriple(Statement triple) {
        // The schema certainly doesn't contain it if it's not a
        // schema-relevant triple at all.
        if (isSchemaTriple(triple)) {
            Resource s = triple.getSubject();
            URI p = triple.getPredicate();
            Value o = triple.getObject();
            // If this is telling us something about a property:
            if (properties.containsKey(s)) {
                OwlProperty prop = properties.get(s);
                // Property types:
                if (p.equals(RDF.TYPE)) {
                    if ((o.equals(OWL.TRANSITIVEPROPERTY)
                            && prop.isTransitive())
                        || (o.equals(OWL2.IRREFLEXIVEPROPERTY)
                            && prop.isIrreflexive())
                        || (o.equals(OWL.SYMMETRICPROPERTY)
                            && prop.isSymmetric())
                        || (o.equals(OWL2.ASYMMETRICPROPERTY)
                            && prop.isAsymmetric())
                        || (o.equals(OWL.FUNCTIONALPROPERTY)
                            && prop.isFunctional())
                        || (o.equals(OWL.INVERSEFUNCTIONALPROPERTY)
                            && prop.isInverseFunctional())) {
                        return true;
                    }
                }
                // Relationships with other properties:
                if ((p.equals(RDFS.SUBPROPERTYOF)
                        && prop.getSuperProperties().contains(o))
                    || (p.equals(OWL2.PROPERTYDISJOINTWITH)
                        && prop.getDisjointProperties().contains(o))
                    || (p.equals(OWL.EQUIVALENTPROPERTY)
                        && prop.getEquivalentProperties().contains(o))
                    || (p.equals(OWL.INVERSEOF)
                        && prop.getInverseProperties().contains(o))) {
                    return true;
                }
                // Relationships with classes:
                if ((p.equals(RDFS.DOMAIN)
                        && prop.getDomain().contains(o))
                    || (p.equals(RDFS.RANGE)
                        && prop.getRange().contains(o))) {
                    return true;
                }
            }
            // If this is about a class relationship:
            if (classes.containsKey(s)) {
                OwlClass subject = classes.get(s);
                if ((p.equals(OWL.EQUIVALENTCLASS)
                        && (subject.getEquivalentClasses().contains(o)))
                    || (p.equals(OWL.DISJOINTWITH)
                        && (subject.getDisjointClasses().contains(o)))
                    || (p.equals(OWL.COMPLEMENTOF)
                        && (subject.getComplementaryClasses().contains(o)))
                    || (p.equals(RDFS.SUBCLASSOF)
                        && (subject.getSuperClasses().contains(o)))) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Collect and return counts of different kinds of schema constructs
     */
    public String getSummary() {
        int nRestrictions = 0;
        for (Resource r : classes.keySet()) {
            OwlClass c = classes.get(r);
            if (!c.getOnProperty().isEmpty()) {
                nRestrictions++;
            }
        }
        int nClasses = classes.size();
        int nProperties = properties.size();
        String[] pTypes = { "Transitive", "Symmetric", "Asymmetric",
            "Functional", "Inverse Functional", "Irreflexive" };
        String[] rTypes = { "someValuesFrom", "allValuesFrom", "hasValue",
            "maxCardinality==0", "maxCardinality>0",
            "maxQualifiedCardinality==0", "maxQualifiedCardinality>0", };
        String[] edgeTypes = { "Superclass", "Disjoint class", "Complement",
            "Superproperty", "Disjoint property", "Inverse property",
            "Domain", "Range",
            "Equivalent class", "Equivalent property"};
        int[] pTotals = { 0, 0, 0, 0, 0, 0 };
        int[] rTotals = { 0, 0, 0, 0, 0, 0, 0 };
        int[] edgeTotals = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
        for (OwlClass c : classes.values()) {
            edgeTotals[0] += c.getSuperClasses().size() - 2;
            edgeTotals[1] += c.getDisjointClasses().size();
            edgeTotals[2] += c.getComplementaryClasses().size();
            edgeTotals[8] += c.getEquivalentClasses().size() - 1;
            if (!c.someValuesFrom().isEmpty()) rTotals[0]++;
            if (!c.allValuesFrom().isEmpty()) rTotals[1]++;
            if (!c.hasValue().isEmpty()) rTotals[2]++;
            if (c.getMaxCardinality() == 0) rTotals[3]++;
            if (c.getMaxCardinality() > 0) rTotals[4]++;
            if (c.getMaxQualifiedCardinality() == 0) rTotals[5]++;
            if (c.getMaxQualifiedCardinality() > 0) rTotals[6]++;
        }
        for (OwlProperty p : properties.values()) {
            if (p.isTransitive()) pTotals[0]++;
            if (p.isSymmetric()) pTotals[1]++;
            if (p.isAsymmetric()) pTotals[2]++;
            if (p.isFunctional()) pTotals[3]++;
            if (p.isInverseFunctional()) pTotals[4]++;
            if (p.isIrreflexive()) pTotals[5]++;
            edgeTotals[3] += p.getSuperProperties().size() - 1;
            edgeTotals[4] += p.getDisjointProperties().size();
            edgeTotals[5] += p.getInverseProperties().size();
            edgeTotals[6] += p.getDomain().size();
            edgeTotals[7] += p.getRange().size();
            edgeTotals[9] += p.getEquivalentProperties().size();
        }
        StringBuilder sb = new StringBuilder();
        sb.append("Schema summary:");
        sb.append("\n\tClasses: " + nClasses);
        sb.append("\n\t\tProperty Restrictions: ").append(nRestrictions);
        for (int i = 0; i < rTypes.length; i++) {
            sb.append("\n\t\t\t");
            sb.append(rTypes[i]).append(": ").append(rTotals[i]);
        }
        sb.append("\n\t\tOther: ").append(nClasses-nRestrictions);
        sb.append("\n\tProperties: ").append(nProperties);
        for (int i = 0; i < pTypes.length; i++) {
            sb.append("\n\t\t");
            sb.append(pTypes[i]).append(": ").append(pTotals[i]);
        }
        sb.append("\n\tConnections:");
        for (int i = 0; i < edgeTypes.length; i++) {
            sb.append("\n\t\t");
            sb.append(edgeTypes[i]).append(": ").append(edgeTotals[i]);
        }
        return sb.toString();
    }

    /**
     * Assuming a given resource corresponds to a property restriction,
     * describe the restriction.
     */
    public String explainRestriction(Resource type) {
        StringBuilder sb = new StringBuilder();
        if (classes.containsKey(type)) {
            OwlClass pr = classes.get(type);
            sb.append("owl:Restriction");
            for (URI p : pr.getOnProperty()) {
                sb.append(" (owl:onProperty ").append(p.toString()).append(")");
            }
            for (Value v : pr.hasValue()) {
                sb.append(" (owl:hasValue ").append(v.toString()).append(")");
            }
            for (Resource c : pr.someValuesFrom()) {
                sb.append(" (owl:someValuesFrom ").append(c.toString()).append(")");
            }
            for (Resource c : pr.allValuesFrom()) {
                sb.append(" (owl:allValuesFrom ").append(c.toString()).append(")");
            }
            int mc = pr.getMaxCardinality();
            int mqc = pr.getMaxQualifiedCardinality();
            if (mc >= 0) {
                sb.append(" (owl:maxCardinality ").append(mc).append(")");
            }
            if (mqc >= 0) {
                sb.append(" (owl:maxQualifiedCardinality ").append(mqc);
            }
            for (Resource c : pr.onClass()) {
                sb.append(" owl:onClass ").append(c.toString()).append(")");
            }
        }
        return sb.toString();
    }
}
