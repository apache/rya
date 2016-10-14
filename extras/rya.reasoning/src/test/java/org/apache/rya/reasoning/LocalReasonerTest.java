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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.SKOS;

public class LocalReasonerTest {
    private LocalReasoner reasoner;
    private Schema schema;

    /**
     * Load in a small schema to use in instance reasoning
     */
    @Before
    public void loadSchema() {
        schema = new Schema();
        reasoner = new LocalReasoner(TestUtils.NODE, schema, 1, 0);
    }

    /**
     * cax-sco
     */
    @Test
    public void testInferSuperclass() throws Exception {
        schema.processTriple(TestUtils.statement(TestUtils.uri("Professor"),
            RDFS.SUBCLASSOF, TestUtils.uri("Faculty")));
        reasoner.processFact(TestUtils.fact(TestUtils.NODE, RDF.TYPE,
            TestUtils.uri("Professor")));
        Assert.assertTrue("Type not derived from subclass",
            reasoner.types.knownTypes.containsKey(TestUtils.uri("Faculty")));
    }

    /**
     * prp-dom
     */
    @Test
    public void testInferDomain() throws Exception {
        schema.processTriple(TestUtils.statement(TestUtils.uri("hasAlumnus"),
            RDFS.DOMAIN, TestUtils.uri("University")));
        reasoner.processFact(TestUtils.fact(TestUtils.NODE,
            TestUtils.uri("hasAlumnus"), TestUtils.uri("John Doe")));
        Assert.assertTrue("Type not derived from rdfs:domain",
            reasoner.types.knownTypes.containsKey(TestUtils.uri("University")));
    }

    /**
     * prp-rng
     */
    @Test
    public void testInferRange() throws Exception {
        schema.processTriple(TestUtils.statement(TestUtils.uri("advisor"),
            RDFS.RANGE, TestUtils.uri("Professor")));
        reasoner.processFact(TestUtils.fact(TestUtils.uri("John Doe"),
            TestUtils.uri("advisor"), TestUtils.NODE));
        Assert.assertTrue("Type not derived from rdfs:range",
            reasoner.types.knownTypes.containsKey(TestUtils.uri("Professor")));
    }

    /**
     * cls-nothing2
     */
    @Test
    public void testNothingInconsistent() throws Exception {
        reasoner.processFact(TestUtils.fact(TestUtils.NODE, RDF.TYPE, OWL.NOTHING));
        Assert.assertTrue("rdf:type owl:Nothing should be inconsistent",
            reasoner.hasInconsistencies());
    }

    /**
     * prp-inv1
     */
    @Test
    public void testInverseProperty1() throws Exception {
        schema.processTriple(TestUtils.statement(TestUtils.uri("memberOf"),
            OWL.INVERSEOF, TestUtils.uri("member")));
        reasoner.processFact(TestUtils.fact(TestUtils.uri("y"),
            TestUtils.uri("memberOf"), TestUtils.NODE));
        for (Fact t : reasoner.getFacts()) {
            if (t.getSubject().equals(TestUtils.NODE)
                && t.getPredicate().equals(TestUtils.uri("member"))
                && t.getObject().equals(TestUtils.uri("y"))) {
                return;
            }
        }
        Assert.fail("Should have derived inverse triple");
    }

    /**
     * prp-inv2
     */
    @Test
    public void testInverseProperty2() throws Exception {
        schema.processTriple(TestUtils.statement(TestUtils.uri("memberOf"),
            OWL.INVERSEOF, TestUtils.uri("member")));
        reasoner.processFact(TestUtils.fact(TestUtils.uri("y"),
            TestUtils.uri("member"), TestUtils.NODE));
        for (Fact t : reasoner.getFacts()) {
            if (t.getSubject().equals(TestUtils.NODE)
                && t.getPredicate().equals(TestUtils.uri("memberOf"))
                && t.getObject().equals(TestUtils.uri("y"))) {
                return;
            }
        }
        Assert.fail("Should have derived inverse triple");
    }

    /**
     * prp-spo1
     */
    @Test
    public void testInferSuperproperty() throws Exception {
        schema.processTriple(TestUtils.statement(TestUtils.uri("headOf"),
            RDFS.SUBPROPERTYOF, TestUtils.uri("worksFor")));
        reasoner.processFact(TestUtils.fact(TestUtils.NODE,
            TestUtils.uri("headOf"), TestUtils.uri("org")));
        for (Fact t : reasoner.getFacts()) {
            if (t.getSubject().equals(TestUtils.NODE)
                && t.getPredicate().equals(TestUtils.uri("worksFor"))
                && t.getObject().equals(TestUtils.uri("org"))) {
                return;
            }
        }
        Assert.fail("Superproperty not inferred from subproperty");
    }

    /**
     * prp-symp
     */
    @Test
    public void testSymmetry() throws Exception {
        schema.processTriple(TestUtils.statement(SKOS.RELATED, RDF.TYPE,
            OWL.SYMMETRICPROPERTY));
        reasoner.processFact(TestUtils.fact(TestUtils.uri("y"), SKOS.RELATED,
            TestUtils.NODE));
        for (Fact t : reasoner.getFacts()) {
            if (t.getSubject().equals(TestUtils.NODE)
                && t.getPredicate().equals(SKOS.RELATED)
                && t.getObject().equals(TestUtils.uri("y"))) {
                return;
            }
        }
        Assert.fail("Symmetric property not inferred");
    }

    /**
     * cls-com
     */
    @Test
    public void testComplementaryClasses() throws Exception {
        schema.processTriple(TestUtils.statement(TestUtils.uri("A"),
            OWL.COMPLEMENTOF, TestUtils.uri("NotA")));
        reasoner.processFact(TestUtils.fact(TestUtils.NODE, RDF.TYPE,
            TestUtils.uri("A")));
        reasoner.processFact(TestUtils.fact(TestUtils.NODE, RDF.TYPE,
            TestUtils.uri("NotA")));
        Assert.assertTrue("Complementary class membership not detected",
            reasoner.hasInconsistencies());
    }

    /**
     * cax-dw
     */
    @Test
    public void testDisjointClasses() throws Exception {
        schema.processTriple(TestUtils.statement(SKOS.CONCEPT, OWL.DISJOINTWITH,
            SKOS.COLLECTION));
        reasoner.processFact(TestUtils.fact(TestUtils.NODE, RDF.TYPE,
            SKOS.CONCEPT));
        reasoner.processFact(TestUtils.fact(TestUtils.NODE, RDF.TYPE,
            SKOS.COLLECTION));
        Assert.assertTrue("Disjoint class membership not detected",
            reasoner.hasInconsistencies());
    }

    /**
     * prp-trp
     */
    @Test
    public void testTransitivePropertyIncomingOutgoing() throws Exception {
        schema.processTriple(TestUtils.statement(TestUtils.uri("subOrganizationOf"),
            RDF.TYPE, OWL.TRANSITIVEPROPERTY));
        reasoner.processFact(TestUtils.fact(TestUtils.uri("y"),
            TestUtils.uri("subOrganizationOf"), TestUtils.NODE));
        reasoner.processFact(TestUtils.fact(TestUtils.NODE,
            TestUtils.uri("subOrganizationOf"), TestUtils.uri("z")));
        for (Fact t : reasoner.getFacts()) {
            if (t.getSubject().equals(TestUtils.uri("y"))
                && t.getPredicate().equals(TestUtils.uri("subOrganizationOf"))
                && t.getObject().equals(TestUtils.uri("z"))) {
                return;
            }
        }
        Assert.fail("Transitive relation not inferred (received incoming edge first)");
    }
    @Test
    public void testTransitivePropertyOutgoingIncoming() throws Exception {
        schema.processTriple(TestUtils.statement(TestUtils.uri("subOrganizationOf"),
            RDF.TYPE, OWL.TRANSITIVEPROPERTY));
        reasoner.processFact(TestUtils.fact(TestUtils.NODE,
            TestUtils.uri("subOrganizationOf"), TestUtils.uri("z")));
        reasoner.processFact(TestUtils.fact(TestUtils.uri("y"),
            TestUtils.uri("subOrganizationOf"), TestUtils.NODE));
        for (Fact t : reasoner.getFacts()) {
            if (t.getSubject().equals(TestUtils.uri("y"))
                && t.getPredicate().equals(TestUtils.uri("subOrganizationOf"))
                && t.getObject().equals(TestUtils.uri("z"))) {
                Assert.fail("Transitive relation should not be inferred "
                + "(received outgoing edge first)");
            }
        }
    }

    /**
     * prp-irp
     */
    @Test
    public void testIrreflexiveProperty() throws Exception {
        schema.processTriple(TestUtils.statement(TestUtils.uri("hasParent"),
            RDF.TYPE, OWL2.IRREFLEXIVEPROPERTY));
        reasoner.processFact(TestUtils.fact(TestUtils.NODE,
            TestUtils.uri("hasParent"), TestUtils.NODE));
        Assert.assertTrue("Relation to self via irreflexive property not detected",
            reasoner.hasInconsistencies());
    }

    /**
     * prp-asyp
     */
    @Test
    public void testAsymmetricPropertyIncomingOutgoing() throws Exception {
        schema.processTriple(TestUtils.statement(TestUtils.uri("hasChild"),
            RDF.TYPE, OWL2.ASYMMETRICPROPERTY));
        reasoner.processFact(TestUtils.fact(TestUtils.uri("y"),
            TestUtils.uri("hasChild"), TestUtils.NODE));
        reasoner.processFact(TestUtils.fact(TestUtils.NODE,
            TestUtils.uri("hasChild"), TestUtils.uri("y")));
        Assert.assertTrue("Symmetric relation with asymmetric property not detected"
            + " (receiving incoming edge first)", reasoner.hasInconsistencies());
    }
    @Test
    public void testAsymmetricPropertyReverseOutgoingIncoming() throws Exception {
        schema.processTriple(TestUtils.statement(TestUtils.uri("hasChild"), RDF.TYPE,
            OWL2.ASYMMETRICPROPERTY));
        reasoner.processFact(TestUtils.fact(TestUtils.NODE,
            TestUtils.uri("hasChild"), TestUtils.uri("y")));
        reasoner.processFact(TestUtils.fact(TestUtils.uri("y"),
            TestUtils.uri("hasChild"), TestUtils.NODE));
        Assert.assertFalse("Symmetric relation with asymmetric property should"
            + " not be detected when receiving outgoing edge first",
            reasoner.hasInconsistencies());
    }
    @Test
    public void testAsymmetricPropertyReflexive() throws Exception {
        schema.processTriple(TestUtils.statement(TestUtils.uri("hasChild"),
            RDF.TYPE, OWL2.ASYMMETRICPROPERTY));
        reasoner.processFact(TestUtils.fact(TestUtils.NODE,
            TestUtils.uri("hasChild"), TestUtils.NODE));
        Assert.assertTrue("Self-referential relation with asymmetric property"
            + " not detected", reasoner.hasInconsistencies());
    }

    /**
     * prp-pdw
     */
    @Test
    public void testDisjointProperties() throws Exception {
        schema.processTriple(TestUtils.statement(TestUtils.uri("p1"),
            OWL2.PROPERTYDISJOINTWITH, TestUtils.uri("p2")));
        reasoner.processFact(TestUtils.fact(TestUtils.NODE,
            TestUtils.uri("p1"), TestUtils.uri("y")));
        reasoner.processFact(TestUtils.fact(TestUtils.NODE,
            TestUtils.uri("p2"), TestUtils.uri("y")));
        Assert.assertTrue("Disjoint property usage not detected (left-hand side"
            + " of propertyDisjointWith received first)", reasoner.hasInconsistencies());
    }
    @Test
    public void testDisjointPropertiesReverse() throws Exception {
        schema.processTriple(TestUtils.statement(TestUtils.uri("p2"),
            OWL2.PROPERTYDISJOINTWITH, TestUtils.uri("p1")));
        reasoner.processFact(TestUtils.fact(TestUtils.NODE,
            TestUtils.uri("p1"), TestUtils.uri("y")));
        reasoner.processFact(TestUtils.fact(TestUtils.NODE,
            TestUtils.uri("p2"), TestUtils.uri("y")));
        Assert.assertTrue("Disjoint property usage not detected (right-hand side"
            + " of propertyDisjointWith received first)", reasoner.hasInconsistencies());
    }

    /**
     * cls-svf1
     */
    @Test
    public void testSomeValuesFromMembership() throws Exception {
        schema.processTriple(TestUtils.statement(TestUtils.uri("x"),
            OWL.SOMEVALUESFROM, TestUtils.uri("Department")));
        schema.processTriple(TestUtils.statement(TestUtils.uri("x"),
            OWL.ONPROPERTY, TestUtils.uri("headOf")));
        reasoner.processFact(TestUtils.fact(TestUtils.NODE, RDF.TYPE,
            TestUtils.uri("Department")));
        reasoner.processFact(TestUtils.fact(TestUtils.uri("John Doe"),
            TestUtils.uri("headOf"), TestUtils.NODE));
        reasoner.getTypes();
        for (Fact t : reasoner.getFacts()) {
            if (t.getSubject().equals(TestUtils.uri("John Doe"))
                && t.getPredicate().equals(RDF.TYPE)
                && t.getObject().equals(TestUtils.uri("x"))) {
                return;
            }
        }
        Assert.fail("If x is a property restriction [owl:someValuesFrom c]"
            + " on property p;  (z type c) then (y p z) should imply"
            + " (y type x).");
    }
    @Test
    public void testSomeValuesFromMembershipReverse() throws Exception {
        schema.processTriple(TestUtils.statement(TestUtils.uri("x"),
            OWL.SOMEVALUESFROM, TestUtils.uri("Department")));
        schema.processTriple(TestUtils.statement(TestUtils.uri("x"),
            OWL.ONPROPERTY, TestUtils.uri("headOf")));
        reasoner.processFact(TestUtils.fact(TestUtils.uri("John Doe"),
            TestUtils.uri("headOf"), TestUtils.NODE));
        reasoner.processFact(TestUtils.fact(TestUtils.NODE, RDF.TYPE,
            TestUtils.uri("Department")));
        reasoner.getTypes();
        for (Fact t : reasoner.getFacts()) {
            if (t.getSubject().equals(TestUtils.uri("John Doe"))
                && t.getPredicate().equals(RDF.TYPE)
                && t.getObject().equals(TestUtils.uri("x"))) {
                return;
            }
        }
        Assert.fail("If x is a property restriction [owl:someValuesFrom c]"
            + " on property p; (y p z) then (z type c) should imply"
            + " (y type x).");
    }

    /**
     * cls-svf2
     */
    @Test
    public void testSomeValuesFromThing() throws Exception {
        schema.processTriple(TestUtils.statement(TestUtils.uri("x"),
            OWL.SOMEVALUESFROM, OWL.THING));
        schema.processTriple(TestUtils.statement(TestUtils.uri("x"),
            OWL.ONPROPERTY, TestUtils.uri("foo")));
        reasoner.processFact(TestUtils.fact(TestUtils.NODE,
            TestUtils.uri("foo"), TestUtils.uri("bar")));
        reasoner.getTypes();
        for (Fact t : reasoner.getFacts()) {
            if (t.getSubject().equals(TestUtils.NODE)
                && t.getPredicate().equals(RDF.TYPE)
                && t.getObject().equals(TestUtils.uri("x"))) {
                return;
            }
        }
        Assert.fail("If x is a property restriction [owl:someValuesFrom owl:Thing]"
            + " on property p; (y p z) should imply (y type x) for any z.");
    }

    /**
     * cls-hv1
     */
    @Test
    public void testHasValueInferValue() throws Exception {
        schema.processTriple(TestUtils.statement(TestUtils.uri("x"), OWL.HASVALUE,
            TestUtils.uri("y")));
        schema.processTriple(TestUtils.statement(TestUtils.uri("x"),
            OWL.ONPROPERTY, TestUtils.uri("p")));
        reasoner.processFact(TestUtils.fact(TestUtils.NODE, RDF.TYPE,
            TestUtils.uri("x")));
        for (Fact t : reasoner.getFacts()) {
            if (t.getSubject().equals(TestUtils.NODE)
                && t.getPredicate().equals(TestUtils.uri("p"))
                && t.getObject().equals(TestUtils.uri("y"))) {
                return;
            }
        }
        Assert.fail("If x is a property restriction [owl:hasValue y]"
            + " on property p; (u type x) should imply (u p y) for any u.");
    }

    /**
     * cls-hv2
     */
    @Test
    public void testHasValueInferClass() throws Exception {
        schema.processTriple(TestUtils.statement(TestUtils.uri("x"), OWL.HASVALUE,
            TestUtils.uri("y")));
        schema.processTriple(TestUtils.statement(TestUtils.uri("x"),
            OWL.ONPROPERTY, TestUtils.uri("p")));
        reasoner.processFact(TestUtils.fact(TestUtils.NODE,
            TestUtils.uri("p"), TestUtils.uri("y")));
        reasoner.getTypes();
        for (Fact t : reasoner.getFacts()) {
            if (t.getSubject().equals(TestUtils.NODE)
                && t.getPredicate().equals(RDF.TYPE)
                && t.getObject().equals(TestUtils.uri("x"))) {
                return;
            }
        }
        Assert.fail("If x is a property restriction [owl:hasValue y]"
            + " on property p; (u p y) should imply (u type x) for any u.");
    }


    /**
     * cls-avf
     */
    @Test
    public void testAllValuesFrom() throws Exception {
        schema.processTriple(TestUtils.statement(TestUtils.uri("x"),
            OWL.ALLVALUESFROM, TestUtils.uri("Human")));
        schema.processTriple(TestUtils.statement(TestUtils.uri("x"),
            OWL.ONPROPERTY, TestUtils.uri("hasParent")));
        reasoner.processFact(TestUtils.fact(TestUtils.NODE,
            TestUtils.uri("hasParent"), TestUtils.uri("v")));
        reasoner.processFact(TestUtils.fact(TestUtils.NODE, RDF.TYPE,
            TestUtils.uri("x")));
        reasoner.getTypes();
        for (Fact t : reasoner.getFacts()) {
            if (t.getSubject().equals(TestUtils.uri("v"))
                && t.getPredicate().equals(RDF.TYPE)
                && t.getObject().equals(TestUtils.uri("Human"))) {
                return;
            }
        }
        Assert.fail("If x is a property restriction [owl:allValuesFrom c]"
            + " on property p; (u p v) then (u type x) should imply (v type c).");
    }
    @Test
    public void testAllValuesFromReverse() throws Exception {
        schema.processTriple(TestUtils.statement(TestUtils.uri("x"),
            OWL.ALLVALUESFROM, TestUtils.uri("Human")));
        schema.processTriple(TestUtils.statement(TestUtils.uri("x"),
            OWL.ONPROPERTY, TestUtils.uri("hasParent")));
        reasoner.processFact(TestUtils.fact(TestUtils.NODE, RDF.TYPE,
            TestUtils.uri("x")));
        reasoner.processFact(TestUtils.fact(TestUtils.NODE,
            TestUtils.uri("hasParent"), TestUtils.uri("v")));
        reasoner.getTypes();
        for (Fact t : reasoner.getFacts()) {
            if (t.getSubject().equals(TestUtils.uri("v"))
                && t.getPredicate().equals(RDF.TYPE)
                && t.getObject().equals(TestUtils.uri("Human"))) {
                return;
            }
        }
        Assert.fail("If x is a property restriction [owl:allValuesFrom c]"
            + " on property p; (u type x) then (u p v) should imply (v type c).");
    }

    /**
     * cls-maxc1
     */
    @Test
    public void testMaxCardinalityZero() throws Exception {
        URI r = TestUtils.uri("restriction");
        URI p = TestUtils.uri("impossiblePredicate1");
        schema.processTriple(TestUtils.statement(r, OWL.MAXCARDINALITY,
            TestUtils.intLiteral("0")));
        schema.processTriple(TestUtils.statement(r, OWL.ONPROPERTY, p));
        reasoner.processFact(TestUtils.fact(TestUtils.NODE, p,
            TestUtils.uri("y")));
        reasoner.processFact(TestUtils.fact(TestUtils.NODE, RDF.TYPE, r));
        Assert.assertTrue("If p has maxCardinality of 0; then any (x p y)"
            + " should be found inconsistent", reasoner.hasInconsistencies());
    }

    /**
     * cls-maxqc2
     */
    @Test
    public void testMaxQCardinalityZeroThings() throws Exception {
        Resource r = TestUtils.bnode("restriction");
        URI p = TestUtils.uri("impossiblePredicate2");
        schema.processTriple(TestUtils.statement(r, OWL2.MAXQUALIFIEDCARDINALITY,
            TestUtils.intLiteral("0")));
        schema.processTriple(TestUtils.statement(r, OWL.ONPROPERTY, p));
        schema.processTriple(TestUtils.statement(r, OWL2.ONCLASS, OWL.THING));
        reasoner.processFact(TestUtils.fact(TestUtils.NODE, p,
            TestUtils.uri("y")));
        reasoner.processFact(TestUtils.fact(TestUtils.NODE, RDF.TYPE, r));
        Assert.assertTrue("If p has maxQualifiedCardinality of 0 on owl:Thing;"
            + " then any (x p y) should be found inconsistent",
            reasoner.hasInconsistencies());
    }
}
