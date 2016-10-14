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
import org.openrdf.model.vocabulary.FOAF;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDFS;

/**
 * Test the application of the OWL RL/RDF schema ("scm-") rules.
 */
public class SchemaReasoningTest {
    private Schema schema;

    /**
     * Setup: load in test data
     */
    @Before
    public void insertData() {
        schema = new Schema();
        // Lifted from the LUBM ontology:
        schema.processTriple(TestUtils.statement(TestUtils.uri("Dean"),
            RDFS.SUBCLASSOF, TestUtils.uri("Professor")));
        schema.processTriple(TestUtils.statement(TestUtils.uri("Professor"),
            RDFS.SUBCLASSOF, TestUtils.uri("Faculty")));
        schema.processTriple(TestUtils.statement(TestUtils.uri("Faculty"),
            RDFS.SUBCLASSOF, TestUtils.uri("Employee")));
        schema.processTriple(TestUtils.statement(TestUtils.uri("University"),
            RDFS.SUBCLASSOF, TestUtils.uri("Organization")));
        schema.processTriple(TestUtils.statement(TestUtils.uri("ResearchGroup"),
            RDFS.SUBCLASSOF, TestUtils.uri("Organization")));
        schema.processTriple(TestUtils.statement(TestUtils.uri("headOf"),
            RDFS.SUBPROPERTYOF, TestUtils.uri("worksFor")));
        schema.processTriple(TestUtils.statement(TestUtils.uri("worksFor"),
            RDFS.SUBPROPERTYOF, TestUtils.uri("memberOf")));
        schema.processTriple(TestUtils.statement(TestUtils.uri("advisor"),
            RDFS.RANGE, TestUtils.uri("Professor")));
        schema.processTriple(TestUtils.statement(TestUtils.uri("hasAlumnus"),
            RDFS.DOMAIN, TestUtils.uri("University")));
        // Not explicitly part of LUBM but helpful for a couple tests:
        schema.processTriple(TestUtils.statement(TestUtils.uri("memberOf"),
            RDFS.DOMAIN, TestUtils.uri("Person")));
        schema.processTriple(TestUtils.statement(TestUtils.uri("memberOf"),
            RDFS.RANGE, TestUtils.uri("Organization")));
        schema.processTriple(TestUtils.statement(TestUtils.uri("researchSpecialty"),
            RDFS.SUBPROPERTYOF, TestUtils.uri("researchInterest")));
        schema.closure();
    }

    /**
     * scm-cls
     */
    @Test
    public void testSelfSubclass() throws Exception {
        Assert.assertTrue("Any class should be its own superclass",
            schema.getClass(TestUtils.uri("foo")).getSuperClasses().contains(
            TestUtils.uri("foo")));
    }
    @Test
    public void testSelfEquivalentClass() throws Exception {
        Assert.assertTrue("Any class should be its own equivalent",
            schema.getClass(TestUtils.uri("foo")).getEquivalentClasses().contains(
            TestUtils.uri("foo")));
    }
    @Test
    public void testSubclassOwlThing() throws Exception {
        Assert.assertTrue("Any class should be a subclass of owl:Thing",
            schema.getClass(TestUtils.uri("foo")).getSuperClasses().contains(
            OWL.THING));
    }

    /**
     * scm-sco
     */
    @Test
    public void testSubclassTransitivity() throws Exception {
        Assert.assertTrue("Failed to infer indirect superclass",
            schema.getClass(TestUtils.uri("Dean")).getSuperClasses().contains(
            TestUtils.uri("Employee")));
    }

    /**
     * scm-eqc1
     */
    @Test
    public void testEquivalentImpliesSubclass() throws Exception {
        schema.processTriple(TestUtils.statement(TestUtils.uri("Person"),
            OWL.EQUIVALENTCLASS, FOAF.PERSON));
        Assert.assertTrue("Failed to infer subClassOf from equivalentClass",
            schema.getClass(TestUtils.uri("Person")).getSuperClasses().contains(
            FOAF.PERSON));
    }

    /**
     * scm-eqc2
     */
    @Test
    public void testSubclassImpliesEquivalence() throws Exception {
        schema.processTriple(TestUtils.statement(FOAF.PERSON,
            RDFS.SUBCLASSOF,
            TestUtils.uri("Person")));
        schema.processTriple(TestUtils.statement(TestUtils.uri("Person"),
            RDFS.SUBCLASSOF,
            FOAF.PERSON));
        Assert.assertTrue("Failed to infer equivalentClass from subClassOf",
            schema.getClass(FOAF.PERSON).getEquivalentClasses().contains(
            TestUtils.uri("Person")));
    }

    /**
     * scm-op, scm-dp
     */
    @Test
    public void testSelfSubproperty() throws Exception {
        Assert.assertTrue("Any property should be its own superproperty",
            schema.getProperty(TestUtils.uri("foo")).getSuperProperties().contains(
            TestUtils.uri("foo")));
    }
    @Test
    public void testSelfEquivalentProperty() throws Exception {
        Assert.assertTrue("Any property should be its own equivalent",
            schema.getProperty(TestUtils.uri("foo")).getEquivalentProperties().contains(
            TestUtils.uri("foo")));
    }

    /**
     * scm-spo
     */
    @Test
    public void testSubpropertyTransitivity() throws Exception {
        Assert.assertTrue("Failed to infer indirect superproperty",
            schema.getProperty(TestUtils.uri("headOf")).getSuperProperties().contains(
            TestUtils.uri("memberOf")));
    }

    /**
     * scm-eqp1
     */
    @Test
    public void testEquivalentImpliesSubproperty() throws Exception {
        schema.processTriple(TestUtils.statement(TestUtils.uri("memberOf"),
            OWL.EQUIVALENTPROPERTY, TestUtils.uri("test:memberOf")));
        Assert.assertTrue("Failed to infer subPropertyOf from equivalentProperty",
            schema.getProperty(TestUtils.uri("test:memberOf")).getSuperProperties().contains(
            TestUtils.uri("memberOf")));
    }

    /**
     * scm-eqp2
     */
    @Test
    public void testSubpropertyImpliesEquivalence() throws Exception {
        schema.processTriple(TestUtils.statement(TestUtils.uri("advisor"),
            RDFS.SUBPROPERTYOF, TestUtils.uri("test:advisor")));
        schema.processTriple(TestUtils.statement(TestUtils.uri("test:advisor"),
            RDFS.SUBPROPERTYOF, TestUtils.uri("advisor")));
        Assert.assertTrue("Failed to infer equivalentProperty from mutual "
            + "subPropertyOf statements", schema.getProperty(
            TestUtils.uri("advisor")).getEquivalentProperties().contains(
            TestUtils.uri("test:advisor")));
    }

    /**
     * scm-dom1
     */
    @Test
    public void testDomainSuperclass() throws Exception {
        Assert.assertTrue("Failed to infer domain: superclass from domain: " +
            "subclass", schema.getProperty(TestUtils.uri("hasAlumnus")).getDomain().contains(
            TestUtils.uri("Organization")));
    }

    /**
     * scm-dom2
     */
    @Test
    public void testDomainInheritance() throws Exception {
        Assert.assertTrue("Subproperty should inherit superproperty's domain",
            schema.getProperty(TestUtils.uri("headOf")).getDomain().contains(
            TestUtils.uri("Person")));
    }

    /**
     * scm-rng1
     */
    @Test
    public void testRangeSuperclass() throws Exception {
        Assert.assertTrue("Failed to infer range: superclass from range: " +
            "subclass", schema.getProperty(TestUtils.uri("advisor")).getRange().contains(
            TestUtils.uri("Faculty")));
    }

    /**
     * scm-rng2
     */
    @Test
    public void testRangeInheritance() throws Exception {
        Assert.assertTrue("Subproperty should inherit superproperty's range",
            schema.getProperty(TestUtils.uri("headOf")).getRange().contains(
            TestUtils.uri("Organization")));
    }


    /**
     * scm-hv
     */
    @Test
    public void testHasValueSubProp() throws Exception {
        Resource c1 = TestUtils.bnode("c1");
        Resource c2 = TestUtils.bnode("c2");
        schema.processTriple(TestUtils.statement(c1, OWL.HASVALUE,
            TestUtils.stringLiteral("big data")));
        schema.processTriple(TestUtils.statement(c1, OWL.ONPROPERTY,
            TestUtils.uri("researchSpecialty")));
        schema.processTriple(TestUtils.statement(c2, OWL.HASVALUE,
            TestUtils.stringLiteral("big data")));
        schema.processTriple(TestUtils.statement(c2, OWL.ONPROPERTY,
            TestUtils.uri("researchInterest")));
        schema.closure();
        Assert.assertTrue("[hasValue x on <subproperty>] should be subclass of"
            + " [hasValue x on <superproperty>]",
            schema.getClass(c1).getSuperClasses().contains(c2));
    }

    /**
     * scm-svf1
     */
    @Test
    public void testSomeValuesSubClass() throws Exception {
        Resource c1 = TestUtils.bnode("c1");
        Resource c2 = TestUtils.bnode("c2");
        schema.processTriple(TestUtils.statement(c1, OWL.SOMEVALUESFROM,
            TestUtils.uri("ResearchGroup")));
        schema.processTriple(TestUtils.statement(c1, OWL.ONPROPERTY,
            TestUtils.uri("worksFor")));
        schema.processTriple(TestUtils.statement(c2, OWL.SOMEVALUESFROM,
            TestUtils.uri("Organization")));
        schema.processTriple(TestUtils.statement(c2, OWL.ONPROPERTY,
            TestUtils.uri("worksFor")));
        schema.closure();
        Assert.assertTrue("[someValuesFrom <subclass> on p] should be subclass of"
            + " [someValuesFrom <superclass> on p]",
            schema.getClass(c1).getSuperClasses().contains(c2));
    }

    /**
     * scm-svf2
     */
    @Test
    public void testSomeValuesSubProp() throws Exception {
        Resource c1 = TestUtils.bnode("c1");
        Resource c2 = TestUtils.bnode("c2");
        schema.processTriple(TestUtils.statement(c1, OWL.SOMEVALUESFROM,
            TestUtils.uri("Organization")));
        schema.processTriple(TestUtils.statement(c1, OWL.ONPROPERTY,
            TestUtils.uri("worksFor")));
        schema.processTriple(TestUtils.statement(c2, OWL.SOMEVALUESFROM,
            TestUtils.uri("Organization")));
        schema.processTriple(TestUtils.statement(c2, OWL.ONPROPERTY,
            TestUtils.uri("memberOf")));
        schema.closure();
        Assert.assertTrue("[someValuesFrom y on <subproperty>] should be subclass of"
            + " [someValuesFrom y on <superproperty>]",
            schema.getClass(c1).getSuperClasses().contains(c2));
    }

    /**
     * scm-avf1
     */
    @Test
    public void testAllValuesSubClass() throws Exception {
        Resource c1 = TestUtils.bnode("c1");
        Resource c2 = TestUtils.bnode("c2");
        schema.processTriple(TestUtils.statement(c1, OWL.ALLVALUESFROM,
            TestUtils.uri("ResearchGroup")));
        schema.processTriple(TestUtils.statement(c1, OWL.ONPROPERTY,
            TestUtils.uri("worksFor")));
        schema.processTriple(TestUtils.statement(c2, OWL.ALLVALUESFROM,
            TestUtils.uri("Organization")));
        schema.processTriple(TestUtils.statement(c2, OWL.ONPROPERTY,
            TestUtils.uri("worksFor")));
        schema.closure();
        Assert.assertTrue("[allValuesFrom <subclass> on p] should be subclass of"
            + " [allValuesFrom <superclass> on p]",
            schema.getClass(c1).getSuperClasses().contains(c2));
    }

    /**
     * scm-avf2
     */
    @Test
    public void testAllValuesSubProp() throws Exception {
        Resource c1 = TestUtils.bnode("c1");
        Resource c2 = TestUtils.bnode("c2");
        schema.processTriple(TestUtils.statement(c1, OWL.ALLVALUESFROM,
            TestUtils.uri("University")));
        schema.processTriple(TestUtils.statement(c1, OWL.ONPROPERTY,
            TestUtils.uri("worksFor")));
        schema.processTriple(TestUtils.statement(c2, OWL.ALLVALUESFROM,
            TestUtils.uri("University")));
        schema.processTriple(TestUtils.statement(c2, OWL.ONPROPERTY,
            TestUtils.uri("memberOf")));
        schema.closure();
        Assert.assertTrue("[allValuesFrom y on <superproperty>] should be subclass of"
            + " [allValuesFrom y on <subproperty>]",
            schema.getClass(c2).getSuperClasses().contains(c1));
    }
}
