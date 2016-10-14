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
import org.junit.Test;
import org.openrdf.model.URI;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.model.vocabulary.SKOS;

public class SchemaTest {
    URI lubm(String s) {
        return TestUtils.uri("http://swat.cse.lehigh.edu/onto/univ-bench.owl", s);
    }

    // Recognize useful schema data
    @Test
    public void testAcceptSubclass() throws Exception {
        Assert.assertTrue(Schema.isSchemaTriple(TestUtils.statement(
            lubm("Lecturer"), RDFS.SUBCLASSOF, lubm("Faculty"))));
    }
    @Test
    public void testAcceptSubproperty() throws Exception {
        Assert.assertTrue(Schema.isSchemaTriple(TestUtils.statement(
            lubm("mastersDegreeFrom"), RDFS.SUBPROPERTYOF, lubm("degreeFrom"))));
    }
    @Test
    public void testAcceptDomain() throws Exception {
        Assert.assertTrue(Schema.isSchemaTriple(TestUtils.statement(
            lubm("degreeFrom"), RDFS.DOMAIN, lubm("Person"))));
    }
    @Test
    public void testAcceptRange() throws Exception {
        Assert.assertTrue(Schema.isSchemaTriple(TestUtils.statement(
            lubm("degreeFrom"), RDFS.RANGE, lubm("University"))));
    }
    @Test
    public void testAcceptInverse() throws Exception {
        Assert.assertTrue(Schema.isSchemaTriple(TestUtils.statement(
            lubm("degreeFrom"), OWL.INVERSEOF, lubm("hasAlumnus"))));
    }

    // Reject trivial schema information
    @Test
    public void testRejectClassAssertion() throws Exception {
        Assert.assertFalse(Schema.isSchemaTriple(TestUtils.statement(
            lubm("TeachingAssistant"), RDF.TYPE, OWL.CLASS)));
    }
    @Test
    public void testRejectObjectPropertyAssertion() throws Exception {
        Assert.assertFalse(Schema.isSchemaTriple(TestUtils.statement(
            lubm("researchProject"), RDF.TYPE, OWL.OBJECTPROPERTY)));
    }
    @Test
    public void testRejectDatatypePropertyAssertion() throws Exception {
        Assert.assertFalse(Schema.isSchemaTriple(TestUtils.statement(
            lubm("researchInterest"), RDF.TYPE, OWL.DATATYPEPROPERTY)));
    }

    // Save and retrieve schema information correctly.
    @Test
    public void testInputSubclass() throws Exception {
        Schema schema = new Schema();
        schema.processTriple(TestUtils.statement(lubm("Dean"),
            RDFS.SUBCLASSOF, lubm("Professor")));
        Assert.assertTrue("Professor should be a superclass of Dean",
            schema.getClass(lubm("Dean")).getSuperClasses().contains(lubm("Professor")));
    }
    @Test
    public void testInputDisjointClass() throws Exception {
        Schema schema = new Schema();
        schema.processTriple(TestUtils.statement(SKOS.CONCEPT,
            OWL.DISJOINTWITH, SKOS.COLLECTION));
        Assert.assertTrue("(x disjointWith y): y not found in x's disjoint classes",
            schema.getClass(SKOS.CONCEPT).getDisjointClasses().contains(SKOS.COLLECTION));
    }
    @Test
    public void testInputDisjointClassReverse() throws Exception {
        Schema schema = new Schema();
        schema.processTriple(TestUtils.statement(SKOS.CONCEPT,
            OWL.DISJOINTWITH, SKOS.COLLECTION));
        Assert.assertTrue("(x disjointWith y): x not found in y's disjoint classes",
            schema.getClass(SKOS.COLLECTION).getDisjointClasses().contains(SKOS.CONCEPT));
    }
    @Test
    public void testInputComplementaryClass() throws Exception {
        Schema schema = new Schema();
        schema.processTriple(TestUtils.statement(TestUtils.uri("A"),
            OWL.COMPLEMENTOF, TestUtils.uri("NotA")));
        Assert.assertTrue("(x complementOf y): y not found in x's complementary classes",
            schema.getClass(TestUtils.uri("A")).getComplementaryClasses().contains(TestUtils.uri("NotA")));
    }
    @Test
    public void testInputComplementaryClassReverse() throws Exception {
        Schema schema = new Schema();
        schema.processTriple(TestUtils.statement(TestUtils.uri("A"),
            OWL.COMPLEMENTOF, TestUtils.uri("NotA")));
        Assert.assertTrue("(x complementOf y): x not found in y's complementary classes",
            schema.getClass(TestUtils.uri("NotA")).getComplementaryClasses().contains(TestUtils.uri("A")));
    }

    @Test
    public void testInputSubproperty() throws Exception {
        Schema schema = new Schema();
        schema.processTriple(TestUtils.statement(lubm("worksFor"),
            RDFS.SUBPROPERTYOF, lubm("memberOf")));
        Assert.assertTrue("memberOf should be a superproperty of worksFor",
            schema.getProperty(lubm("worksFor")).getSuperProperties().contains(lubm("memberOf")));
    }
    @Test
    public void testInputDomain() throws Exception {
        Schema schema = new Schema();
        schema.processTriple(TestUtils.statement(lubm("hasAlumnus"),
            RDFS.DOMAIN, lubm("University")));
        Assert.assertTrue("Domain information not correctly returned",
            schema.getProperty(lubm("hasAlumnus")).getDomain().contains(lubm("University")));
    }
    @Test
    public void testInputRange() throws Exception {
        Schema schema = new Schema();
        schema.processTriple(TestUtils.statement(lubm("advisor"), RDFS.RANGE, lubm("Professor")));
        Assert.assertTrue("Range information not correctly returned",
            schema.getProperty(lubm("advisor")).getRange().contains(lubm("Professor")));
    }
    @Test
    public void testInputInverse() throws Exception {
        Schema schema = new Schema();
        schema.processTriple(TestUtils.statement(lubm("memberOf"),
            OWL.INVERSEOF, lubm("memberOf")));
        Assert.assertTrue("inverseOf relation not returned",
            schema.getProperty(lubm("memberOf")).getInverseProperties().contains(lubm("memberOf")));
    }
    @Test
    public void testInputInverseReverse() throws Exception {
        Schema schema = new Schema();
        schema.processTriple(TestUtils.statement(lubm("memberOf"),
            OWL.INVERSEOF, lubm("memberOf")));
        Assert.assertTrue("symmetric inverseOf relation not returned",
            schema.getProperty(lubm("memberOf")).getInverseProperties().contains(lubm("memberOf")));
    }
    @Test
    public void testInputSymmetric() throws Exception {
        Schema schema = new Schema();
        schema.processTriple(TestUtils.statement(SKOS.RELATED,
            RDF.TYPE, OWL.SYMMETRICPROPERTY));
        Assert.assertTrue("Property should be identified as symmetric",
            schema.getProperty(SKOS.RELATED).isSymmetric());
    }
    @Test
    public void testInputAsymmetric() throws Exception {
        Schema schema = new Schema();
        schema.processTriple(TestUtils.statement(TestUtils.uri("hasChild"),
            RDF.TYPE, OWL2.ASYMMETRICPROPERTY));
        Assert.assertTrue("Property should be identified as asymmetric",
            schema.getProperty(TestUtils.uri("hasChild")).isAsymmetric());
    }
    @Test
    public void testInputIrreflexive() throws Exception {
        Schema schema = new Schema();
        schema.processTriple(TestUtils.statement(TestUtils.uri("hasParent"),
            RDF.TYPE, OWL2.IRREFLEXIVEPROPERTY));
        Assert.assertTrue("Property should be identified as irreflexive",
            schema.getProperty(TestUtils.uri("hasParent")).isIrreflexive());
    }
    @Test
    public void testInputTransitive() throws Exception {
        Schema schema = new Schema();
        schema.processTriple(TestUtils.statement(lubm("subOrganizationOf"),
            RDF.TYPE, OWL.TRANSITIVEPROPERTY));
        Assert.assertTrue("Property should be identified as transitive",
            schema.getProperty(lubm("subOrganizationOf")).isTransitive());
    }
    @Test
    public void testInputDisjointProperty() throws Exception {
        Schema schema = new Schema();
        schema.processTriple(TestUtils.statement(TestUtils.uri("p1"),
            OWL2.PROPERTYDISJOINTWITH, TestUtils.uri("p2")));
        Assert.assertTrue("(x propertyDisjointWith y): y not one of x's disjoint properties",
            schema.getProperty(TestUtils.uri("p1")).getDisjointProperties().contains(TestUtils.uri("p2")));
    }
    @Test
    public void testInputDisjointPropertyReverse() throws Exception {
        Schema schema = new Schema();
        schema.processTriple(TestUtils.statement(TestUtils.uri("p1"),
            OWL2.PROPERTYDISJOINTWITH, TestUtils.uri("p2")));
        Assert.assertTrue("(x propertyDisjointWith y): x not one of y's disjoint properties",
            schema.getProperty(TestUtils.uri("p2")).getDisjointProperties().contains(TestUtils.uri("p1")));
    }

    @Test
    public void testInputOnPropertyRestriction() throws Exception {
        Schema schema = new Schema();
        schema.processTriple(TestUtils.statement(TestUtils.uri("x"),
            OWL.ONPROPERTY, lubm("headOf")));
        Assert.assertTrue("onProperty not returned for restriction x",
            schema.getClass(TestUtils.uri("x")).getOnProperty().contains(lubm("headOf")));
    }
    @Test
    public void testInputOnPropertyReverse() throws Exception {
        Schema schema = new Schema();
        schema.processTriple(TestUtils.statement(TestUtils.uri("x"),
            OWL.ONPROPERTY, lubm("headOf")));
        Assert.assertTrue("onProperty restriction not returned given property",
            schema.getProperty(lubm("headOf")).getRestrictions().contains(TestUtils.uri("x")));
    }
    @Test
    public void testInputSvfRestriction() throws Exception {
        Schema schema = new Schema();
        schema.processTriple(TestUtils.statement(TestUtils.uri("x"),
            OWL.SOMEVALUESFROM, lubm("Department")));
        Assert.assertTrue("target of someValuesFrom restriction not returned",
            schema.getClass(TestUtils.uri("x")).someValuesFrom().contains(lubm("Department")));
    }
    @Test
    public void testInputAvfRestriction() throws Exception {
        Schema schema = new Schema();
        schema.processTriple(TestUtils.statement(TestUtils.uri("x"),
            OWL.ALLVALUESFROM, lubm("Department")));
        Assert.assertTrue("target of allValuesFrom restriction not returned",
            schema.getClass(TestUtils.uri("x")).allValuesFrom().contains(lubm("Department")));
    }
    @Test
    public void testInputOnClassRestriction() throws Exception {
        Schema schema = new Schema();
        schema.processTriple(TestUtils.statement(TestUtils.uri("x"),
            OWL2.ONCLASS, lubm("Department")));
        Assert.assertTrue("onClass not returned for restriction x",
            schema.getClass(TestUtils.uri("x")).onClass().contains(lubm("Department")));
    }
    @Test
    public void testInputHasValueRestriction() throws Exception {
        Schema schema = new Schema();
        schema.processTriple(TestUtils.statement(TestUtils.uri("x"),
            OWL.HASVALUE, TestUtils.uri("y")));
        Assert.assertTrue("hasValue not returned for restriction x",
            schema.getClass(TestUtils.uri("x")).hasValue().contains(TestUtils.uri("y")));
    }

    @Test
    public void testInputMaxCardinality() throws Exception {
        Schema schema = new Schema();
        URI s = TestUtils.uri("x");
        URI p = OWL.MAXCARDINALITY;
        schema.processTriple(TestUtils.statement(s, p, TestUtils.stringLiteral("7")));
        schema.processTriple(TestUtils.statement(s, p, TestUtils.stringLiteral("4")));
        schema.processTriple(TestUtils.statement(s, p, TestUtils.stringLiteral("-1")));
        schema.processTriple(TestUtils.statement(s, p, TestUtils.stringLiteral("3")));
        schema.processTriple(TestUtils.statement(s, p, TestUtils.stringLiteral("5")));
        Assert.assertEquals("Incorrect value returned for maxCardinality", 3,
            schema.getClass(s).getMaxCardinality());
    }

    @Test
    public void testInputMaxQualifiedCardinality() throws Exception {
        Schema schema = new Schema();
        URI s = TestUtils.uri("x");
        URI p = OWL2.MAXQUALIFIEDCARDINALITY;
        schema.processTriple(TestUtils.statement(s, p, TestUtils.stringLiteral("-20")));
        schema.processTriple(TestUtils.statement(s, p, TestUtils.stringLiteral("100")));
        schema.processTriple(TestUtils.statement(s, p, TestUtils.stringLiteral("0")));
        schema.processTriple(TestUtils.statement(s, p, TestUtils.stringLiteral("42")));
        Assert.assertEquals("Incorrect value returned for maxQualifiedCardinality", 0,
            schema.getClass(s).getMaxQualifiedCardinality());
    }
}
