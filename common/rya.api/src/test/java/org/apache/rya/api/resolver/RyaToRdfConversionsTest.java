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
package org.apache.rya.api.resolver;

import com.google.common.collect.Lists;
import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.api.domain.RyaResource;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaValue;
import org.apache.rya.api.utils.LiteralLanguageUtils;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleLiteral;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.Literals;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests the methods of {@link RyaToRdfConversionsTest}.
 */
public class RyaToRdfConversionsTest {
    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    private static final Set<String> LANGUAGE_CODES = LanguageCodesTestHelper.getInstance().getLanguageCodes();

    @Test
    public void testConvertLiteral_null() {
        assertNull(RyaToRdfConversions.convertLiteral(null));
    }

    @Test
    public void testConvertLiteral_nullDataType() {
        final String expectedData = "Ice Cream";
        final RyaType ryaType = new RyaType(null, expectedData);
        final Literal literal = RyaToRdfConversions.convertLiteral(ryaType);
        final Literal expected = VF.createLiteral(expectedData, XMLSchema.STRING);
        assertEquals(expected, literal);
        assertFalse(literal.getLanguage().isPresent());
    }

    @Test
    public void testConvertLiteral_validLanguage() {
        final String expectedData = "Hello";
        for (final String language : LANGUAGE_CODES) {
            // This only checks the validity of the format. Not that the language tag actually exists.
            assertTrue(Literals.isValidLanguageTag(language));
            final RyaType ryaType = new RyaType(RDF.LANGSTRING, expectedData, language);
            final Literal literal = RyaToRdfConversions.convertLiteral(ryaType);
            assertEquals(RDF.LANGSTRING, literal.getDatatype());
            assertEquals(expectedData, literal.getLabel());
            assertTrue(literal.getLanguage().isPresent());
            assertEquals(language, literal.getLanguage().get());
            final Literal expectedLiteral = VF.createLiteral(expectedData, language);
            assertEquals(expectedLiteral, literal);
        }
    }

    @Test
    public void testConvertLiteral_undeterminedLanguage() {
        final String expectedData = "Hello";
        final String language = LiteralLanguageUtils.UNDETERMINED_LANGUAGE;
        assertTrue(Literals.isValidLanguageTag(language));
        final RyaType ryaType = new RyaType(RDF.LANGSTRING, expectedData, language);
        final Literal literal = RyaToRdfConversions.convertLiteral(ryaType);
        assertEquals(RDF.LANGSTRING, literal.getDatatype());
        assertEquals(expectedData, literal.getLabel());
        assertTrue(literal.getLanguage().isPresent());
        assertEquals(LiteralLanguageUtils.UNDETERMINED_LANGUAGE, literal.getLanguage().get());
        final Literal expectedLiteral = VF.createLiteral(expectedData, language);
        assertEquals(expectedLiteral, literal);
    }

    @Test
    public void testConvertLiteral_invalidLanguage() {
        final String expectedData = "Hello";
        final List<String> badLanguages = Lists.newArrayList(
                "bad language",
                "en-",
                "en-US-"
        );
        for (final String badLanguage : badLanguages) {
            // This only checks the validity of the format. Not that the language tag actually exists.
            assertFalse(Literals.isValidLanguageTag(badLanguage));
            final RyaType ryaType = new RyaType(RDF.LANGSTRING, expectedData, badLanguage);
            final Literal literal = RyaToRdfConversions.convertLiteral(ryaType);
            assertEquals(RDF.LANGSTRING, literal.getDatatype());
            assertEquals(expectedData, literal.getLabel());
            assertTrue(literal.getLanguage().isPresent());
            // Check that the invalid language is replaced with "und"
            assertEquals(LiteralLanguageUtils.UNDETERMINED_LANGUAGE, literal.getLanguage().get());
        }
    }

    @Test
    public void testConvertLiteral_normalString() {
        final String expectedData = "Hello";
        final RyaType ryaType = new RyaType(XMLSchema.STRING, expectedData);
        final Literal literal = RyaToRdfConversions.convertLiteral(ryaType);
        assertEquals(XMLSchema.STRING, literal.getDatatype());
        assertEquals(expectedData, literal.getLabel());
        assertFalse(literal.getLanguage().isPresent());
        final Literal expectedLiteral = VF.createLiteral(expectedData);
        assertEquals(expectedLiteral, literal);
    }
    
    @Test
    public void testConvertValue() {
        final String expectedData = "urn:the/test";
        final RyaValue ryaValue = new RyaIRI(expectedData);
        final Value value = RyaToRdfConversions.convertValue(ryaValue);
        assertEquals(expectedData, value.stringValue());
        assertTrue(value.getClass().getName(), value instanceof IRI);
    }

    @Test
    public void testConvertLiteralValue() {
        final String expectedData = "Value";
        final RyaValue ryaValue = new RyaType(expectedData);
        final Value value = RyaToRdfConversions.convertValue(ryaValue);
        assertEquals(expectedData, value.stringValue());
        assertTrue(value.getClass().getName(), value instanceof Literal);
    }

    @Test
    public void testConvertIRIValueByDataType() {
        final String expectedData = "urn:the/test";
        final RyaValue ryaValue = new RyaType(XMLSchema.ANYURI, expectedData);
        final Value value = RyaToRdfConversions.convertValue(ryaValue);
        assertEquals(expectedData, value.stringValue());
        assertTrue(value.getClass().getName(), value instanceof IRI);
    }

    @Test
    public void testConvertIRI() {
        final String expectedData = "urn:the/test";
        final RyaResource ryaResource = new RyaIRI(expectedData);
        final Resource resource = RyaToRdfConversions.convertResource(ryaResource);
        assertEquals(expectedData, resource.stringValue());
        assertTrue(resource.getClass().getName(), resource instanceof IRI);
    }

    @Test
    public void testConvertResource() {
        final String expectedData = "urn:the/test";
        final RyaIRI ryaIRI = new RyaIRI(expectedData);
        final IRI iri = RyaToRdfConversions.convertIRI(ryaIRI);
        assertEquals(expectedData, iri.toString());
        assertEquals("test", iri.getLocalName());
        assertEquals("urn:the/", iri.getNamespace());
    }

    @Test
    public void testConvertResources() {
        final String expectedData1 = "urn:the/test1";
        final String expectedData2 = "urn:the/test2";
        final RyaResource[] ryaResources = {new RyaIRI(expectedData1), new RyaIRI(expectedData2)};
        final Resource[] resources = RyaToRdfConversions.convertResource(ryaResources);
        assertEquals(2, resources.length);

        assertEquals(expectedData1, ((IRI)resources[0]).toString());
        assertEquals("test1", ((IRI)resources[0]).getLocalName());
        assertEquals("urn:the/", ((IRI)resources[0]).getNamespace());

        assertEquals(expectedData2, ((IRI)resources[1]).toString());
        assertEquals("test2", ((IRI)resources[1]).getLocalName());
        assertEquals("urn:the/", ((IRI)resources[1]).getNamespace());
    }

    @Test
    public void testConvertStatement() {
        final RyaResource subject = new RyaIRI("urn:subject");
        final RyaIRI predicate = new RyaIRI("urn:predicate");
        final RyaValue object = new RyaIRI("urn:object");
        final RyaResource context = new RyaIRI("urn:context");
        final RyaStatement ryaStatement = new RyaStatement(subject, predicate, object, context);
        final Statement statement = RyaToRdfConversions.convertStatement(ryaStatement);
        assertEquals(subject.stringValue(), statement.getSubject().stringValue());
        assertEquals(predicate.stringValue(), statement.getPredicate().stringValue());
        assertEquals(object.stringValue(), statement.getObject().stringValue());
        assertEquals(context.stringValue(), statement.getContext().stringValue());
    }

    @Test
    public void testConvertStatementLiteral() {
        final RyaResource subject = new RyaIRI("urn:subject");
        final RyaIRI predicate = new RyaIRI("urn:predicate");
        final RyaValue object = new RyaType(XMLSchema.DOUBLE, "12.345");
        final RyaResource context = new RyaIRI("urn:context");
        final RyaStatement ryaStatement = new RyaStatement(subject, predicate, object, context);
        final Statement statement = RyaToRdfConversions.convertStatement(ryaStatement);
        assertEquals(subject.stringValue(), statement.getSubject().stringValue());
        assertEquals(predicate.stringValue(), statement.getPredicate().stringValue());
        assertEquals(object.stringValue(), statement.getObject().stringValue());
        assertEquals(12.345, ((SimpleLiteral)statement.getObject()).doubleValue(), 0.0001);
        assertFalse(((SimpleLiteral)statement.getObject()).getLanguage().isPresent());
        assertEquals(XMLSchema.DOUBLE, ((SimpleLiteral)statement.getObject()).getDatatype());
        assertEquals(context.stringValue(), statement.getContext().stringValue());
    }
    
}