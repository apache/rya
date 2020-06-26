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
import com.sun.org.apache.xerces.internal.jaxp.datatype.XMLGregorianCalendarImpl;
import org.apache.rya.api.domain.RangeIRI;
import org.apache.rya.api.domain.RangeValue;
import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.api.domain.RyaRange;
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests the methods of {@link RdfToRyaConversions}.
 */
public class RdfToRyaConversionsTest {
    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    private static final Set<String> LANGUAGE_CODES = LanguageCodesTestHelper.getInstance().getLanguageCodes();

    @Test
    public void testConvertLiteral_null() {
        assertNull(RdfToRyaConversions.convertLiteral(null));
    }

    @Test
    public void testConvertLiteral_nullDataType() {
        final Literal literal = mock(SimpleLiteral.class);
        final String expectedData = "Ice Cream";
        when(literal.getLabel()).thenReturn(expectedData);
        when(literal.stringValue()).thenReturn(expectedData);
        // Don't think this is possible but test anyways. Need to mock to force this null value.
        when(literal.getDatatype()).thenReturn(null);
        final RyaValue ryaType = RdfToRyaConversions.convertLiteral(literal);
        final RyaValue expected = new RyaType(XMLSchema.STRING, expectedData);
        assertEquals(expected, ryaType);
        assertNull(ryaType.getLanguage());
    }

    @Test
    public void testConvertLiteral_validLanguage() {
        final String expectedData = "Hello";
        for (final String language : LANGUAGE_CODES) {
            // This only checks the validity of the format. Not that the language tag actually exists.
            assertTrue(Literals.isValidLanguageTag(language));
            final Literal literal = VF.createLiteral(expectedData, language);
            final RyaValue ryaType = RdfToRyaConversions.convertLiteral(literal);
            assertEquals(RDF.LANGSTRING, ryaType.getDataType());
            assertEquals(expectedData, ryaType.getData());
            assertEquals(language, ryaType.getLanguage());
            final RyaValue expectedRyaType = new RyaType(RDF.LANGSTRING, expectedData, language);
            assertEquals(expectedRyaType, ryaType);
        }
    }

    @Test
    public void testConvertLiteral_undeterminedLanguage() {
        final String expectedData = "Hello";
        final String language = LiteralLanguageUtils.UNDETERMINED_LANGUAGE;
        assertTrue(Literals.isValidLanguageTag(language));
        final Literal literal = VF.createLiteral(expectedData, language);
        final RyaValue ryaType = RdfToRyaConversions.convertLiteral(literal);
        assertEquals(RDF.LANGSTRING, ryaType.getDataType());
        assertEquals(expectedData, ryaType.getData());
        final RyaValue expectedRyaType = new RyaType(RDF.LANGSTRING, expectedData, language);
        assertEquals(expectedRyaType, ryaType);
        assertEquals(LiteralLanguageUtils.UNDETERMINED_LANGUAGE, ryaType.getLanguage());
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
            final Literal literal = VF.createLiteral(expectedData, badLanguage);
            final RyaValue ryaType = RdfToRyaConversions.convertLiteral(literal);
            assertEquals(RDF.LANGSTRING, ryaType.getDataType());
            assertEquals(expectedData, ryaType.getData());
            // Check that the invalid language is replaced with "und"
            assertEquals(LiteralLanguageUtils.UNDETERMINED_LANGUAGE, ryaType.getLanguage());
        }
    }

    @Test
    public void testConvertLiteral_normalString() {
        final String expectedData = "Hello";
        final Literal literal = VF.createLiteral(expectedData);
        final RyaValue ryaType = RdfToRyaConversions.convertLiteral(literal);
        assertEquals(XMLSchema.STRING, ryaType.getDataType());
        assertEquals(expectedData, ryaType.getData());
        final RyaValue expectedRyaType = new RyaType(XMLSchema.STRING, expectedData);
        assertEquals(expectedRyaType, ryaType);
        assertNull(ryaType.getLanguage());
    }

    @Test
    public void testConvertValue() {
        final String expectedData = "urn:the/test";
        final IRI iri = VF.createIRI(expectedData);
        final RyaValue ryaType = RdfToRyaConversions.convertValue(iri);
        assertEquals(XMLSchema.ANYURI, ryaType.getDataType());
        assertEquals(expectedData, ryaType.getData());
        final RyaValue expectedRyaType = new RyaType(XMLSchema.ANYURI, expectedData);
        assertEquals(expectedRyaType, ryaType);
        assertNull(ryaType.getLanguage());
    }

    @Test
    public void testConvertLiteralValue() {
        final String expectedData = "Value";
        final Literal value = VF.createLiteral(expectedData);
        final RyaValue ryaType = RdfToRyaConversions.convertValue(value);
        assertEquals(XMLSchema.STRING, ryaType.getDataType());
        assertEquals(expectedData, ryaType.getData());
        final RyaValue expectedRyaType = new RyaType(XMLSchema.STRING, expectedData);
        assertEquals(expectedRyaType, ryaType);
        assertNull(ryaType.getLanguage());
    }

    @Test
    public void testConvertRyaValue() {
        final String expectedData = "urn:the/test";
        final RyaValue value = new RyaType(expectedData);
        final RyaValue ryaType = RdfToRyaConversions.convertValue(value);
        assertEquals(XMLSchema.STRING, ryaType.getDataType());
        assertEquals(expectedData, ryaType.getData());
        final RyaValue expectedRyaType = new RyaType(XMLSchema.STRING, expectedData);
        assertEquals(expectedRyaType, ryaType);
        assertNull(ryaType.getLanguage());
    }

    @Test
    public void testConvertRangeValueIRI() {
        final RangeValue<Resource> rangeValue = new RangeValue<>(VF.createIRI("urn:abc"), VF.createIRI("urn:xyz"));
        final Value range = new RangeIRI(rangeValue);
        final RyaRange ryaType = (RyaRange) RdfToRyaConversions.convertValue(range);
        assertEquals(XMLSchema.ANYURI, ryaType.getStart().getDataType());
        assertEquals("urn:abc", ryaType.getStart().getData());
        assertNull(ryaType.getStart().getLanguage());
        assertEquals(XMLSchema.ANYURI, ryaType.getStop().getDataType());
        assertEquals("urn:xyz", ryaType.getStop().getData());
        assertNull(ryaType.getStop().getLanguage());
    }

    @Test
    public void testConvertIRI() {
        final String expectedData = "urn:the/test";
        final IRI iri = VF.createIRI(expectedData);
        final RyaIRI ryaType = RdfToRyaConversions.convertIRI(iri);
        assertEquals(XMLSchema.ANYURI, ryaType.getDataType());
        assertEquals(expectedData, ryaType.getData());
        final RyaValue expectedRyaType = new RyaType(XMLSchema.ANYURI, expectedData);
        assertEquals(expectedRyaType, ryaType);
        assertNull(ryaType.getLanguage());
    }

    @Test
    public void testConvertRyaIRI() {
        final String expectedData = "urn:the/test";
        final IRI iri = new RyaIRI(expectedData);
        final RyaIRI ryaType = RdfToRyaConversions.convertIRI(iri);
        assertEquals(XMLSchema.ANYURI, ryaType.getDataType());
        assertEquals(expectedData, ryaType.getData());
        final RyaValue expectedRyaType = new RyaType(XMLSchema.ANYURI, expectedData);
        assertEquals(expectedRyaType, ryaType);
        assertNull(ryaType.getLanguage());
    }

    @Test
    public void testConvertRangeIRI() {
        final RangeValue<Resource> rangeValue = new RangeValue<>(VF.createIRI("urn:abc"), VF.createIRI("urn:xyz"));
        final IRI rangeIRI = new RangeIRI(rangeValue);
        final RyaRange ryaType = (RyaRange) RdfToRyaConversions.convertIRI(rangeIRI);
        assertEquals(XMLSchema.ANYURI, ryaType.getStart().getDataType());
        assertEquals("urn:abc", ryaType.getStart().getData());
        assertNull(ryaType.getStart().getLanguage());
        assertEquals(XMLSchema.ANYURI, ryaType.getStop().getDataType());
        assertEquals("urn:xyz", ryaType.getStop().getData());
        assertNull(ryaType.getStop().getLanguage());
    }

    @Test
    public void testConvertResource() {
        final String expectedData = "urn:the/test";
        final IRI iri = VF.createIRI(expectedData);
        final RyaResource ryaType = RdfToRyaConversions.convertResource(iri);
        assertEquals(XMLSchema.ANYURI, ryaType.getDataType());
        assertEquals(expectedData, ryaType.getData());
        final RyaValue expectedRyaType = new RyaType(XMLSchema.ANYURI, expectedData);
        assertEquals(expectedRyaType, ryaType);
        assertNull(ryaType.getLanguage());
    }

    @Test
    public void testConvertResourceRyaIRI() {
        final String expectedData = "urn:the/test";
        final IRI iri = new RyaIRI(expectedData);
        final RyaResource ryaType = RdfToRyaConversions.convertResource(iri);
        assertEquals(XMLSchema.ANYURI, ryaType.getDataType());
        assertEquals(expectedData, ryaType.getData());
        final RyaValue expectedRyaType = new RyaType(XMLSchema.ANYURI, expectedData);
        assertEquals(expectedRyaType, ryaType);
        assertNull(ryaType.getLanguage());
    }

    @Test
    public void testConvertResources() {
        final String expectedData1 = "urn:the/test1";
        final String expectedData2 = "urn:the/test2";
        final IRI[] iri = {VF.createIRI(expectedData1), VF.createIRI(expectedData2)};
        final RyaResource[] ryaType = RdfToRyaConversions.convertResource(iri);
        assertEquals(2, ryaType.length);

        assertEquals(XMLSchema.ANYURI, ryaType[0].getDataType());
        assertEquals(expectedData1, ryaType[0].getData());
        final RyaValue expectedRyaType1 = new RyaType(XMLSchema.ANYURI, expectedData1);
        assertEquals(expectedRyaType1, ryaType[0]);
        assertNull(ryaType[0].getLanguage());

        assertEquals(XMLSchema.ANYURI, ryaType[1].getDataType());
        assertEquals(expectedData2, ryaType[1].getData());
        final RyaValue expectedRyaType2 = new RyaType(XMLSchema.ANYURI, expectedData2);
        assertEquals(expectedRyaType2, ryaType[1]);
        assertNull(ryaType[1].getLanguage());
    }

    @Test
    public void testConvertStatement() {
        final Resource subject = VF.createIRI("urn:subject");
        final IRI predicate = VF.createIRI("urn:predicate");
        final Value object = VF.createIRI("urn:object");
        final Resource context = VF.createIRI("urn:context");
        final Statement statement = VF.createStatement(subject, predicate, object, context);
        final RyaStatement ryaType = RdfToRyaConversions.convertStatement(statement);
        assertEquals(subject.stringValue(), ryaType.getSubject().stringValue());
        assertEquals(predicate.stringValue(), ryaType.getPredicate().stringValue());
        assertEquals(object.stringValue(), ryaType.getObject().stringValue());
        assertEquals("urn:", ((IRI)statement.getObject()).getNamespace());
        assertEquals("object", ((IRI)statement.getObject()).getLocalName());
        assertEquals(context.stringValue(), ryaType.getContext().stringValue());
        //assertSame(ryaType, RdfToRyaConversions.convertStatement((Statement) ryaType));
    }

    @Test
    public void testConvertStatementLiteral() {
        final Resource subject = VF.createIRI("urn:subject");
        final IRI predicate = VF.createIRI("urn:predicate");
        final Value object = VF.createLiteral("Some String");
        final Resource context = VF.createIRI("urn:context");
        final Statement statement = VF.createStatement(subject, predicate, object, context);
        final RyaStatement ryaStatement = RdfToRyaConversions.convertStatement(statement);
        assertEquals(subject.stringValue(), ryaStatement.getSubject().stringValue());
        assertEquals(predicate.stringValue(), ryaStatement.getPredicate().stringValue());
        assertEquals(object.stringValue(), ryaStatement.getObject().stringValue());
        assertEquals("Some String", ryaStatement.getObject().getData());
        assertEquals(XMLSchema.STRING, ryaStatement.getObject().getDataType());
        assertNull(ryaStatement.getObject().getLanguage());
        assertEquals(context.stringValue(), ryaStatement.getContext().stringValue());
    }

    @Test
    public void testConvertStatementLiteralDouble() {
        final Resource subject = VF.createIRI("urn:subject");
        final IRI predicate = VF.createIRI("urn:predicate");
        final Value object = VF.createLiteral(12.345);
        final Resource context = VF.createIRI("urn:context");
        final Statement statement = VF.createStatement(subject, predicate, object, context);
        final RyaStatement ryaStatement = RdfToRyaConversions.convertStatement(statement);
        assertEquals(subject.stringValue(), ryaStatement.getSubject().stringValue());
        assertEquals(predicate.stringValue(), ryaStatement.getPredicate().stringValue());
        assertEquals(object.stringValue(), ryaStatement.getObject().stringValue());
        assertEquals("12.345", ryaStatement.getObject().getData());
        assertEquals(XMLSchema.DOUBLE, ryaStatement.getObject().getDataType());
        assertNull(ryaStatement.getObject().getLanguage());
        assertEquals(context.stringValue(), ryaStatement.getContext().stringValue());
    }

    @Test
    public void testConvertStatementLiteralDate() {
        final Resource subject = VF.createIRI("urn:subject");
        final IRI predicate = VF.createIRI("urn:predicate");
        final Value object = VF.createLiteral(XMLGregorianCalendarImpl.createDate(2020, 05, 07, 5 * 60));
        final Resource context = VF.createIRI("urn:context");
        final Statement statement = VF.createStatement(subject, predicate, object, context);
        final RyaStatement ryaStatement = RdfToRyaConversions.convertStatement(statement);
        assertEquals(subject.stringValue(), ryaStatement.getSubject().stringValue());
        assertEquals(predicate.stringValue(), ryaStatement.getPredicate().stringValue());
        assertEquals(object.stringValue(), ryaStatement.getObject().stringValue());
        assertEquals("2020-05-07+05:00", ryaStatement.getObject().getData());
        assertEquals(XMLSchema.DATE, ryaStatement.getObject().getDataType());
        assertNull(ryaStatement.getObject().getLanguage());
        assertEquals(context.stringValue(), ryaStatement.getContext().stringValue());
    }

}