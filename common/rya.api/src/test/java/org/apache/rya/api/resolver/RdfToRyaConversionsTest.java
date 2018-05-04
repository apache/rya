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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Set;

import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.utils.LiteralLanguageUtils;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleLiteral;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.Literals;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Tests the methods of {@link RdfToRyaConversions}.
 */
public class RdfToRyaConversionsTest {
    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    private static final Set<String> LANGUAGE_CODES = LanguageCodesTestHelper.getInstance().getLanguageCodes();

    @Test
    public void testConvertLiteral_null() {
        final RyaType ryaType = RdfToRyaConversions.convertLiteral(null);
        assertNull(ryaType);
    }

    @Test
    public void testConvertLiteral_nullDataType() {
        final Literal literal = mock(SimpleLiteral.class);
        final String expectedData = "Ice Cream";
        when(literal.getLabel()).thenReturn(expectedData);
        when(literal.stringValue()).thenReturn(expectedData);
        // Don't think this is possible but test anyways. Need to mock to force this null value.
        when(literal.getDatatype()).thenReturn(null);
        final RyaType ryaType = RdfToRyaConversions.convertLiteral(literal);
        final RyaType expected = new RyaType(XMLSchema.STRING, expectedData);
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
            final RyaType ryaType = RdfToRyaConversions.convertLiteral(literal);
            assertEquals(RDF.LANGSTRING, ryaType.getDataType());
            assertEquals(expectedData, ryaType.getData());
            assertEquals(language, ryaType.getLanguage());
            final RyaType expectedRyaType = new RyaType(RDF.LANGSTRING, expectedData, language);
            assertEquals(expectedRyaType, ryaType);
        }
    }

    @Test
    public void testConvertLiteral_undeterminedLanguage() {
        final String expectedData = "Hello";
        final String language = LiteralLanguageUtils.UNDETERMINED_LANGUAGE;
        assertTrue(Literals.isValidLanguageTag(language));
        final Literal literal = VF.createLiteral(expectedData, language);
        final RyaType ryaType = RdfToRyaConversions.convertLiteral(literal);
        assertEquals(RDF.LANGSTRING, ryaType.getDataType());
        assertEquals(expectedData, ryaType.getData());
        final RyaType expectedRyaType = new RyaType(RDF.LANGSTRING, expectedData, language);
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
            final RyaType ryaType = RdfToRyaConversions.convertLiteral(literal);
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
        final RyaType ryaType = RdfToRyaConversions.convertLiteral(literal);
        assertEquals(XMLSchema.STRING, ryaType.getDataType());
        assertEquals(expectedData, ryaType.getData());
        final RyaType expectedRyaType = new RyaType(XMLSchema.STRING, expectedData);
        assertEquals(expectedRyaType, ryaType);
        assertNull(ryaType.getLanguage());
    }
}