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
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Set;

import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.utils.LiteralLanguageUtils;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.Literals;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Tests the methods of {@link RyaToRdfConversionsTest}.
 */
public class RyaToRdfConversionsTest {
    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    private static final Set<String> LANGUAGE_CODES = LanguageCodesTestHelper.getInstance().getLanguageCodes();

    @Test (expected=NullPointerException.class)
    public void testConvertLiteral_null() {
        RyaToRdfConversions.convertLiteral(null);
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
}