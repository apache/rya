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
package org.apache.rya.api.utils;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.util.Literals;
import org.eclipse.rdf4j.model.vocabulary.RDF;

/**
 * Utility methods and constants for {@link Literal} languages.
 */
public final class LiteralLanguageUtils {
    /**
     * Special identifier used if there is language content, but the specific
     * language cannot be determined. Should be avoided if possible.
     * See <a href=https://www.loc.gov/standards/iso639-2/faq.html#25>here</a>
     * for more info.
     */
    public static final String UNDETERMINED_LANGUAGE = "und";

    /**
     * Delimiter between the data and the language tag.
     */
    public static final String LANGUAGE_DELIMITER = "@";

    /**
     * Private constructor to prevent instantiation.
     */
    private LiteralLanguageUtils() {
    }

    /**
     * Validates the language based on the data type.
     * <p>
     * This will do one of the following:
     * <ul>
     *   <li>Return the original {@code language} if the {@code dataType} is
     *       {@link RDF#LANGSTRING} and it's of a VALID format.</li>
     *   <li>Returns {@link UNDETERMINED_LANGUAGE} if the {@code dataType} is
     *       {@link RDF#LANGSTRING} and it's of an INVALID format.</li>
     *   <li>Return {@code null} if the dataType is NOT {@link RDF#LANGSTRING}.</li>
     * </ul>
     * @param language the language to validate.
     * @param dataType the {@link IRI} data type to validate against.
     * @return the validated language.
     */
    public static String validateLanguage(final String language, final IRI dataType) {
        String result = null;
        if (RDF.LANGSTRING.equals(dataType)) {
            if (language != null && Literals.isValidLanguageTag(language)) {
                result = language;
            } else {
                result = UNDETERMINED_LANGUAGE;
            }
        }
        return result;
    }
}