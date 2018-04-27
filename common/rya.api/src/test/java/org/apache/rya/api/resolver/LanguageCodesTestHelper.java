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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

/**
 * Helper class for reading language code files and generating a list of
 * languages to test.
 */
public final class LanguageCodesTestHelper {
    private static final String ALPHA_2_LANGUAGE_CODE_FILE = "src/test/resources/ISO-639-1_Language_Codes.txt";
    private static final String ALPHA_3_LANGUAGE_CODE_FILE = "src/test/resources/ISO-639-2_Language_Codes.txt";
    private static final String COUNTRY_CODE_FILE = "src/test/resources/ISO-3166-1_Country_Codes.txt";

    private Set<String> languageCodes;

    /**
     * Private constructor to enforce singleton pattern.
     * @throws Exception
     */
    private LanguageCodesTestHelper() throws Exception {
        setupLanguageCodes();
    }

    private static class InstanceHolder {
        private static final LanguageCodesTestHelper INSTANCE;
        static {
            try {
                INSTANCE = new LanguageCodesTestHelper();
            } catch (final Exception e) {
                throw new ExceptionInInitializerError(e);
            }
        }
    }

    /**
     * @return the singleton instance of {@link LanguageCodesTestHelper}.
     */
    public static LanguageCodesTestHelper getInstance(){
        return InstanceHolder.INSTANCE;
    }

    /**
     * Generates a list of language codes with country codes to test. This tries
     * to reproduce BCP-47 compliant language tags as specified by
     * <a href="https://tools.ietf.org/html/bcp47">BCP47</a>.
     * <p>
     * Adds all of the following combinations for testing:
     * <ul>
     *   <li>Alpha-2 language code only</li>
     *   <li>Alpha-3 language code only</li>
     *   <li>Alpha-2 language code + Country code</li>
     *   <li>Alpha-3 language code + Country code</li>
     * </ul>
     * This should produce most common combinations and a lot of unlikely ones
     * too.
     * @throws Exception
     */
    private void setupLanguageCodes() throws Exception {
        final List<String> alpha2LangCodes = readCodeFile(ALPHA_2_LANGUAGE_CODE_FILE);
        final List<String> alpha3LangCodes = readCodeFile(ALPHA_3_LANGUAGE_CODE_FILE);
        final List<String> countryCodes = readCodeFile(COUNTRY_CODE_FILE);

        // Generate all combinations of language codes and region codes.
        final List<String> langCodes = new ArrayList<>();
        langCodes.addAll(alpha2LangCodes);
        langCodes.addAll(alpha3LangCodes);
        for (final String languageCode : alpha2LangCodes) {
            for (final String countryCode : countryCodes) {
                langCodes.add(languageCode + "-" + countryCode);
            }
        }
        for (final String languageCode : alpha3LangCodes) {
            for (final String countryCode : countryCodes) {
                langCodes.add(languageCode + "-" + countryCode);
            }
        }
        languageCodes = ImmutableSet.copyOf(langCodes);
    }

    private static List<String> readCodeFile(final String fileName) throws IOException {
        final List<String> codes = new ArrayList<>();
        // Read each line
        try (final Stream<String> stream = Files.lines(Paths.get(fileName))) {
            // Each line might be comma-separated so add multiple codes per line
            stream.forEach(line -> codes.addAll(Lists.newArrayList(line.split(","))));
        }
        return codes;
    }

    /**
     * @return the {@link Set} of language codes.
     */
    public Set<String> getLanguageCodes() {
        return languageCodes;
    }
}