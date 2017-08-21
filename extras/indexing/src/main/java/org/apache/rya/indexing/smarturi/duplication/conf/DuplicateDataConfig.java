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
package org.apache.rya.indexing.smarturi.duplication.conf;

import static java.util.Objects.requireNonNull;

import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.rya.indexing.smarturi.duplication.Tolerance;
import org.apache.rya.indexing.smarturi.duplication.ToleranceType;

/**
 * Configuration options for data duplication.
 */
public class DuplicateDataConfig {
    public static final String DEFAULT_CONFIG_FILE_PATH = "conf/duplicate_data_detection_config.xml";

    private Tolerance booleanTolerance;
    private Tolerance byteTolerance;
    private Tolerance dateTolerance;
    private Tolerance doubleTolerance;
    private Tolerance floatTolerance;
    private Tolerance integerTolerance;
    private Tolerance longTolerance;
    private Tolerance shortTolerance;
    private Tolerance stringTolerance;
    private Tolerance uriTolerance;

    private Map<String, List<String>> equivalentTermsMap;

    private boolean isDetectionEnabled;

    /**
     * Creates a new instance of {@link DuplicateDataConfig}.
     * @throws ConfigurationException
     */
    public DuplicateDataConfig() throws ConfigurationException {
        this(new XMLConfiguration(DEFAULT_CONFIG_FILE_PATH));
    }

    /**
     * Creates a new instance of {@link DuplicateDataConfig}.
     * @param xmlFilePath the config's XML file path. (not {@code null})
     * @throws ConfigurationException
     */
    public DuplicateDataConfig(final String xmlFileLocation) throws ConfigurationException {
        this(new XMLConfiguration(requireNonNull(xmlFileLocation)));
    }

    /**
     * Creates a new instance of {@link DuplicateDataConfig}.
     * @param xmlConfig the {@link XMLConfiguration} file. (not {@code null})
     * @throws ConfigurationException
     */
    public DuplicateDataConfig(final XMLConfiguration xmlConfig) throws ConfigurationException {
        requireNonNull(xmlConfig);

        final Tolerance booleanTolerance = parseTolerance("tolerances.booleanTolerance", xmlConfig);
        final Tolerance byteTolerance = parseTolerance("tolerances.byteTolerance", xmlConfig);
        final Tolerance dateTolerance = parseTolerance("tolerances.dateTolerance", xmlConfig);
        final Tolerance doubleTolerance = parseTolerance("tolerances.doubleTolerance", xmlConfig);
        final Tolerance floatTolerance = parseTolerance("tolerances.floatTolerance", xmlConfig);
        final Tolerance integerTolerance = parseTolerance("tolerances.integerTolerance", xmlConfig);
        final Tolerance longTolerance = parseTolerance("tolerances.longTolerance", xmlConfig);
        final Tolerance shortTolerance = parseTolerance("tolerances.shortTolerance", xmlConfig);
        final Tolerance stringTolerance = parseTolerance("tolerances.stringTolerance", xmlConfig);
        final Tolerance uriTolerance = parseTolerance("tolerances.uriTolerance", xmlConfig);

        final Map<String, List<String>> equivalentTermsMap = parseEquivalentTermsMap(xmlConfig);

        final boolean isDetectionEnabled = xmlConfig.getBoolean("enableDetection", false);
        init(booleanTolerance, byteTolerance, dateTolerance, doubleTolerance, floatTolerance, integerTolerance, longTolerance, shortTolerance, stringTolerance, uriTolerance, equivalentTermsMap, isDetectionEnabled);
    }

    /**
     * Creates a new instance of {@link DuplicateDataConfig}.
     * @param booleanTolerance the {@link Boolean} tolerance value or
     * {@code null} if not specified.
     * @param byteTolerance the {@link Byte} tolerance value or {@code null} if
     * not specified.
     * @param dateTolerance the {@link Date} tolerance value or {@code null} if
     * not specified.
     * @param doubleTolerance the {@link Double} tolerance value or {@code null}
     * if not specified.
     * @param floatTolerance the {@link Float} tolerance value or {@code null}
     * if not specified.
     * @param integerTolerance the {@link Integer} tolerance value or
     * {@code null} if not specified.
     * @param longTolerance the {@link Long} tolerance value or {@code null} if
     * not specified.
     * @param shortTolerance the {@link Short} tolerance value or {@code null}
     * if not specified.
     * @param stringTolerance the {@link String} tolerance value or {@code null}
     * if not specified.
     * @param uriTolerance the {@link URI} tolerance value or {@code null} if
     * not specified.
     * @param equivalentTermsMap the {@link Map} of terms that are considered
     * equivalent to each other. (not {@code null})
     * @param isDetectionEnabled {@code true} to enable detection. {@code false}
     * to disable detection.
     */
    public DuplicateDataConfig(final Tolerance booleanTolerance, final Tolerance byteTolerance,
        final Tolerance dateTolerance, final Tolerance doubleTolerance, final Tolerance floatTolerance,
        final Tolerance integerTolerance, final Tolerance longTolerance, final Tolerance shortTolerance,
        final Tolerance stringTolerance, final Tolerance uriTolerance, final Map<String, List<String>> equivalentTermsMap,
        final boolean isDetectionEnabled)
    {
        init(booleanTolerance, byteTolerance, dateTolerance, doubleTolerance, floatTolerance, integerTolerance, longTolerance, shortTolerance, stringTolerance, uriTolerance, equivalentTermsMap, isDetectionEnabled);
    }

    private void init(final Tolerance booleanTolerance, final Tolerance byteTolerance,
        final Tolerance dateTolerance, final Tolerance doubleTolerance, final Tolerance floatTolerance,
        final Tolerance integerTolerance, final Tolerance longTolerance, final Tolerance shortTolerance,
        final Tolerance stringTolerance, final Tolerance uriTolerance, final Map<String, List<String>> equivalentTermsMap,
        final boolean isDetectionEnabled)
    {
        this.booleanTolerance = booleanTolerance;
        this.byteTolerance = byteTolerance;
        this.dateTolerance= dateTolerance;
        this.doubleTolerance = doubleTolerance;
        this.floatTolerance = floatTolerance;
        this.integerTolerance = integerTolerance;
        this.longTolerance = longTolerance;
        this.shortTolerance = shortTolerance;
        this.stringTolerance = stringTolerance;
        this.uriTolerance = uriTolerance;
        this.equivalentTermsMap = requireNonNull(equivalentTermsMap);
        this.isDetectionEnabled = isDetectionEnabled;
    }

    private static Tolerance parseTolerance(final String key, final XMLConfiguration xmlConfig) throws ConfigurationException {
        final String type = xmlConfig.getString(key + ".type", null);
        final ToleranceType toleranceType = ToleranceType.getToleranceTypeByName(type);
        Double doubleValue = null;
        if (toleranceType != null) {
            switch (toleranceType) {
                case PERCENTAGE:
                    final String value = xmlConfig.getString(key + ".value", null);
                    if (value != null && value.contains("%")) {
                        try {
                            final Number number = NumberFormat.getPercentInstance().parse(value);
                            doubleValue = number.doubleValue();
                        } catch (final ParseException e) {
                            throw new ConfigurationException(e);
                        }
                    } else {
                        doubleValue = xmlConfig.getDouble(key + ".value", null);
                    }
                    if (doubleValue != null) {
                        if (doubleValue < 0) {
                            throw new ConfigurationException("The " + toleranceType + " tolerance type for \"" + key + "\" must be a positive value. Found this value: " + doubleValue);
                        }
                        if (doubleValue > 1) {
                            throw new ConfigurationException("The " + toleranceType + " tolerance type for \"" + key + "\" can NOT be greater than 100%. Found this value: " + doubleValue);
                        }
                    }
                    break;
                case DIFFERENCE:
                    doubleValue = xmlConfig.getDouble(key + ".value", null);
                    if (doubleValue != null && doubleValue < 0) {
                        throw new ConfigurationException("The " + toleranceType + " tolerance type for \"" + key + "\" must be a positive value. Found this value: " + doubleValue);
                    }
                    break;
                default:
                    throw new ConfigurationException("Unknown Tolerance Type specified in config for <" + type + ">: " + toleranceType);
            }
            if (doubleValue != null) {
                return new Tolerance(doubleValue, toleranceType);
            }
        }
        return null;
    }

    private static Map<String, List<String>> parseEquivalentTermsMap(final XMLConfiguration xmlConfig) {
        final Map<String, List<String>> equivalentTermsMap = new LinkedHashMap<>();
        final Object prop = xmlConfig.getProperty("termMappings.termMapping.term");
        if (prop != null) {
            if (prop instanceof Collection) {
                final int size = ((Collection<?>) prop).size();
                for (int i = 0; i < size; i++) {
                    final String termElement = "termMappings.termMapping(" + i + ")";
                    parseTermMapping(termElement, xmlConfig, equivalentTermsMap);
                }
            } else {
                final String termElement = "termMappings.termMapping";
                parseTermMapping(termElement, xmlConfig, equivalentTermsMap);
            }
        }
        return equivalentTermsMap;
    }

    private static void parseTermMapping(final String termElement, final XMLConfiguration xmlConfig, final Map<String, List<String>> equivalentTermsMap) {
        final String term = xmlConfig.getString(termElement + ".term");
        final Object equivalentProp = xmlConfig.getString(termElement + ".equivalents.equivalent");
        if (equivalentProp instanceof Collection) {
            final int equivalentSize = ((Collection<?>) equivalentProp).size();
            if (term != null && equivalentSize > 1) {
                final List<String> equivalents = new ArrayList<>();
                for (int j = 0; j < equivalentSize; j++) {
                    final String equivalent = xmlConfig.getString(termElement + ".equivalents.equivalent(" + j + ")");
                    if (equivalent != null) {
                        equivalents.add(equivalent);
                    }
                }
                equivalentTermsMap.put(term, equivalents);
            }
        } else {
            final List<String> equivalents = new ArrayList<>();
            final String equivalent = xmlConfig.getString(termElement + ".equivalents.equivalent");
            if (equivalent != null) {
                equivalents.add(equivalent);
                if (term != null) {
                    equivalentTermsMap.put(term, equivalents);
                }
            }
        }
    }

    /**
     * @return the {@link Boolean} tolerance value or {@code null} if not
     * specified.
     */
    public Tolerance getBooleanTolerance() {
        return booleanTolerance;
    }

    /**
     * @return the {@link Byte} tolerance value or {@code null} if not
     * specified.
     */
    public Tolerance getByteTolerance() {
        return byteTolerance;
    }

    /**
     * @return the {@link Date} tolerance value or {@code null} if not
     * specified.
     */
    public Tolerance getDateTolerance() {
        return dateTolerance;
    }

    /**
     * @return the {@link Double} tolerance value or {@code null} if not
     * specified.
     */
    public Tolerance getDoubleTolerance() {
        return doubleTolerance;
    }

    /**
     * @return the {@link Float} tolerance value or {@code null} if not
     * specified.
     */
    public Tolerance getFloatTolerance() {
        return floatTolerance;
    }

    /**
     * @return the {@link Integer} tolerance value or {@code null} if not
     * specified.
     */
    public Tolerance getIntegerTolerance() {
        return integerTolerance;
    }

    /**
     * @return the {@link Long} tolerance value or {@code null} if not
     * specified.
     */
    public Tolerance getLongTolerance() {
        return longTolerance;
    }

    /**
     * @return the {@link Short} tolerance value or {@code null} if not
     * specified.
     */
    public Tolerance getShortTolerance() {
        return shortTolerance;
    }

    /**
     * @return the {@link String} tolerance value or {@code null} if not
     * specified.
     */
    public Tolerance getStringTolerance() {
        return stringTolerance;
    }

    /**
     * @return the {@link URI} tolerance value or {@code null} if not specified.
     */
    public Tolerance getUriTolerance() {
        return uriTolerance;
    }

    /**
     * @return the {@link Map} of terms that are considered equivalent to each
     * other.
     */
    public Map<String, List<String>> getEquivalentTermsMap() {
        return equivalentTermsMap;
    }

    /**
     * @return {@code true} to enable detection. {@code false} to disable
     * detection.
     */
    public boolean isDetectionEnabled() {
        return isDetectionEnabled;
    }
}