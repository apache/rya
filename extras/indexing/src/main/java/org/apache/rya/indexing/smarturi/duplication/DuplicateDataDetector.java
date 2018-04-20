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
package org.apache.rya.indexing.smarturi.duplication;

import static java.util.Objects.requireNonNull;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.StringUtils;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.api.resolver.impl.DateTimeRyaTypeResolver;
import org.apache.rya.indexing.entity.model.Entity;
import org.apache.rya.indexing.entity.model.Property;
import org.apache.rya.indexing.smarturi.SmartUriAdapter;
import org.apache.rya.indexing.smarturi.SmartUriException;
import org.apache.rya.indexing.smarturi.duplication.conf.DuplicateDataConfig;
import org.calrissian.mango.types.exception.TypeEncodingException;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.joda.time.DateTime;

import com.google.common.collect.ImmutableMap;

/**
 * Detects if two entities contain data that's nearly identical based on a set
 * tolerance for each field's type. Two entities are considered nearly
 * identical if all their properties are equal and/or within the specified
 * tolerance for the property's object type. Setting all object type tolerances
 * to 0 means that the objects need to be exactly equal to each other to be
 * considered duplicates. Duplicate data detection can be enabled/disabled
 * through configuration and each object type can have a tolerance based on
 * either the difference or the percentage difference between the objects being
 * compared.
 */
public class DuplicateDataDetector {
    private final Map<IRI, ApproxEqualsDetector<?>> uriMap = new HashMap<>();
    private final Map<Class<?>, ApproxEqualsDetector<?>> classMap = new HashMap<>();

    private boolean isDetectionEnabled;

    /**
     * Creates a new instance of {@link DuplicateDataDetector} with the
     * values provided by the configuration file.
     * @param duplicateDataConfig the {@link DuplicateDataConfig}
     */
    public DuplicateDataDetector(final DuplicateDataConfig duplicateDataConfig) {
        this(duplicateDataConfig.getBooleanTolerance(),
            duplicateDataConfig.getByteTolerance(),
            duplicateDataConfig.getDateTolerance(),
            duplicateDataConfig.getDoubleTolerance(),
            duplicateDataConfig.getFloatTolerance(),
            duplicateDataConfig.getIntegerTolerance(),
            duplicateDataConfig.getLongTolerance(),
            duplicateDataConfig.getShortTolerance(),
            duplicateDataConfig.getStringTolerance(),
            duplicateDataConfig.getUriTolerance(),
            duplicateDataConfig.getEquivalentTermsMap(),
            duplicateDataConfig.isDetectionEnabled()
        );
    }

    /**
     * Creates a new instance of {@link DuplicateDataDetector} with the values
     * from the config.
     * @throws ConfigurationException
     */
    public DuplicateDataDetector() throws ConfigurationException {
        this(new DuplicateDataConfig());
    }

    /**
     * Creates a new instance of {@link DuplicateDataDetector}.
     * @param tolerance the tolerance to assign to all types.
     */
    public DuplicateDataDetector(final double tolerance) {
        this(new Tolerance(tolerance, ToleranceType.DIFFERENCE), new LinkedHashMap<>());
    }

    /**
     * Creates a new instance of {@link DuplicateDataDetector}.
     * @param tolerance the tolerance to assign to all types.
     * @param equivalentTermsMap the {@link Map} of terms that are considered
     * equivalent to each other. (not {@code null})
     */
    public DuplicateDataDetector(final Tolerance tolerance, final Map<String, List<String>> equivalentTermsMap) {
        this(tolerance, tolerance, tolerance, tolerance, tolerance,
            tolerance, tolerance, tolerance, tolerance, tolerance , equivalentTermsMap, true);
    }

    /**
     * Creates a new instance of {@link DuplicateDataDetector}.
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
     * @param uriTolerance the {@link IRI} tolerance value or {@code null} if
     * not specified.
     * @param equivalentTermsMap the {@link Map} of terms that are considered
     * equivalent to each other. (not {@code null})
     * @param isDetectionEnabled {@code true} to enable detection. {@code false}
     * to disable detection.
     */
    public DuplicateDataDetector(final Tolerance booleanTolerance, final Tolerance byteTolerance,
            final Tolerance dateTolerance, final Tolerance doubleTolerance, final Tolerance floatTolerance,
            final Tolerance integerTolerance, final Tolerance longTolerance, final Tolerance shortTolerance,
            final Tolerance stringTolerance, final Tolerance uriTolerance, final Map<String, List<String>> equivalentTermsMap,
            final boolean isDetectionEnabled)
    {
        init(booleanTolerance, byteTolerance, dateTolerance, doubleTolerance, floatTolerance,
            integerTolerance, longTolerance, shortTolerance, stringTolerance, uriTolerance, equivalentTermsMap, isDetectionEnabled);
    }

    private void init(final Tolerance booleanTolerance, final Tolerance byteTolerance,
            final Tolerance dateTolerance, final Tolerance doubleTolerance, final Tolerance floatTolerance,
            final Tolerance integerTolerance, final Tolerance longTolerance, final Tolerance shortTolerance,
            final Tolerance stringTolerance, final Tolerance uriTolerance, final Map<String, List<String>> equivalentTermsMap,
            final boolean isDetectionEnabled)
    {
        final List<ApproxEqualsDetector<?>> detectors = new ArrayList<>();
        detectors.add(new BooleanApproxEqualsDetector(booleanTolerance));
        detectors.add(new ByteApproxEqualsDetector(byteTolerance));
        detectors.add(new DateApproxEqualsDetector(dateTolerance));
        detectors.add(new DateTimeApproxEqualsDetector(dateTolerance));
        detectors.add(new DoubleApproxEqualsDetector(doubleTolerance));
        detectors.add(new FloatApproxEqualsDetector(floatTolerance));
        detectors.add(new IntegerApproxEqualsDetector(integerTolerance));
        detectors.add(new LongApproxEqualsDetector(longTolerance));
        detectors.add(new ShortApproxEqualsDetector(shortTolerance));
        detectors.add(new StringApproxEqualsDetector(stringTolerance, equivalentTermsMap));
        detectors.add(new UriApproxEqualsDetector(uriTolerance));

        for (final ApproxEqualsDetector<?> approxEqualsDetector : detectors) {
            uriMap.put(approxEqualsDetector.getXmlSchemaUri(), approxEqualsDetector);
            classMap.put(approxEqualsDetector.getTypeClass(), approxEqualsDetector);
        }

        this.isDetectionEnabled = isDetectionEnabled;
    }

    /**
     * @return {@code true} to enable detection. {@code false} to disable
     * detection.
     */
    public boolean isDetectionEnabled() {
        return isDetectionEnabled;
    }

    /**
     * Removes any duplicate (nearly identical) entities from the collection
     * of entities.
     * @param entities the {@link List} of {@link Entity}s. (not {@code null})
     * @throws SmartUriException
     */
    public void removeDuplicatesFromCollection(final List<Entity> entities) throws SmartUriException {
        requireNonNull(entities);
        // Use a Sorted Set in reverse order to hold the indices
        final Set<Integer> indicesToRemove = new TreeSet<>((a, b) -> Integer.compare(b, a));
        if (entities != null && entities.size() > 1) {
            // Compare all entities to each other while avoiding making the
            // same comparisons again and not comparing an entity to itself.
            for (int i = 0; i < entities.size() - 1; i++) {
                final Entity entity1 = entities.get(i);
                for (int j = entities.size() - 1; j > i; j--) {
                    final Entity entity2 = entities.get(j);
                    final boolean areDuplicates = compareEntities(entity1, entity2);
                    if (areDuplicates) {
                        indicesToRemove.add(j);
                    }
                }
            }
        }
        if (!indicesToRemove.isEmpty()) {
            // Remove indices in reverse order (already sorted in descending
            // order so just loop through them)
            for (final int index : indicesToRemove) {
                entities.remove(index);
            }
        }
    }

    /**
     * Compares two Smart URI's to determine if they have nearly identical data.
     * @param uri1 the first Smart {@link IRI}. (not {@code null})
     * @param uri2 the second Smart {@link IRI}. (not {@code null})
     * @return {@code true} if the two Smart URI's have nearly identical data.
     * {@code false} otherwise.
     * @throws SmartUriException
     */
    public boolean compareSmartUris(final IRI uri1, final IRI uri2) throws SmartUriException {
        requireNonNull(uri1);
        requireNonNull(uri2);
        final Entity entity1 = SmartUriAdapter.deserializeUriEntity(uri1);
        final Entity entity2 = SmartUriAdapter.deserializeUriEntity(uri2);
        return compareEntities(entity1, entity2);
    }

    /**
     * Compares two entities to determine if they have nearly identical data.
     * @param entity1 the first {@link Entity}. (not {@code null})
     * @param entity2 the second {@link Entity}. (not {@code null})
     * @return {@code true} if the two entities have nearly identical data.
     * {@code false} otherwise.
     * @throws SmartUriException
     */
    public boolean compareEntities(final Entity entity1, final Entity entity2) throws SmartUriException {
        requireNonNull(entity1);
        requireNonNull(entity2);
        boolean allValuesNearlyEqual = true;

        final List<RyaIRI> types1 = entity1.getExplicitTypeIds();
        final List<RyaIRI> types2 = entity2.getExplicitTypeIds();
        final boolean doBothHaveSameTypes = types1.containsAll(types2);
        if (!doBothHaveSameTypes) {
            return false;
        }
        for (final Entry<RyaIRI, ImmutableMap<RyaIRI, Property>> entry : entity1.getProperties().entrySet()) {
            final RyaIRI typeIdUri = entry.getKey();
            for (final Entry<RyaIRI, Property> typeProperty : entry.getValue().entrySet()) {
                final RyaIRI propertyNameUri = typeProperty.getKey();
                final Property property1 = typeProperty.getValue();

                final Optional<Property> p2 = entity2.lookupTypeProperty(typeIdUri, propertyNameUri);
                if (p2.isPresent()) {
                    final Property property2 = p2.get();
                    final RyaType value1 = property1.getValue();
                    final RyaType value2 = property2.getValue();
                    final String data1 = value1.getData();
                    final String data2 = value2.getData();
                    final IRI xmlSchemaUri1 = value1.getDataType();
                    final ApproxEqualsDetector<?> approxEqualsDetector = uriMap.get(xmlSchemaUri1);
                    if (approxEqualsDetector == null) {
                        throw new SmartUriException("No appropriate detector found for the type: " + xmlSchemaUri1);
                    }
                    final boolean approxEquals = approxEqualsDetector.areApproxEquals(data1, data2);
                    if (!approxEquals) {
                        allValuesNearlyEqual = false;
                        break;
                    }
                } else {
                    allValuesNearlyEqual = false;
                    break;
                }
            }
            if (!allValuesNearlyEqual) {
                break;
            }
        }
        return allValuesNearlyEqual;
    }

    /**
     * Gets the appropriate {@link ApproxEqualsDetector} for the specified
     * class.
     * @param clazz the {@link Class} to find an {@link ApproxEqualsDetector}
     * for.
     * @return the {@link ApproxEqualsDetector} for the class or {@code null} if
     * none could be found.
     */
    public ApproxEqualsDetector<?> getDetectorForType(final Class<?> clazz) {
        return classMap.get(clazz);
    }

    private static boolean isOnlyOneNull(final Object lhs, final Object rhs) {
        return (lhs == null && rhs != null) || (lhs != null && rhs == null);
    }

    /**
     * Class to detect if two booleans are considered approximately equal to
     * each other.
     */
    public static class BooleanApproxEqualsDetector implements ApproxEqualsDetector<Boolean> {
        private static final Tolerance DEFAULT_TOLERANCE = new Tolerance(0.0, ToleranceType.DIFFERENCE);
        private final Tolerance tolerance;

        /**
         * Creates a new instance of {@link BooleanApproxEqualsDetector}.
         * @param tolerance the {@link Tolerance}.
         */
        public BooleanApproxEqualsDetector(final Tolerance tolerance) {
            this.tolerance = tolerance != null ? tolerance : getDefaultTolerance();
        }

        @Override
        public boolean areObjectsApproxEquals(final Boolean lhs, final Boolean rhs) {
            // Should never be almost equals when tolerance is 0, only exactly equals
            // Otherwise if there's any tolerance specified everything is equal
            return tolerance.getValue() != 0 || Objects.equals(lhs, rhs);
        }

        @Override
        public Tolerance getDefaultTolerance() {
            return DEFAULT_TOLERANCE;
        }

        @Override
        public Boolean convertStringToObject(final String string) throws SmartUriException {
            return Boolean.valueOf(string);
        }

        @Override
        public Class<?> getTypeClass() {
            return Boolean.class;
        }

        @Override
        public IRI getXmlSchemaUri() {
            return XMLSchema.BOOLEAN;
        }
    }

    /**
     * Class to detect if two bytes are considered approximately equal to each
     * other.
     */
    public static class ByteApproxEqualsDetector implements ApproxEqualsDetector<Byte> {
        private static final Tolerance DEFAULT_TOLERANCE = new Tolerance(0.0, ToleranceType.DIFFERENCE);
        private final Tolerance tolerance;

        /**
         * Creates a new instance of {@link ByteApproxEqualsDetector}.
         * @param tolerance the {@link Tolerance}.
         */
        public ByteApproxEqualsDetector(final Tolerance tolerance) {
            this.tolerance = tolerance != null ? tolerance : getDefaultTolerance();
        }

        @Override
        public boolean areObjectsApproxEquals(final Byte lhs, final Byte rhs) {
            if (isOnlyOneNull(lhs, rhs)) {
                return false;
            }
            if (Objects.equals(lhs, rhs)) {
                // They're exactly equals so get out
                return true;
            } else if (tolerance.getValue() == 0) {
                // If they're not exactly equals with zero tolerance then get out
                return false;
            }
            // Check based on tolerance
            switch (tolerance.getToleranceType()) {
                case PERCENTAGE:
                    if (lhs == 0) {
                        return lhs == rhs;
                    }
                    if (tolerance.getValue() >= 1) {
                        return true;
                    }
                    return ((double)Math.abs(lhs - rhs) / lhs) <= tolerance.getValue();
                case DIFFERENCE:
                default:
                    return Math.abs(lhs - rhs) <= tolerance.getValue();
            }
        }

        @Override
        public Tolerance getDefaultTolerance() {
            return DEFAULT_TOLERANCE;
        }

        @Override
        public Byte convertStringToObject(final String string) throws SmartUriException {
            return Byte.valueOf(string);
        }

        @Override
        public Class<?> getTypeClass() {
            return Byte.class;
        }

        @Override
        public IRI getXmlSchemaUri() {
            return XMLSchema.BYTE;
        }
    }

    /**
     * Class to detect if two dates are considered approximately equal to each
     * other.
     */
    public static class DateApproxEqualsDetector implements ApproxEqualsDetector<Date> {
        private static final Tolerance DEFAULT_TOLERANCE = new Tolerance(500.0, ToleranceType.DIFFERENCE); // milliseconds
        private final Tolerance tolerance;

        /**
         * Creates a new instance of {@link DateApproxEqualsDetector}.
         * @param tolerance the {@link Tolerance}.
         */
        public DateApproxEqualsDetector(final Tolerance tolerance) {
            this.tolerance = tolerance != null ? tolerance : getDefaultTolerance();
        }

        @Override
        public boolean areObjectsApproxEquals(final Date lhs, final Date rhs) {
            if (isOnlyOneNull(lhs, rhs)) {
                return false;
            }
            if (Objects.equals(lhs, rhs)) {
                // They're exactly equals so get out
                return true;
            } else if (tolerance.getValue() == 0) {
                // If they're not exactly equals with zero tolerance then get out
                return false;
            }
            // Check based on tolerance
            final long lhsTime = lhs.getTime();
            final long rhsTime = rhs.getTime();
            switch (tolerance.getToleranceType()) {
                case PERCENTAGE:
                    if (lhsTime == 0) {
                        return lhsTime == rhsTime;
                    }
                    if (tolerance.getValue() >= 1) {
                        return true;
                    }
                    return ((double)Math.abs(lhsTime - rhsTime) / lhsTime) <= tolerance.getValue();
                case DIFFERENCE:
                default:
                    return Math.abs(lhsTime - rhsTime) <= tolerance.getValue();
            }
        }

        @Override
        public Tolerance getDefaultTolerance() {
            return DEFAULT_TOLERANCE;
        }

        @Override
        public Date convertStringToObject(final String string) throws SmartUriException {
            DateTime dateTime = null;
            try {
                dateTime = DateTime.parse(string, DateTimeRyaTypeResolver.XMLDATETIME_PARSER);
            } catch (final TypeEncodingException e) {
                throw new SmartUriException("Exception occurred serializing data[" + string + "]", e);
            }
            final Date date = dateTime.toDate();
            return date;
        }

        @Override
        public Class<?> getTypeClass() {
            return Date.class;
        }

        @Override
        public IRI getXmlSchemaUri() {
            return XMLSchema.DATE;
        }
    }

    /**
     * Class to detect if two datetimes are considered approximately equal to
     * each other.
     */
    public static class DateTimeApproxEqualsDetector implements ApproxEqualsDetector<DateTime> {
        private static final Tolerance DEFAULT_TOLERANCE = new Tolerance(500.0, ToleranceType.DIFFERENCE); // milliseconds
        private final Tolerance tolerance;

        /**
         * Creates a new instance of {@link DateTimeApproxEqualsDetector}.
         * @param tolerance the {@link Tolerance}.
         */
        public DateTimeApproxEqualsDetector(final Tolerance tolerance) {
            this.tolerance = tolerance != null ? tolerance : getDefaultTolerance();
        }

        @Override
        public boolean areObjectsApproxEquals(final DateTime lhs, final DateTime rhs) {
            if (isOnlyOneNull(lhs, rhs)) {
                return false;
            }
            if (Objects.equals(lhs, rhs)) {
                // They're exactly equals so get out
                return true;
            } else if (tolerance.getValue() == 0) {
                // If they're not exactly equals with zero tolerance then get out
                return false;
            }
            // Check based on tolerance
            final long lhsTime = lhs.getMillis();
            final long rhsTime = rhs.getMillis();
            switch (tolerance.getToleranceType()) {
                case PERCENTAGE:
                    if (lhsTime == 0) {
                        return lhsTime == rhsTime;
                    }
                    if (tolerance.getValue() >= 1) {
                        return true;
                    }
                    return ((double)Math.abs(lhsTime - rhsTime) / lhsTime) <= tolerance.getValue();
                case DIFFERENCE:
                default:
                    return Math.abs(lhsTime - rhsTime) <= tolerance.getValue();
            }
        }

        @Override
        public Tolerance getDefaultTolerance() {
            return DEFAULT_TOLERANCE;
        }

        @Override
        public DateTime convertStringToObject(final String string) throws SmartUriException {
            DateTime dateTime = null;
            try {
                dateTime = DateTime.parse(string, DateTimeRyaTypeResolver.XMLDATETIME_PARSER);
            } catch (final TypeEncodingException e) {
                throw new SmartUriException("Exception occurred serializing data[" + string + "]", e);
            }
            return dateTime;
        }

        @Override
        public Class<?> getTypeClass() {
            return DateTime.class;
        }

        @Override
        public IRI getXmlSchemaUri() {
            return XMLSchema.DATETIME;
        }
    }

    /**
     * Class to detect if two doubles are considered approximately equal to each
     * other.
     */
    public static class DoubleApproxEqualsDetector implements ApproxEqualsDetector<Double> {
        private static final Tolerance DEFAULT_TOLERANCE = new Tolerance(0.0001, ToleranceType.PERCENTAGE);
        private final Tolerance tolerance;

        /**
         * Creates a new instance of {@link DoubleApproxEqualsDetector}.
         * @param tolerance the {@link Tolerance}.
         */
        public DoubleApproxEqualsDetector(final Tolerance tolerance) {
            this.tolerance = tolerance != null ? tolerance : getDefaultTolerance();
        }

        @Override
        public boolean areObjectsApproxEquals(final Double lhs, final Double rhs) {
            if (isOnlyOneNull(lhs, rhs)) {
                return false;
            }
            if (Objects.equals(lhs, rhs)) {
                // They're exactly equals so get out
                return true;
            } else if (tolerance.getValue() == 0) {
                // If they're not exactly equals with zero tolerance then get out
                return false;
            }
            // Doubles can be unpredictable with how they store a value
            // like 0.1. So use BigDecimal with its String constructor
            // to make things more predictable.
            final BigDecimal lhsBd = new BigDecimal(String.valueOf(lhs));
            final BigDecimal rhsBd = new BigDecimal(String.valueOf(rhs));
            switch (tolerance.getToleranceType()) {
                case PERCENTAGE:
                    if (lhs == 0) {
                        return lhs == rhs;
                    }
                    if (tolerance.getValue() >= 1) {
                        return true;
                    }
                    final BigDecimal absDiff = lhsBd.subtract(rhsBd).abs();
                    try {
                        final BigDecimal percent = absDiff.divide(lhsBd);
                        return percent.doubleValue() <= tolerance.getValue();
                    } catch (final ArithmeticException e) {
                        // BigDecimal quotient did not have a terminating
                        // decimal expansion. So, try without BigDecimal.
                        return (Math.abs(lhs - rhs) / lhs) <= tolerance.getValue();
                    }
                case DIFFERENCE:
                default:
                    final BigDecimal absDiff1 = lhsBd.subtract(rhsBd).abs();
                    return absDiff1.doubleValue() <= tolerance.getValue();
                    //return Math.abs(lhs - rhs) <= tolerance.getValue();
            }
        }

        @Override
        public Tolerance getDefaultTolerance() {
            return DEFAULT_TOLERANCE;
        }

        @Override
        public Double convertStringToObject(final String string) throws SmartUriException {
            return Double.valueOf(string);
        }

        @Override
        public Class<?> getTypeClass() {
            return Double.class;
        }

        @Override
        public IRI getXmlSchemaUri() {
            return XMLSchema.DOUBLE;
        }
    }

    /**
     * Class to detect if two floats are considered approximately equal to each
     * other.
     */
    public static class FloatApproxEqualsDetector implements ApproxEqualsDetector<Float> {
        private static final Tolerance DEFAULT_TOLERANCE = new Tolerance(0.0001, ToleranceType.PERCENTAGE);
        private final Tolerance tolerance;

        /**
         * Creates a new instance of {@link FloatApproxEqualsDetector}.
         * @param tolerance the {@link Tolerance}.
         */
        public FloatApproxEqualsDetector(final Tolerance tolerance) {
            this.tolerance = tolerance != null ? tolerance : getDefaultTolerance();
        }

        @Override
        public boolean areObjectsApproxEquals(final Float lhs, final Float rhs) {
            if (isOnlyOneNull(lhs, rhs)) {
                return false;
            }
            if (Objects.equals(lhs, rhs)) {
                // They're exactly equals so get out
                return true;
            } else if (tolerance.getValue() == 0) {
                // If they're not exactly equals with zero tolerance then get out
                return false;
            }
            // Check based on tolerance
            // Floats can be unpredictable with how they store a value
            // like 0.1. So use BigDecimal with its String constructor
            // to make things more predictable.
            final BigDecimal lhsBd = new BigDecimal(String.valueOf(lhs));
            final BigDecimal rhsBd = new BigDecimal(String.valueOf(rhs));
            switch (tolerance.getToleranceType()) {
                case PERCENTAGE:
                    if (lhs == 0) {
                        return lhs == rhs;
                    }
                    if (tolerance.getValue() >= 1) {
                        return true;
                    }
                    final BigDecimal absDiff = lhsBd.subtract(rhsBd).abs();
                    try {
                        final BigDecimal percent = absDiff.divide(lhsBd);
                        return percent.floatValue() <= tolerance.getValue();
                    } catch (final ArithmeticException e) {
                        // BigDecimal quotient did not have a terminating
                        // decimal expansion. So, try without BigDecimal.
                        return ((double)Math.abs(lhs - rhs) / lhs) <= tolerance.getValue();
                    }
                case DIFFERENCE:
                default:
                    final BigDecimal absDiff1 = lhsBd.subtract(rhsBd).abs();
                    return absDiff1.floatValue() <= tolerance.getValue();
                    //return Math.abs(lhs - rhs) <= tolerance.getValue();
            }
        }

        @Override
        public Tolerance getDefaultTolerance() {
            return DEFAULT_TOLERANCE;
        }

        @Override
        public Float convertStringToObject(final String string) throws SmartUriException {
            return Float.valueOf(string);
        }

        @Override
        public Class<?> getTypeClass() {
            return Float.class;
        }

        @Override
        public IRI getXmlSchemaUri() {
            return XMLSchema.FLOAT;
        }
    }

    /**
     * Class to detect if two integers are considered approximately equal to
     * each other.
     */
    public static class IntegerApproxEqualsDetector implements ApproxEqualsDetector<Integer> {
        private static final Tolerance DEFAULT_TOLERANCE = new Tolerance(1.0, ToleranceType.DIFFERENCE);
        private final Tolerance tolerance;

        /**
         * Creates a new instance of {@link IntegerApproxEqualsDetector}.
         * @param tolerance the {@link Tolerance}.
         */
        public IntegerApproxEqualsDetector(final Tolerance tolerance) {
            this.tolerance = tolerance != null ? tolerance : getDefaultTolerance();
        }

        @Override
        public boolean areObjectsApproxEquals(final Integer lhs, final Integer rhs) {
            if (isOnlyOneNull(lhs, rhs)) {
                return false;
            }
            if (Objects.equals(lhs, rhs)) {
                // They're exactly equals so get out
                return true;
            } else if (tolerance.getValue() == 0) {
                // If they're not exactly equals with zero tolerance then get out
                return false;
            }
            // Check based on tolerance
            switch (tolerance.getToleranceType()) {
                case PERCENTAGE:
                    if (lhs == 0) {
                        return lhs == rhs;
                    }
                    if (tolerance.getValue() >= 1) {
                        return true;
                    }
                    return ((double)Math.abs(lhs - rhs) / lhs) <= tolerance.getValue();
                case DIFFERENCE:
                default:
                    return Math.abs(lhs - rhs) <= tolerance.getValue();
            }
        }

        @Override
        public Tolerance getDefaultTolerance() {
            return DEFAULT_TOLERANCE;
        }

        @Override
        public Integer convertStringToObject(final String string) throws SmartUriException {
            return Integer.valueOf(string);
        }

        @Override
        public Class<?> getTypeClass() {
            return Integer.class;
        }

        @Override
        public IRI getXmlSchemaUri() {
            return XMLSchema.INTEGER;
        }
    }

    /**
     * Class to detect if two longs are considered approximately equal to
     * each other.
     */
    public static class LongApproxEqualsDetector implements ApproxEqualsDetector<Long> {
        private static final Tolerance DEFAULT_TOLERANCE = new Tolerance(1.0, ToleranceType.DIFFERENCE);
        private final Tolerance tolerance;

        /**
         * Creates a new instance of {@link LongApproxEqualsDetector}.
         * @param tolerance the {@link Tolerance}.
         */
        public LongApproxEqualsDetector(final Tolerance tolerance) {
            this.tolerance = tolerance != null ? tolerance : getDefaultTolerance();
        }

        @Override
        public boolean areObjectsApproxEquals(final Long lhs, final Long rhs) {
            if (isOnlyOneNull(lhs, rhs)) {
                return false;
            }
            if (Objects.equals(lhs, rhs)) {
                // They're exactly equals so get out
                return true;
            } else if (tolerance.getValue() == 0) {
                // If they're not exactly equals with zero tolerance then get out
                return false;
            }
            // Check based on tolerance
            switch (tolerance.getToleranceType()) {
                case PERCENTAGE:
                    if (lhs == 0) {
                        return lhs == rhs;
                    }
                    if (tolerance.getValue() >= 1) {
                        return true;
                    }
                    return ((double)Math.abs(lhs - rhs) / lhs) <= tolerance.getValue();
                case DIFFERENCE:
                default:
                    return Math.abs(lhs - rhs) <= tolerance.getValue();
            }
        }

        @Override
        public Tolerance getDefaultTolerance() {
            return DEFAULT_TOLERANCE;
        }

        @Override
        public Long convertStringToObject(final String string) throws SmartUriException {
            return Long.valueOf(string);
        }

        @Override
        public Class<?> getTypeClass() {
            return Long.class;
        }

        @Override
        public IRI getXmlSchemaUri() {
            return XMLSchema.LONG;
        }
    }

    /**
     * Class to detect if two shorts are considered approximately equal to each
     * other.
     */
    public static class ShortApproxEqualsDetector implements ApproxEqualsDetector<Short> {
        private static final Tolerance DEFAULT_TOLERANCE = new Tolerance(1.0, ToleranceType.DIFFERENCE);
        private final Tolerance tolerance;

        /**
         * Creates a new instance of {@link ShortApproxEqualsDetector}.
         * @param tolerance the {@link Tolerance}.
         */
        public ShortApproxEqualsDetector(final Tolerance tolerance) {
            this.tolerance = tolerance != null ? tolerance : getDefaultTolerance();
        }

        @Override
        public boolean areObjectsApproxEquals(final Short lhs, final Short rhs) {
            if (isOnlyOneNull(lhs, rhs)) {
                return false;
            }
            if (Objects.equals(lhs, rhs)) {
                // They're exactly equals so get out
                return true;
            } else if (tolerance.getValue() == 0) {
                // If they're not exactly equals with zero tolerance then get out
                return false;
            }
            // Check based on tolerance
            switch (tolerance.getToleranceType()) {
                case PERCENTAGE:
                    if (lhs == 0) {
                        return lhs == rhs;
                    }
                    if (tolerance.getValue() >= 1) {
                        return true;
                    }
                    return ((double)Math.abs(lhs - rhs) / lhs) <= tolerance.getValue();
                case DIFFERENCE:
                default:
                    return Math.abs(lhs - rhs) <= tolerance.getValue();
            }
        }

        @Override
        public Tolerance getDefaultTolerance() {
            return DEFAULT_TOLERANCE;
        }

        @Override
        public Short convertStringToObject(final String string) throws SmartUriException {
            return Short.valueOf(string);
        }

        @Override
        public Class<?> getTypeClass() {
            return Short.class;
        }

        @Override
        public IRI getXmlSchemaUri() {
            return XMLSchema.SHORT;
        }
    }

    /**
     * Class to detect if two string are considered approximately equal to each
     * other.
     */
    public static class StringApproxEqualsDetector implements ApproxEqualsDetector<String> {
        private static final Tolerance DEFAULT_TOLERANCE = new Tolerance(0.05, ToleranceType.PERCENTAGE);
        private final Tolerance tolerance;
        private final Map<String, List<String>> equivalentTermsMap;

        /**
         * Creates a new instance of {@link StringApproxEqualsDetector}.
         * @param tolerance the {@link Tolerance}.
         */
        public StringApproxEqualsDetector(final Tolerance tolerance, final Map<String, List<String>> equivalentTermsMap) {
            this.tolerance = tolerance != null ? tolerance : getDefaultTolerance();
            this.equivalentTermsMap = equivalentTermsMap;
        }

        @Override
        public boolean areObjectsApproxEquals(final String lhs, final String rhs) {
            if (isOnlyOneNull(lhs, rhs)) {
                return false;
            }
            if (StringUtils.equalsIgnoreCase(lhs, rhs)) {
                // They're exactly equals so get out
                return true;
            } else if (tolerance.getValue() == 0) {
                // If they're not exactly equals with zero tolerance then get out
                return false;
            }

            // Only check one-way. Terms are not bi-directionally equivalent
            // unless specified.
            final List<String> lhsTermEquivalents = equivalentTermsMap.get(lhs);
            if (lhsTermEquivalents != null && lhsTermEquivalents.contains(rhs)) {
                return true;
            }
            final int distance = StringUtils.getLevenshteinDistance(lhs, rhs);
            // Check based on tolerance
            switch (tolerance.getToleranceType()) {
                case PERCENTAGE:
                    if (lhs.length() == 0) {
                        return lhs.length() == rhs.length();
                    }
                    if (tolerance.getValue() >= 1) {
                        return true;
                    }
                    return ((double)distance / lhs.length()) <= tolerance.getValue();
                case DIFFERENCE:
                default:
                    return distance <= tolerance.getValue();
            }
        }

        @Override
        public Tolerance getDefaultTolerance() {
            return DEFAULT_TOLERANCE;
        }

        @Override
        public String convertStringToObject(final String string) throws SmartUriException {
            return string;
        }

        @Override
        public Class<?> getTypeClass() {
            return String.class;
        }

        @Override
        public IRI getXmlSchemaUri() {
            return XMLSchema.STRING;
        }
    }

    /**
     * Class to detect if two URIs are considered approximately equal to each
     * other.
     */
    public static class UriApproxEqualsDetector implements ApproxEqualsDetector<IRI> {
        private static final Tolerance DEFAULT_TOLERANCE = new Tolerance(1.0, ToleranceType.DIFFERENCE);
        private final Tolerance tolerance;

        /**
         * Creates a new instance of {@link UriApproxEqualsDetector}.
         * @param tolerance the {@link Tolerance}.
         */
        public UriApproxEqualsDetector(final Tolerance tolerance) {
            this.tolerance = tolerance != null ? tolerance : getDefaultTolerance();
        }

        @Override
        public boolean areObjectsApproxEquals(final IRI lhs, final IRI rhs) {
            if (isOnlyOneNull(lhs, rhs)) {
                return false;
            }
            if (Objects.equals(lhs, rhs)) {
                return true;
            }
            final String uriString1 = lhs.stringValue();
            final String uriString2 = rhs.stringValue();
            if (StringUtils.equalsIgnoreCase(uriString1, uriString2)) {
                // They're exactly equals so get out
                return true;
            } else if (tolerance.getValue() == 0) {
                // If they're not exactly equals with zero tolerance then get out
                return false;
            }
            final int distance = StringUtils.getLevenshteinDistance(uriString1, uriString2);
            // Check based on tolerance
            switch (tolerance.getToleranceType()) {
                case PERCENTAGE:
                    if (uriString1.length() == 0) {
                        return uriString1.length() == uriString2.length();
                    }
                    if (tolerance.getValue() >= 1) {
                        return true;
                    }
                    return ((double)distance / uriString1.length()) <= tolerance.getValue();
                case DIFFERENCE:
                default:
                    return distance <= tolerance.getValue();
            }
        }

        @Override
        public Tolerance getDefaultTolerance() {
            return DEFAULT_TOLERANCE;
        }

        @Override
        public IRI convertStringToObject(final String string) throws SmartUriException {
            return SimpleValueFactory.getInstance().createIRI(string);
        }

        @Override
        public Class<?> getTypeClass() {
            return IRI.class;
        }

        @Override
        public IRI getXmlSchemaUri() {
            return XMLSchema.ANYURI;
        }
    }
}
