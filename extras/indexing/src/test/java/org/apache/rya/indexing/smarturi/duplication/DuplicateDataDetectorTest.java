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
import static org.apache.rya.api.domain.RyaTypeUtils.booleanRyaType;
import static org.apache.rya.api.domain.RyaTypeUtils.byteRyaType;
import static org.apache.rya.api.domain.RyaTypeUtils.dateRyaType;
import static org.apache.rya.api.domain.RyaTypeUtils.doubleRyaType;
import static org.apache.rya.api.domain.RyaTypeUtils.floatRyaType;
import static org.apache.rya.api.domain.RyaTypeUtils.intRyaType;
import static org.apache.rya.api.domain.RyaTypeUtils.longRyaType;
import static org.apache.rya.api.domain.RyaTypeUtils.shortRyaType;
import static org.apache.rya.api.domain.RyaTypeUtils.stringRyaType;
import static org.apache.rya.api.domain.RyaTypeUtils.uriRyaType;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.rya.api.domain.RyaSchema;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaTypeUtils;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.indexing.entity.model.Entity;
import org.apache.rya.indexing.entity.model.Entity.Builder;
import org.apache.rya.indexing.entity.model.Property;
import org.apache.rya.indexing.entity.model.Type;
import org.apache.rya.indexing.entity.storage.EntityStorage;
import org.apache.rya.indexing.entity.storage.EntityStorage.EntityStorageException;
import org.apache.rya.indexing.entity.storage.TypeStorage;
import org.apache.rya.indexing.entity.storage.TypeStorage.TypeStorageException;
import org.apache.rya.indexing.entity.storage.mongo.MongoEntityStorage;
import org.apache.rya.indexing.entity.storage.mongo.MongoTypeStorage;
import org.apache.rya.indexing.mongodb.update.RyaObjectStorage.ObjectStorageException;
import org.apache.rya.indexing.smarturi.SmartUriException;
import org.apache.rya.indexing.smarturi.duplication.conf.DuplicateDataConfig;
import org.apache.rya.mongodb.MongoTestBase;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.joda.time.DateTime;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

/**
 * Tests the methods of {@link DuplicateDataDetector}.
 */
public class DuplicateDataDetectorTest extends MongoTestBase {
    private static final String RYA_INSTANCE_NAME = "testInstance";

    private static final String NAMESPACE = RyaSchema.NAMESPACE;
    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    // People
    private static final RyaURI BOB = createRyaUri("Bob");

    // Attributes
    private static final RyaURI HAS_WEIGHT = createRyaUri("hasWeight");
    private static final RyaURI HAS_HEIGHT = createRyaUri("hasHeight");
    private static final RyaURI HAS_SSN = createRyaUri("hasSSN");
    private static final RyaURI HAS_AGE = createRyaUri("hasAge");
    private static final RyaURI HAS_INCOME = createRyaUri("hasIncome");
    private static final RyaURI HAS_NUMBER_OF_CHILDREN = createRyaUri("hasNumberOfChildren");
    private static final RyaURI HAS_LICENSE_NUMBER = createRyaUri("hasLicenseNumber");
    private static final RyaURI HAS_EYE_COLOR = createRyaUri("hasEyeColor");
    private static final RyaURI HAS_HAIR_COLOR = createRyaUri("hasHairColor");
    private static final RyaURI HAS_DATE_OF_BIRTH = createRyaUri("hasDateOfBirth");
    private static final RyaURI HAS_EXPIRATION_DATE = createRyaUri("hasExpirationDate");
    private static final RyaURI HAS_GLASSES = createRyaUri("hasGlasses");
    private static final RyaURI HAS_EMAIL_ADDRESS = createRyaUri("hasEmailAddress");
    private static final RyaURI HAS_ATTRIBUTE_SPACE = createRyaUri("has Attribute Space");
    private static final RyaURI HAS_MOTTO = createRyaUri("hasMotto");
    private static final RyaURI HAS_BLOOD_TYPE = createRyaUri("hasBloodType");
    private static final RyaURI HAS_SEX = createRyaUri("hasSex");
    private static final RyaURI HAS_ADDRESS = createRyaUri("hasAddress");
    private static final RyaURI HAS_POSITION_TITLE = createRyaUri("hasPositionTitle");
    private static final RyaURI HAS_WORK_ADDRESS = createRyaUri("hasWorkAddress");
    private static final RyaURI HAS_EXTENSION = createRyaUri("hasExtension");
    private static final RyaURI HAS_OFFICE_ROOM_NUMBER = createRyaUri("hasOfficeRoomNumber");

    // Type URIs
    private static final RyaURI PERSON_TYPE_URI = new RyaURI("urn:example/person");
    private static final RyaURI EMPLOYEE_TYPE_URI = new RyaURI("urn:example/employee");

    private static final Date NOW = new Date();

    /**
     * Creates a {@link RyaURI} for the specified local name.
     * @param localName the URI's local name.
     * @return the {@link RyraURI}.
     */
    private static RyaURI createRyaUri(final String localName) {
        return createRyaUri(NAMESPACE, localName);
    }

    /**
     * Creates a {@link RyaURI} for the specified local name.
     * @param namespace the namespace.
     * @param localName the URI's local name.
     * @return the {@link RyraURI}.
     */
    private static RyaURI createRyaUri(final String namespace, final String localName) {
        return RdfToRyaConversions.convertURI(VF.createIRI(namespace, localName));
    }

    private static Entity createBobEntity() {
        final Entity bobEntity = Entity.builder()
            .setSubject(BOB)
            .setExplicitType(PERSON_TYPE_URI)
            .setExplicitType(EMPLOYEE_TYPE_URI)
            .setProperty(PERSON_TYPE_URI, new Property(HAS_WEIGHT, floatRyaType(250.75f)))
            .setProperty(PERSON_TYPE_URI, new Property(HAS_HEIGHT, doubleRyaType(72.5)))
            .setProperty(PERSON_TYPE_URI, new Property(HAS_SSN, stringRyaType("123-45-6789")))
            .setProperty(PERSON_TYPE_URI, new Property(HAS_AGE, shortRyaType((short) 40)))
            .setProperty(PERSON_TYPE_URI, new Property(HAS_INCOME, intRyaType(50000)))
            .setProperty(PERSON_TYPE_URI, new Property(HAS_NUMBER_OF_CHILDREN, byteRyaType((byte) 2)))
            .setProperty(PERSON_TYPE_URI, new Property(HAS_LICENSE_NUMBER, longRyaType(123456789012L)))
            .setProperty(PERSON_TYPE_URI, new Property(HAS_EYE_COLOR, stringRyaType("blue")))
            .setProperty(PERSON_TYPE_URI, new Property(HAS_HAIR_COLOR, stringRyaType("brown")))
            .setProperty(PERSON_TYPE_URI, new Property(HAS_DATE_OF_BIRTH, dateRyaType(new DateTime(NOW.getTime()).minusYears(40))))
            .setProperty(PERSON_TYPE_URI, new Property(HAS_EXPIRATION_DATE, dateRyaType(NOW)))
            .setProperty(PERSON_TYPE_URI, new Property(HAS_GLASSES, booleanRyaType(true)))
            .setProperty(PERSON_TYPE_URI, new Property(HAS_EMAIL_ADDRESS, uriRyaType(VF.createIRI("mailto:bob.smitch00@gmail.com"))))
            .setProperty(PERSON_TYPE_URI, new Property(HAS_ATTRIBUTE_SPACE, stringRyaType("attribute space")))
            .setProperty(PERSON_TYPE_URI, new Property(HAS_MOTTO, stringRyaType("!@#*\\&%20^ smörgåsbord")))
            .setProperty(PERSON_TYPE_URI, new Property(HAS_BLOOD_TYPE, stringRyaType("A+ blood type")))
            .setProperty(PERSON_TYPE_URI, new Property(HAS_SEX, stringRyaType("M")))
            .setProperty(PERSON_TYPE_URI, new Property(HAS_ADDRESS, stringRyaType("123 Fake St. Washington, DC 20024")))
            .setProperty(EMPLOYEE_TYPE_URI, new Property(HAS_POSITION_TITLE, stringRyaType("Assistant to the Regional Manager")))
            .setProperty(EMPLOYEE_TYPE_URI, new Property(HAS_WORK_ADDRESS, stringRyaType("987 Fake Office Rd. Washington, DC 20024")))
            .setProperty(EMPLOYEE_TYPE_URI, new Property(HAS_EXTENSION, shortRyaType((short) 555)))
            .setProperty(EMPLOYEE_TYPE_URI, new Property(HAS_OFFICE_ROOM_NUMBER, shortRyaType((short) 9999)))
            .build();

        return bobEntity;
    }

    private static Type createPersonType() {
        final Type personType =
            new Type(
                PERSON_TYPE_URI,
                ImmutableSet.<RyaURI>builder()
                    .add(HAS_WEIGHT)
                    .add(HAS_HEIGHT)
                    .add(HAS_SSN)
                    .add(HAS_AGE)
                    .add(HAS_INCOME)
                    .add(HAS_NUMBER_OF_CHILDREN)
                    .add(HAS_LICENSE_NUMBER)
                    .add(HAS_EYE_COLOR)
                    .add(HAS_HAIR_COLOR)
                    .add(HAS_DATE_OF_BIRTH)
                    .add(HAS_EXPIRATION_DATE)
                    .add(HAS_GLASSES)
                    .add(HAS_EMAIL_ADDRESS)
                    .add(HAS_ATTRIBUTE_SPACE)
                    .add(HAS_MOTTO)
                    .add(HAS_BLOOD_TYPE)
                    .add(HAS_SEX)
                    .add(HAS_ADDRESS)
                .build()
            );
        return personType;
    }

    private static Type createEmployeeType() {
        final Type employeeType =
            new Type(
                EMPLOYEE_TYPE_URI,
                ImmutableSet.<RyaURI>builder()
                    .add(HAS_POSITION_TITLE)
                    .add(HAS_WORK_ADDRESS)
                    .add(HAS_EXTENSION)
                    .add(HAS_OFFICE_ROOM_NUMBER)
                .build()
            );
        return employeeType;
    }

    private static String createErrorMessage(final Object originalValue, final Object testValue, final boolean expected, final boolean actual, final Tolerance tolerance) {
        final String message = "The test value \"" + testValue + "\" was " + (expected ? "" : "NOT ") + "supposed to be almost equals to \"" + originalValue + "\" when the tolerance was \"" + tolerance.toString() + "\" but " + (actual ? "was" : "wasn't") + ".";
        return message;
    }

    @Test
    public void testCompareEntities() throws SmartUriException, ConfigurationException {
        final Entity entity1 = createBobEntity();
        final Entity entity2 = new Builder(entity1).build();

        final DuplicateDataDetector duplicateDataDetector = new DuplicateDataDetector();
        final boolean areDuplicates = duplicateDataDetector.compareEntities(entity1, entity2);
        assertTrue(areDuplicates);
    }

    @Test
    public void testCompareSmartUris() throws SmartUriException, ConfigurationException {
        final Entity entity1 = createBobEntity();
        final Entity entity2 = new Builder(entity1).build();

        final DuplicateDataDetector duplicateDataDetector = new DuplicateDataDetector();
        final boolean areDuplicates = duplicateDataDetector.compareSmartUris(entity1.getSmartUri(), entity2.getSmartUri());
        assertTrue(areDuplicates);
    }

    @Test
    public void testEntitySubjectsDifferent() throws SmartUriException, ConfigurationException {
        final Entity entity1 = createBobEntity();
        final Builder builder = new Builder(entity1);
        builder.setSubject(createRyaUri("Susan"));
        final Entity entity2 = builder.build();

        final DuplicateDataDetector duplicateDataDetector = new DuplicateDataDetector();
        final boolean areDuplicates = duplicateDataDetector.compareEntities(entity1, entity2);
        assertTrue(areDuplicates);
    }

    @Test
    public void testEntityMissingType() throws SmartUriException, ConfigurationException {
        final Entity entity1 = createBobEntity();
        final Builder builder = new Builder(entity1);
        builder.setExplicitType(new RyaURI("urn:example/manager"));
        final Entity entity2 = builder.build();

        final DuplicateDataDetector duplicateDataDetector = new DuplicateDataDetector();
        final boolean areDuplicates = duplicateDataDetector.compareEntities(entity1, entity2);
        assertFalse(areDuplicates);
    }

    @Test
    public void testEntityMissingProperty() throws SmartUriException, ConfigurationException {
        final Entity entity1 = createBobEntity();
        final Builder builder = new Builder(entity1);
        builder.unsetProperty(PERSON_TYPE_URI, HAS_SSN);
        final Entity entity2 = builder.build();

        final DuplicateDataDetector duplicateDataDetector = new DuplicateDataDetector();
        final boolean areDuplicates = duplicateDataDetector.compareEntities(entity1, entity2);
        assertFalse(areDuplicates);
    }

    @Test
    public void testReadConfigFile() throws SmartUriException, ConfigurationException {
        final DuplicateDataConfig duplicateDataConfig = new DuplicateDataConfig();

        assertNotNull(duplicateDataConfig.getBooleanTolerance());
        assertNotNull(duplicateDataConfig.getByteTolerance());
        assertNotNull(duplicateDataConfig.getDateTolerance());
        assertNotNull(duplicateDataConfig.getDoubleTolerance());
        assertNotNull(duplicateDataConfig.getFloatTolerance());
        assertNotNull(duplicateDataConfig.getIntegerTolerance());
        assertNotNull(duplicateDataConfig.getLongTolerance());
        assertNotNull(duplicateDataConfig.getShortTolerance());
        assertNotNull(duplicateDataConfig.getStringTolerance());
        assertNotNull(duplicateDataConfig.getUriTolerance());

        assertNotNull(duplicateDataConfig.getEquivalentTermsMap());
        assertNotNull(duplicateDataConfig.isDetectionEnabled());
    }

    @Test
    public void testBooleanProperty() throws SmartUriException {
        System.out.println("Boolean Property Test");
        final ImmutableList.Builder<TestInput> builder = ImmutableList.builder();
        // Tolerance 0.0
        Tolerance tolerance = new Tolerance(0.0, ToleranceType.DIFFERENCE);
        builder.add(new TestInput(false, tolerance, false));
        builder.add(new TestInput(true, tolerance, true)); // Equals value
        // Tolerance 1.0
        tolerance = new Tolerance(1.0, ToleranceType.DIFFERENCE);
        builder.add(new TestInput(false, tolerance, true));
        builder.add(new TestInput(true, tolerance, true)); // Equals value
        // Tolerance 2.0
        tolerance = new Tolerance(2.0, ToleranceType.DIFFERENCE);
        builder.add(new TestInput(false, tolerance, true));
        builder.add(new TestInput(true, tolerance, true)); // Equals value

        // Tolerance 0.0%
        tolerance = new Tolerance(0.00, ToleranceType.PERCENTAGE);
        builder.add(new TestInput(false, tolerance, false));
        builder.add(new TestInput(true, tolerance, true)); // Equals value
        // Tolerance 1.0%
        tolerance = new Tolerance(0.01, ToleranceType.PERCENTAGE);
        builder.add(new TestInput(false, tolerance, true));
        builder.add(new TestInput(true, tolerance, true)); // Equals value
        // Tolerance 100.0%
        tolerance = new Tolerance(1.00, ToleranceType.PERCENTAGE);
        builder.add(new TestInput(false, tolerance, true));
        builder.add(new TestInput(true, tolerance, true)); // Equals value

        final ImmutableList<TestInput> testInputs = builder.build();

        testProperty(testInputs, PERSON_TYPE_URI, HAS_GLASSES);
    }

    @Test
    public void testByteProperty() throws SmartUriException {
        System.out.println("Byte Property Test");
        final ImmutableList.Builder<TestInput> builder = ImmutableList.builder();
        // Tolerance 0.0
        Tolerance tolerance = new Tolerance(0.0, ToleranceType.DIFFERENCE);
        builder.add(new TestInput(Byte.MIN_VALUE, tolerance, false));
        builder.add(new TestInput((byte) 0xff, tolerance, false));
        builder.add(new TestInput((byte) 0x00, tolerance, false));
        builder.add(new TestInput((byte) 0x01, tolerance, false));
        builder.add(new TestInput((byte) 0x02, tolerance, true)); // Equals value
        builder.add(new TestInput((byte) 0x03, tolerance, false));
        builder.add(new TestInput((byte) 0x04, tolerance, false));
        builder.add(new TestInput((byte) 0x05, tolerance, false));
        builder.add(new TestInput((byte) 0x10, tolerance, false));
        builder.add(new TestInput(Byte.MAX_VALUE, tolerance, false));
        // Tolerance 1.0
        tolerance = new Tolerance(1.0, ToleranceType.DIFFERENCE);
        builder.add(new TestInput(Byte.MIN_VALUE, tolerance, false));
        builder.add(new TestInput((byte) 0xff, tolerance, false));
        builder.add(new TestInput((byte) 0x00, tolerance, false));
        builder.add(new TestInput((byte) 0x01, tolerance, true));
        builder.add(new TestInput((byte) 0x02, tolerance, true)); // Equals value
        builder.add(new TestInput((byte) 0x03, tolerance, true));
        builder.add(new TestInput((byte) 0x04, tolerance, false));
        builder.add(new TestInput((byte) 0x05, tolerance, false));
        builder.add(new TestInput((byte) 0x10, tolerance, false));
        builder.add(new TestInput(Byte.MAX_VALUE, tolerance, false));
        // Tolerance 2.0
        tolerance = new Tolerance(2.0, ToleranceType.DIFFERENCE);
        builder.add(new TestInput(Byte.MIN_VALUE, tolerance, false));
        builder.add(new TestInput((byte) 0xff, tolerance, false));
        builder.add(new TestInput((byte) 0x00, tolerance, true));
        builder.add(new TestInput((byte) 0x01, tolerance, true));
        builder.add(new TestInput((byte) 0x02, tolerance, true)); // Equals value
        builder.add(new TestInput((byte) 0x03, tolerance, true));
        builder.add(new TestInput((byte) 0x04, tolerance, true));
        builder.add(new TestInput((byte) 0x05, tolerance, false));
        builder.add(new TestInput((byte) 0x10, tolerance, false));
        builder.add(new TestInput(Byte.MAX_VALUE, tolerance, false));

        // Tolerance 0.0%
        tolerance = new Tolerance(0.00, ToleranceType.PERCENTAGE);
        builder.add(new TestInput(Byte.MIN_VALUE, tolerance, false));
        builder.add(new TestInput((byte) 0xff, tolerance, false));
        builder.add(new TestInput((byte) 0x00, tolerance, false));
        builder.add(new TestInput((byte) 0x01, tolerance, false));
        builder.add(new TestInput((byte) 0x02, tolerance, true)); // Equals value
        builder.add(new TestInput((byte) 0x03, tolerance, false));
        builder.add(new TestInput((byte) 0x04, tolerance, false));
        builder.add(new TestInput((byte) 0x05, tolerance, false));
        builder.add(new TestInput((byte) 0x10, tolerance, false));
        builder.add(new TestInput(Byte.MAX_VALUE, tolerance, false));
        // Tolerance 50.0%
        tolerance = new Tolerance(0.50, ToleranceType.PERCENTAGE);
        builder.add(new TestInput(Byte.MIN_VALUE, tolerance, false));
        builder.add(new TestInput((byte) 0xff, tolerance, false));
        builder.add(new TestInput((byte) 0x00, tolerance, false));
        builder.add(new TestInput((byte) 0x01, tolerance, true));
        builder.add(new TestInput((byte) 0x02, tolerance, true)); // Equals value
        builder.add(new TestInput((byte) 0x03, tolerance, true));
        builder.add(new TestInput((byte) 0x04, tolerance, false));
        builder.add(new TestInput((byte) 0x05, tolerance, false));
        builder.add(new TestInput((byte) 0x10, tolerance, false));
        builder.add(new TestInput(Byte.MAX_VALUE, tolerance, false));
        // Tolerance 100.0%
        tolerance = new Tolerance(1.00, ToleranceType.PERCENTAGE);
        builder.add(new TestInput(Byte.MIN_VALUE, tolerance, true));
        builder.add(new TestInput((byte) 0xff, tolerance, true));
        builder.add(new TestInput((byte) 0x00, tolerance, true));
        builder.add(new TestInput((byte) 0x01, tolerance, true));
        builder.add(new TestInput((byte) 0x02, tolerance, true)); // Equals value
        builder.add(new TestInput((byte) 0x03, tolerance, true));
        builder.add(new TestInput((byte) 0x04, tolerance, true));
        builder.add(new TestInput((byte) 0x05, tolerance, true));
        builder.add(new TestInput((byte) 0x10, tolerance, true));
        builder.add(new TestInput(Byte.MAX_VALUE, tolerance, true));

        final ImmutableList<TestInput> testInputs = builder.build();

        testProperty(testInputs, PERSON_TYPE_URI, HAS_NUMBER_OF_CHILDREN);
    }

    @Test
    public void testDateProperty() throws SmartUriException {
        System.out.println("Date Property Test");
        final long ONE_YEAR_IN_MILLIS = 1000L * 60L * 60L * 24L * 365L;
        final ImmutableList.Builder<TestInput> builder = ImmutableList.builder();
        // Tolerance 0.0
        Tolerance tolerance = new Tolerance(0.0, ToleranceType.DIFFERENCE);
        builder.add(new TestInput(new Date(0L), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() - ONE_YEAR_IN_MILLIS), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() - 10000), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() - 2000), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() - 1001), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() - 1000), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() - 999), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() - 3), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() - 2), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() - 1), tolerance, false));
        builder.add(new TestInput(NOW, tolerance, true)); // Equals value
        builder.add(new TestInput(new Date(NOW.getTime() + 1), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() + 2), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() + 3), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() + 999), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() + 1000), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() + 1001), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() + 2000), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() + 10000), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() + ONE_YEAR_IN_MILLIS), tolerance, false));
        builder.add(new TestInput(new Date(Long.MAX_VALUE - ONE_YEAR_IN_MILLIS), tolerance, false));
        // Tolerance 1.0
        tolerance = new Tolerance(1.0, ToleranceType.DIFFERENCE);
        builder.add(new TestInput(new Date(0L), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() - ONE_YEAR_IN_MILLIS), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() - 10000), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() - 2000), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() - 1001), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() - 1000), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() - 999), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() - 3), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() - 2), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() - 1), tolerance, true));
        builder.add(new TestInput(NOW, tolerance, true)); // Equals value
        builder.add(new TestInput(new Date(NOW.getTime() + 1), tolerance, true));
        builder.add(new TestInput(new Date(NOW.getTime() + 2), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() + 3), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() + 999), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() + 1000), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() + 1001), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() + 2000), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() + 10000), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() + ONE_YEAR_IN_MILLIS), tolerance, false));
        builder.add(new TestInput(new Date(Long.MAX_VALUE - ONE_YEAR_IN_MILLIS), tolerance, false));
        // Tolerance 2.0
        tolerance = new Tolerance(2.0, ToleranceType.DIFFERENCE);
        builder.add(new TestInput(new Date(0L), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() - ONE_YEAR_IN_MILLIS), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() - 10000), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() - 2000), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() - 1001), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() - 1000), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() - 999), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() - 3), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() - 2), tolerance, true));
        builder.add(new TestInput(new Date(NOW.getTime() - 1), tolerance, true));
        builder.add(new TestInput(NOW, tolerance, true)); // Equals value
        builder.add(new TestInput(new Date(NOW.getTime() + 1), tolerance, true));
        builder.add(new TestInput(new Date(NOW.getTime() + 2), tolerance, true));
        builder.add(new TestInput(new Date(NOW.getTime() + 3), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() + 999), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() + 1000), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() + 1001), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() + 2000), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() + 10000), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() + ONE_YEAR_IN_MILLIS), tolerance, false));
        builder.add(new TestInput(new Date(Long.MAX_VALUE - ONE_YEAR_IN_MILLIS), tolerance, false));

        // Tolerance 0.0%
        tolerance = new Tolerance(0.00, ToleranceType.PERCENTAGE);
        builder.add(new TestInput(new Date(0L), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() - ONE_YEAR_IN_MILLIS), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() - 10000), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() - 2000), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() - 1001), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() - 1000), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() - 999), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() - 3), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() - 2), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() - 1), tolerance, false));
        builder.add(new TestInput(NOW, tolerance, true)); // Equals value
        builder.add(new TestInput(new Date(NOW.getTime() + 1), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() + 2), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() + 3), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() + 999), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() + 1000), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() + 1001), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() + 2000), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() + 10000), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() + ONE_YEAR_IN_MILLIS), tolerance, false));
        builder.add(new TestInput(new Date(Long.MAX_VALUE - ONE_YEAR_IN_MILLIS), tolerance, false));
        // Tolerance 1.0%
        tolerance = new Tolerance(0.01, ToleranceType.PERCENTAGE);
        builder.add(new TestInput(new Date(0L), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() - ONE_YEAR_IN_MILLIS), tolerance, false));
        builder.add(new TestInput(new Date((long) (NOW.getTime() * 0.985)), tolerance, false));
        builder.add(new TestInput(new Date((long) (NOW.getTime() * 0.989)), tolerance, false));
        // It's tricky near the exact threshold since it might create a fraction
        // of a time which is rounded. Check if it's fraction and test it with
        // the floor and ceiling values if it is. Otherwise, use the exact value
        // if it is not a fraction.
        final double lowerThresholdFloor = Math.floor(NOW.getTime() * 0.99);
        final double lowerThresholdCeiling = Math.ceil(NOW.getTime() * 0.99);
        // If the floor equals the ceiling then it's not a fraction.
        if (lowerThresholdFloor != lowerThresholdCeiling) {
           builder.add(new TestInput(new Date((long) lowerThresholdFloor), tolerance, false));
        }
        builder.add(new TestInput(new Date((long) lowerThresholdCeiling), tolerance, true));
        builder.add(new TestInput(new Date((long) (NOW.getTime() * 0.991)), tolerance, true));
        builder.add(new TestInput(new Date((long) (NOW.getTime() * 0.995)), tolerance, true));
        builder.add(new TestInput(new Date(NOW.getTime() - 10000), tolerance, true));
        builder.add(new TestInput(new Date(NOW.getTime() - 2000), tolerance, true));
        builder.add(new TestInput(new Date(NOW.getTime() - 1001), tolerance, true));
        builder.add(new TestInput(new Date(NOW.getTime() - 1000), tolerance, true));
        builder.add(new TestInput(new Date(NOW.getTime() - 999), tolerance, true));
        builder.add(new TestInput(new Date(NOW.getTime() - 3), tolerance, true));
        builder.add(new TestInput(new Date(NOW.getTime() - 2), tolerance, true));
        builder.add(new TestInput(new Date(NOW.getTime() - 1), tolerance, true));
        builder.add(new TestInput(NOW, tolerance, true)); // Equals value
        builder.add(new TestInput(new Date(NOW.getTime() + 1), tolerance, true));
        builder.add(new TestInput(new Date(NOW.getTime() + 2), tolerance, true));
        builder.add(new TestInput(new Date(NOW.getTime() + 3), tolerance, true));
        builder.add(new TestInput(new Date(NOW.getTime() + 999), tolerance, true));
        builder.add(new TestInput(new Date(NOW.getTime() + 1000), tolerance, true));
        builder.add(new TestInput(new Date(NOW.getTime() + 1001), tolerance, true));
        builder.add(new TestInput(new Date(NOW.getTime() + 2000), tolerance, true));
        builder.add(new TestInput(new Date(NOW.getTime() + 10000), tolerance, true));
        builder.add(new TestInput(new Date((long) (NOW.getTime() * 1.005)), tolerance, true));
        builder.add(new TestInput(new Date((long) (NOW.getTime() * 1.009)), tolerance, true));
        // It's tricky near the exact threshold since it might create a fraction
        // of a time which is rounded. Check if it's fraction and test it with
        // the floor and ceiling values if it is. Otherwise, use the exact value
        // if it is not a fraction.
        final double upperThresholdFloor = Math.floor(NOW.getTime() * 1.01);
        final double upperThresholdCeiling = Math.ceil(NOW.getTime() * 1.01);
        builder.add(new TestInput(new Date((long) upperThresholdFloor), tolerance, true));
        // If the floor equals the ceiling then it's not a fraction.
        if (upperThresholdFloor != upperThresholdCeiling) {
           builder.add(new TestInput(new Date((long) upperThresholdCeiling), tolerance, false));
        }
        builder.add(new TestInput(new Date((long) (NOW.getTime() * 1.011)), tolerance, false));
        builder.add(new TestInput(new Date((long) (NOW.getTime() * 1.015)), tolerance, false));
        builder.add(new TestInput(new Date(NOW.getTime() + ONE_YEAR_IN_MILLIS), tolerance, false));
        builder.add(new TestInput(new Date(Long.MAX_VALUE - ONE_YEAR_IN_MILLIS), tolerance, false));
        // Tolerance 100.0%
        tolerance = new Tolerance(1.00, ToleranceType.PERCENTAGE);
        builder.add(new TestInput(new Date(0L), tolerance, true));
        builder.add(new TestInput(new Date(NOW.getTime() - ONE_YEAR_IN_MILLIS), tolerance, true));
        builder.add(new TestInput(new Date(NOW.getTime() - 10000), tolerance, true));
        builder.add(new TestInput(new Date(NOW.getTime() - 2000), tolerance, true));
        builder.add(new TestInput(new Date(NOW.getTime() - 1001), tolerance, true));
        builder.add(new TestInput(new Date(NOW.getTime() - 1000), tolerance, true));
        builder.add(new TestInput(new Date(NOW.getTime() - 999), tolerance, true));
        builder.add(new TestInput(new Date(NOW.getTime() - 3), tolerance, true));
        builder.add(new TestInput(new Date(NOW.getTime() - 2), tolerance, true));
        builder.add(new TestInput(new Date(NOW.getTime() - 1), tolerance, true));
        builder.add(new TestInput(NOW, tolerance, true)); // Equals value
        builder.add(new TestInput(new Date(NOW.getTime() + 1), tolerance, true));
        builder.add(new TestInput(new Date(NOW.getTime() + 2), tolerance, true));
        builder.add(new TestInput(new Date(NOW.getTime() + 3), tolerance, true));
        builder.add(new TestInput(new Date(NOW.getTime() + 999), tolerance, true));
        builder.add(new TestInput(new Date(NOW.getTime() + 1000), tolerance, true));
        builder.add(new TestInput(new Date(NOW.getTime() + 1001), tolerance, true));
        builder.add(new TestInput(new Date(NOW.getTime() + 2000), tolerance, true));
        builder.add(new TestInput(new Date(NOW.getTime() + 10000), tolerance, true));
        builder.add(new TestInput(new Date(NOW.getTime() + ONE_YEAR_IN_MILLIS), tolerance, true));
        builder.add(new TestInput(new Date(Long.MAX_VALUE - ONE_YEAR_IN_MILLIS), tolerance, true));

        final ImmutableList<TestInput> testInputs = builder.build();

        testProperty(testInputs, PERSON_TYPE_URI, HAS_EXPIRATION_DATE);
    }

    @Test
    public void testDateTimeProperty() throws SmartUriException {
        System.out.println("DateTime Property Test");
        final DateTime dob = new DateTime(NOW).minusYears(40);
        final long ONE_YEAR_IN_MILLIS = 1000L * 60L * 60L * 24L * 365L;
        final ImmutableList.Builder<TestInput> builder = ImmutableList.builder();
        // Tolerance 0.0
        Tolerance tolerance = new Tolerance(0.0, ToleranceType.DIFFERENCE);
        builder.add(new TestInput(new DateTime(0L), tolerance, false));
        builder.add(new TestInput(dob.minusYears(1), tolerance, false));
        builder.add(new TestInput(dob.minus(10000), tolerance, false));
        builder.add(new TestInput(dob.minus(2000), tolerance, false));
        builder.add(new TestInput(dob.minus(1001), tolerance, false));
        builder.add(new TestInput(dob.minus(1000), tolerance, false));
        builder.add(new TestInput(dob.minus(999), tolerance, false));
        builder.add(new TestInput(dob.minus(3), tolerance, false));
        builder.add(new TestInput(dob.minus(2), tolerance, false));
        builder.add(new TestInput(dob.minus(1), tolerance, false));
        builder.add(new TestInput(dob, tolerance, true)); // Equals value
        builder.add(new TestInput(dob.plus(1), tolerance, false));
        builder.add(new TestInput(dob.plus(2), tolerance, false));
        builder.add(new TestInput(dob.plus(3), tolerance, false));
        builder.add(new TestInput(dob.plus(999), tolerance, false));
        builder.add(new TestInput(dob.plus(1000), tolerance, false));
        builder.add(new TestInput(dob.plus(1001), tolerance, false));
        builder.add(new TestInput(dob.plus(2000), tolerance, false));
        builder.add(new TestInput(dob.plus(10000), tolerance, false));
        builder.add(new TestInput(dob.plusYears(1), tolerance, false));
        builder.add(new TestInput(new DateTime(Long.MAX_VALUE - ONE_YEAR_IN_MILLIS), tolerance, false));
        // Tolerance 1.0
        tolerance = new Tolerance(1.0, ToleranceType.DIFFERENCE);
        builder.add(new TestInput(new DateTime(0L), tolerance, false));
        builder.add(new TestInput(dob.minusYears(1), tolerance, false));
        builder.add(new TestInput(dob.minus(10000), tolerance, false));
        builder.add(new TestInput(dob.minus(2000), tolerance, false));
        builder.add(new TestInput(dob.minus(1001), tolerance, false));
        builder.add(new TestInput(dob.minus(1000), tolerance, false));
        builder.add(new TestInput(dob.minus(999), tolerance, false));
        builder.add(new TestInput(dob.minus(3), tolerance, false));
        builder.add(new TestInput(dob.minus(2), tolerance, false));
        builder.add(new TestInput(dob.minus(1), tolerance, true));
        builder.add(new TestInput(dob, tolerance, true)); // Equals value
        builder.add(new TestInput(dob.plus(1), tolerance, true));
        builder.add(new TestInput(dob.plus(2), tolerance, false));
        builder.add(new TestInput(dob.plus(3), tolerance, false));
        builder.add(new TestInput(dob.plus(999), tolerance, false));
        builder.add(new TestInput(dob.plus(1000), tolerance, false));
        builder.add(new TestInput(dob.plus(1001), tolerance, false));
        builder.add(new TestInput(dob.plus(2000), tolerance, false));
        builder.add(new TestInput(dob.plus(10000), tolerance, false));
        builder.add(new TestInput(dob.plusYears(1), tolerance, false));
        builder.add(new TestInput(new DateTime(Long.MAX_VALUE - ONE_YEAR_IN_MILLIS), tolerance, false));
        // Tolerance 2.0
        tolerance = new Tolerance(2.0, ToleranceType.DIFFERENCE);
        builder.add(new TestInput(new DateTime(0L), tolerance, false));
        builder.add(new TestInput(dob.minusYears(1), tolerance, false));
        builder.add(new TestInput(dob.minus(10000), tolerance, false));
        builder.add(new TestInput(dob.minus(2000), tolerance, false));
        builder.add(new TestInput(dob.minus(1001), tolerance, false));
        builder.add(new TestInput(dob.minus(1000), tolerance, false));
        builder.add(new TestInput(dob.minus(999), tolerance, false));
        builder.add(new TestInput(dob.minus(3), tolerance, false));
        builder.add(new TestInput(dob.minus(2), tolerance, true));
        builder.add(new TestInput(dob.minus(1), tolerance, true));
        builder.add(new TestInput(dob, tolerance, true)); // Equals value
        builder.add(new TestInput(dob.plus(1), tolerance, true));
        builder.add(new TestInput(dob.plus(2), tolerance, true));
        builder.add(new TestInput(dob.plus(3), tolerance, false));
        builder.add(new TestInput(dob.plus(999), tolerance, false));
        builder.add(new TestInput(dob.plus(1000), tolerance, false));
        builder.add(new TestInput(dob.plus(1001), tolerance, false));
        builder.add(new TestInput(dob.plus(2000), tolerance, false));
        builder.add(new TestInput(dob.plus(10000), tolerance, false));
        builder.add(new TestInput(dob.plusYears(1), tolerance, false));
        builder.add(new TestInput(new DateTime(Long.MAX_VALUE - ONE_YEAR_IN_MILLIS), tolerance, false));

        // Tolerance 0.0%
        tolerance = new Tolerance(0.00, ToleranceType.PERCENTAGE);
        builder.add(new TestInput(new DateTime(0L), tolerance, false));
        builder.add(new TestInput(dob.minusYears(1), tolerance, false));
        builder.add(new TestInput(dob.minus(10000), tolerance, false));
        builder.add(new TestInput(dob.minus(2000), tolerance, false));
        builder.add(new TestInput(dob.minus(1001), tolerance, false));
        builder.add(new TestInput(dob.minus(1000), tolerance, false));
        builder.add(new TestInput(dob.minus(999), tolerance, false));
        builder.add(new TestInput(dob.minus(3), tolerance, false));
        builder.add(new TestInput(dob.minus(2), tolerance, false));
        builder.add(new TestInput(dob.minus(1), tolerance, false));
        builder.add(new TestInput(dob, tolerance, true)); // Equals value
        builder.add(new TestInput(dob.plus(1), tolerance, false));
        builder.add(new TestInput(dob.plus(2), tolerance, false));
        builder.add(new TestInput(dob.plus(3), tolerance, false));
        builder.add(new TestInput(dob.plus(999), tolerance, false));
        builder.add(new TestInput(dob.plus(1000), tolerance, false));
        builder.add(new TestInput(dob.plus(1001), tolerance, false));
        builder.add(new TestInput(dob.plus(2000), tolerance, false));
        builder.add(new TestInput(dob.plus(10000), tolerance, false));
        builder.add(new TestInput(dob.plusYears(1), tolerance, false));
        builder.add(new TestInput(new DateTime(Long.MAX_VALUE - ONE_YEAR_IN_MILLIS), tolerance, false));
        // Tolerance 1.0%
        tolerance = new Tolerance(0.01, ToleranceType.PERCENTAGE);
        builder.add(new TestInput(new DateTime(0L), tolerance, false));
        builder.add(new TestInput(dob.minusYears(1), tolerance, false));
        builder.add(new TestInput(new DateTime((long) (dob.getMillis() * 0.985)), tolerance, false));
        builder.add(new TestInput(new DateTime((long) (dob.getMillis() * 0.989)), tolerance, false));
        // It's tricky near the exact threshold since it might create a fraction
        // of a time which is rounded. Check if it's fraction and test it with
        // the floor and ceiling values if it is. Otherwise, use the exact value
        // if it is not a fraction.
        final double lowerThresholdFloor = Math.floor(dob.getMillis() * 0.99);
        final double lowerThresholdCeiling = Math.ceil(dob.getMillis() * 0.99);
        // If the floor equals the ceiling then it's not a fraction.
        if (lowerThresholdFloor != lowerThresholdCeiling) {
           builder.add(new TestInput(new DateTime((long) lowerThresholdFloor), tolerance, false));
        }
        builder.add(new TestInput(new DateTime((long) lowerThresholdCeiling), tolerance, true));
        builder.add(new TestInput(new DateTime((long) (dob.getMillis() * 0.991)), tolerance, true));
        builder.add(new TestInput(new DateTime((long) (dob.getMillis() * 0.995)), tolerance, true));
        builder.add(new TestInput(dob.minus(10000), tolerance, true));
        builder.add(new TestInput(dob.minus(2000), tolerance, true));
        builder.add(new TestInput(dob.minus(1001), tolerance, true));
        builder.add(new TestInput(dob.minus(1000), tolerance, true));
        builder.add(new TestInput(dob.minus(999), tolerance, true));
        builder.add(new TestInput(dob.minus(3), tolerance, true));
        builder.add(new TestInput(dob.minus(2), tolerance, true));
        builder.add(new TestInput(dob.minus(1), tolerance, true));
        builder.add(new TestInput(dob, tolerance, true)); // Equals value
        builder.add(new TestInput(dob.plus(1), tolerance, true));
        builder.add(new TestInput(dob.plus(2), tolerance, true));
        builder.add(new TestInput(dob.plus(3), tolerance, true));
        builder.add(new TestInput(dob.plus(999), tolerance, true));
        builder.add(new TestInput(dob.plus(1000), tolerance, true));
        builder.add(new TestInput(dob.plus(1001), tolerance, true));
        builder.add(new TestInput(dob.plus(2000), tolerance, true));
        builder.add(new TestInput(dob.plus(10000), tolerance, true));
        builder.add(new TestInput(new DateTime((long) (dob.getMillis() * 1.005)), tolerance, true));
        builder.add(new TestInput(new DateTime((long) (dob.getMillis() * 1.009)), tolerance, true));
        // It's tricky near the exact threshold since it might create a fraction
        // of a time which is rounded. Check if it's fraction and test it with
        // the floor and ceiling values if it is. Otherwise, use the exact value
        // if it is not a fraction.
        final double upperThresholdFloor = Math.floor(dob.getMillis() * 1.01);
        final double upperThresholdCeiling = Math.ceil(dob.getMillis() * 1.01);
        builder.add(new TestInput(new DateTime((long) upperThresholdFloor), tolerance, true));
        // If the floor equals the ceiling then it's not a fraction.
        if (upperThresholdFloor != upperThresholdCeiling) {
           builder.add(new TestInput(new DateTime((long) upperThresholdCeiling), tolerance, false));
        }
        builder.add(new TestInput(new DateTime((long) (dob.getMillis() * 1.011)), tolerance, false));
        builder.add(new TestInput(new DateTime((long) (dob.getMillis() * 1.015)), tolerance, false));
        builder.add(new TestInput(dob.plusYears(1), tolerance, false));
        builder.add(new TestInput(new DateTime(Long.MAX_VALUE - ONE_YEAR_IN_MILLIS), tolerance, false));
        // Tolerance 100.0%
        tolerance = new Tolerance(1.00, ToleranceType.PERCENTAGE);
        builder.add(new TestInput(new DateTime(0L), tolerance, true));
        builder.add(new TestInput(dob.minusYears(1), tolerance, true));
        builder.add(new TestInput(dob.minus(10000), tolerance, true));
        builder.add(new TestInput(dob.minus(2000), tolerance, true));
        builder.add(new TestInput(dob.minus(1001), tolerance, true));
        builder.add(new TestInput(dob.minus(1000), tolerance, true));
        builder.add(new TestInput(dob.minus(999), tolerance, true));
        builder.add(new TestInput(dob.minus(3), tolerance, true));
        builder.add(new TestInput(dob.minus(2), tolerance, true));
        builder.add(new TestInput(dob.minus(1), tolerance, true));
        builder.add(new TestInput(dob, tolerance, true)); // Equals value
        builder.add(new TestInput(dob.plus(1), tolerance, true));
        builder.add(new TestInput(dob.plus(2), tolerance, true));
        builder.add(new TestInput(dob.plus(3), tolerance, true));
        builder.add(new TestInput(dob.plus(999), tolerance, true));
        builder.add(new TestInput(dob.plus(1000), tolerance, true));
        builder.add(new TestInput(dob.plus(1001), tolerance, true));
        builder.add(new TestInput(dob.plus(2000), tolerance, true));
        builder.add(new TestInput(dob.plus(10000), tolerance, true));
        builder.add(new TestInput(dob.plusYears(1), tolerance, true));
        builder.add(new TestInput(new DateTime(Long.MAX_VALUE - ONE_YEAR_IN_MILLIS), tolerance, true));

        final ImmutableList<TestInput> testInputs = builder.build();

        testProperty(testInputs, PERSON_TYPE_URI, HAS_DATE_OF_BIRTH);
    }

    @Test
    public void testDoubleProperty() throws SmartUriException {
        System.out.println("Double Property Test");
        final ImmutableList.Builder<TestInput> builder = ImmutableList.builder();
        // Tolerance 0.0
        Tolerance tolerance = new Tolerance(0.0, ToleranceType.DIFFERENCE);
        builder.add(new TestInput(Double.MIN_VALUE, tolerance, false));
        builder.add(new TestInput(-1.0, tolerance, false));
        builder.add(new TestInput(0.0, tolerance, false));
        builder.add(new TestInput(0.01, tolerance, false));
        builder.add(new TestInput(0.02, tolerance, false));
        builder.add(new TestInput(0.1, tolerance, false));
        builder.add(new TestInput(0.2, tolerance, false));
        builder.add(new TestInput(1.0, tolerance, false));
        builder.add(new TestInput(71, tolerance, false));
        builder.add(new TestInput(72, tolerance, false));
        builder.add(new TestInput(72.4, tolerance, false));
        builder.add(new TestInput(72.47, tolerance, false));
        builder.add(new TestInput(72.48, tolerance, false));
        builder.add(new TestInput(72.49, tolerance, false));
        builder.add(new TestInput(72.5, tolerance, true)); // Equals value
        builder.add(new TestInput(72.51, tolerance, false));
        builder.add(new TestInput(72.52, tolerance, false));
        builder.add(new TestInput(72.53, tolerance, false));
        builder.add(new TestInput(72.6, tolerance, false));
        builder.add(new TestInput(73, tolerance, false));
        builder.add(new TestInput(74, tolerance, false));
        builder.add(new TestInput(100, tolerance, false));
        builder.add(new TestInput(Double.MAX_VALUE, tolerance, false));
        // Tolerance 0.01
        tolerance = new Tolerance(0.01, ToleranceType.DIFFERENCE);
        builder.add(new TestInput(Double.MIN_VALUE, tolerance, false));
        builder.add(new TestInput(-1.0, tolerance, false));
        builder.add(new TestInput(0.0, tolerance, false));
        builder.add(new TestInput(0.01, tolerance, false));
        builder.add(new TestInput(0.02, tolerance, false));
        builder.add(new TestInput(0.1, tolerance, false));
        builder.add(new TestInput(0.2, tolerance, false));
        builder.add(new TestInput(1.0, tolerance, false));
        builder.add(new TestInput(71, tolerance, false));
        builder.add(new TestInput(72, tolerance, false));
        builder.add(new TestInput(72.4, tolerance, false));
        builder.add(new TestInput(72.47, tolerance, false));
        builder.add(new TestInput(72.48, tolerance, false));
        builder.add(new TestInput(72.49, tolerance, true));
        builder.add(new TestInput(72.5, tolerance, true)); // Equals value
        builder.add(new TestInput(72.51, tolerance, true));
        builder.add(new TestInput(72.52, tolerance, false));
        builder.add(new TestInput(72.53, tolerance, false));
        builder.add(new TestInput(72.6, tolerance, false));
        builder.add(new TestInput(73, tolerance, false));
        builder.add(new TestInput(74, tolerance, false));
        builder.add(new TestInput(100, tolerance, false));
        builder.add(new TestInput(Double.MAX_VALUE, tolerance, false));
        // Tolerance 0.02
        tolerance = new Tolerance(0.02, ToleranceType.DIFFERENCE);
        builder.add(new TestInput(Double.MIN_VALUE, tolerance, false));
        builder.add(new TestInput(-1.0, tolerance, false));
        builder.add(new TestInput(0.0, tolerance, false));
        builder.add(new TestInput(0.01, tolerance, false));
        builder.add(new TestInput(0.02, tolerance, false));
        builder.add(new TestInput(0.1, tolerance, false));
        builder.add(new TestInput(0.2, tolerance, false));
        builder.add(new TestInput(1.0, tolerance, false));
        builder.add(new TestInput(71, tolerance, false));
        builder.add(new TestInput(72, tolerance, false));
        builder.add(new TestInput(72.4, tolerance, false));
        builder.add(new TestInput(72.47, tolerance, false));
        builder.add(new TestInput(72.48, tolerance, true));
        builder.add(new TestInput(72.49, tolerance, true));
        builder.add(new TestInput(72.5, tolerance, true)); // Equals value
        builder.add(new TestInput(72.51, tolerance, true));
        builder.add(new TestInput(72.52, tolerance, true));
        builder.add(new TestInput(72.53, tolerance, false));
        builder.add(new TestInput(72.6, tolerance, false));
        builder.add(new TestInput(73, tolerance, false));
        builder.add(new TestInput(74, tolerance, false));
        builder.add(new TestInput(100, tolerance, false));
        builder.add(new TestInput(Double.MAX_VALUE, tolerance, false));

        // Tolerance 0%
        tolerance = new Tolerance(0.0, ToleranceType.PERCENTAGE);
        builder.add(new TestInput(Double.MIN_VALUE, tolerance, false));
        builder.add(new TestInput(-1.0, tolerance, false));
        builder.add(new TestInput(0.0, tolerance, false));
        builder.add(new TestInput(0.01, tolerance, false));
        builder.add(new TestInput(0.02, tolerance, false));
        builder.add(new TestInput(0.1, tolerance, false));
        builder.add(new TestInput(0.2, tolerance, false));
        builder.add(new TestInput(1.0, tolerance, false));
        builder.add(new TestInput(71, tolerance, false));
        builder.add(new TestInput(71.774, tolerance, false));
        builder.add(new TestInput(71.775, tolerance, false));
        builder.add(new TestInput(71.776, tolerance, false));
        builder.add(new TestInput(72, tolerance, false));
        builder.add(new TestInput(72.4, tolerance, false));
        builder.add(new TestInput(72.47, tolerance, false));
        builder.add(new TestInput(72.48, tolerance, false));
        builder.add(new TestInput(72.49, tolerance, false));
        builder.add(new TestInput(72.5, tolerance, true)); // Equals value
        builder.add(new TestInput(72.51, tolerance, false));
        builder.add(new TestInput(72.52, tolerance, false));
        builder.add(new TestInput(72.53, tolerance, false));
        builder.add(new TestInput(72.6, tolerance, false));
        builder.add(new TestInput(73, tolerance, false));
        builder.add(new TestInput(73.224, tolerance, false));
        builder.add(new TestInput(73.225, tolerance, false));
        builder.add(new TestInput(73.226, tolerance, false));
        builder.add(new TestInput(73, tolerance, false));
        builder.add(new TestInput(74, tolerance, false));
        builder.add(new TestInput(100, tolerance, false));
        builder.add(new TestInput(Double.MAX_VALUE, tolerance, false));
        // Tolerance 1%
        tolerance = new Tolerance(0.01, ToleranceType.PERCENTAGE);
        builder.add(new TestInput(Double.MIN_VALUE, tolerance, false));
        builder.add(new TestInput(-1.0, tolerance, false));
        builder.add(new TestInput(0.0, tolerance, false));
        builder.add(new TestInput(0.01, tolerance, false));
        builder.add(new TestInput(0.02, tolerance, false));
        builder.add(new TestInput(0.1, tolerance, false));
        builder.add(new TestInput(0.2, tolerance, false));
        builder.add(new TestInput(1.0, tolerance, false));
        builder.add(new TestInput(71, tolerance, false));
        builder.add(new TestInput(71.774, tolerance, false));
        builder.add(new TestInput(71.775, tolerance, true));
        builder.add(new TestInput(71.776, tolerance, true));
        builder.add(new TestInput(72, tolerance, true));
        builder.add(new TestInput(72.4, tolerance, true));
        builder.add(new TestInput(72.47, tolerance, true));
        builder.add(new TestInput(72.48, tolerance, true));
        builder.add(new TestInput(72.49, tolerance, true));
        builder.add(new TestInput(72.5, tolerance, true)); // Equals value
        builder.add(new TestInput(72.51, tolerance, true));
        builder.add(new TestInput(72.52, tolerance, true));
        builder.add(new TestInput(72.53, tolerance, true));
        builder.add(new TestInput(72.6, tolerance, true));
        builder.add(new TestInput(73, tolerance, true));
        builder.add(new TestInput(73.224, tolerance, true));
        builder.add(new TestInput(73.225, tolerance, true));
        builder.add(new TestInput(73.226, tolerance, false));
        builder.add(new TestInput(74, tolerance, false));
        builder.add(new TestInput(100, tolerance, false));
        builder.add(new TestInput(Double.MAX_VALUE, tolerance, false));
        // Tolerance 100%
        tolerance = new Tolerance(1.00, ToleranceType.PERCENTAGE);
        builder.add(new TestInput(Double.MIN_VALUE, tolerance, true));
        builder.add(new TestInput(-1.0, tolerance, true));
        builder.add(new TestInput(0.0, tolerance, true));
        builder.add(new TestInput(0.01, tolerance, true));
        builder.add(new TestInput(0.02, tolerance, true));
        builder.add(new TestInput(0.1, tolerance, true));
        builder.add(new TestInput(0.2, tolerance, true));
        builder.add(new TestInput(1.0, tolerance, true));
        builder.add(new TestInput(71, tolerance, true));
        builder.add(new TestInput(71.774, tolerance, true));
        builder.add(new TestInput(71.775, tolerance, true));
        builder.add(new TestInput(71.776, tolerance, true));
        builder.add(new TestInput(72, tolerance, true));
        builder.add(new TestInput(72.4, tolerance, true));
        builder.add(new TestInput(72.47, tolerance, true));
        builder.add(new TestInput(72.48, tolerance, true));
        builder.add(new TestInput(72.49, tolerance, true));
        builder.add(new TestInput(72.5, tolerance, true)); // Equals value
        builder.add(new TestInput(72.51, tolerance, true));
        builder.add(new TestInput(72.52, tolerance, true));
        builder.add(new TestInput(72.53, tolerance, true));
        builder.add(new TestInput(72.6, tolerance, true));
        builder.add(new TestInput(73, tolerance, true));
        builder.add(new TestInput(73.224, tolerance, true));
        builder.add(new TestInput(73.225, tolerance, true));
        builder.add(new TestInput(73.226, tolerance, true));
        builder.add(new TestInput(74, tolerance, true));
        builder.add(new TestInput(100, tolerance, true));
        builder.add(new TestInput(Double.MAX_VALUE, tolerance, true));

        final ImmutableList<TestInput> testInputs = builder.build();

        testProperty(testInputs, PERSON_TYPE_URI, HAS_HEIGHT);
    }

    @Test
    public void testFloatProperty() throws SmartUriException {
        System.out.println("Float Property Test");
        final ImmutableList.Builder<TestInput> builder = ImmutableList.builder();
        // Tolerance 0.0
        Tolerance tolerance = new Tolerance(0.0, ToleranceType.DIFFERENCE);
        builder.add(new TestInput(Float.MIN_VALUE, tolerance, false));
        builder.add(new TestInput(-1.0f, tolerance, false));
        builder.add(new TestInput(0.0f, tolerance, false));
        builder.add(new TestInput(0.01f, tolerance, false));
        builder.add(new TestInput(0.02f, tolerance, false));
        builder.add(new TestInput(0.1f, tolerance, false));
        builder.add(new TestInput(0.2f, tolerance, false));
        builder.add(new TestInput(1.0f, tolerance, false));
        builder.add(new TestInput(250f, tolerance, false));
        builder.add(new TestInput(250.7f, tolerance, false));
        builder.add(new TestInput(250.72f, tolerance, false));
        builder.add(new TestInput(250.73f, tolerance, false));
        builder.add(new TestInput(250.74f, tolerance, false));
        builder.add(new TestInput(250.75f, tolerance, true)); // Equals value
        builder.add(new TestInput(250.76f, tolerance, false));
        builder.add(new TestInput(250.77f, tolerance, false));
        builder.add(new TestInput(250.78f, tolerance, false));
        builder.add(new TestInput(250.8f, tolerance, false));
        builder.add(new TestInput(251f, tolerance, false));
        builder.add(new TestInput(300.0f, tolerance, false));
        builder.add(new TestInput(Float.MAX_VALUE, tolerance, false));
        // Tolerance 0.01
        tolerance = new Tolerance(0.01, ToleranceType.DIFFERENCE);
        builder.add(new TestInput(Float.MIN_VALUE, tolerance, false));
        builder.add(new TestInput(-1.0f, tolerance, false));
        builder.add(new TestInput(0.0f, tolerance, false));
        builder.add(new TestInput(0.01f, tolerance, false));
        builder.add(new TestInput(0.02f, tolerance, false));
        builder.add(new TestInput(0.1f, tolerance, false));
        builder.add(new TestInput(0.2f, tolerance, false));
        builder.add(new TestInput(1.0f, tolerance, false));
        builder.add(new TestInput(250f, tolerance, false));
        builder.add(new TestInput(250.7f, tolerance, false));
        builder.add(new TestInput(250.72f, tolerance, false));
        builder.add(new TestInput(250.73f, tolerance, false));
        builder.add(new TestInput(250.74f, tolerance, true));
        builder.add(new TestInput(250.75f, tolerance, true)); // Equals value
        builder.add(new TestInput(250.76f, tolerance, true));
        builder.add(new TestInput(250.77f, tolerance, false));
        builder.add(new TestInput(250.78f, tolerance, false));
        builder.add(new TestInput(250.8f, tolerance, false));
        builder.add(new TestInput(251f, tolerance, false));
        builder.add(new TestInput(300.0f, tolerance, false));
        builder.add(new TestInput(Float.MAX_VALUE, tolerance, false));
        // Tolerance 0.02
        tolerance = new Tolerance(0.02, ToleranceType.DIFFERENCE);
        builder.add(new TestInput(Float.MIN_VALUE, tolerance, false));
        builder.add(new TestInput(-1.0f, tolerance, false));
        builder.add(new TestInput(0.0f, tolerance, false));
        builder.add(new TestInput(0.01f, tolerance, false));
        builder.add(new TestInput(0.02f, tolerance, false));
        builder.add(new TestInput(0.1f, tolerance, false));
        builder.add(new TestInput(0.2f, tolerance, false));
        builder.add(new TestInput(1.0f, tolerance, false));
        builder.add(new TestInput(250f, tolerance, false));
        builder.add(new TestInput(250.7f, tolerance, false));
        builder.add(new TestInput(250.72f, tolerance, false));
        builder.add(new TestInput(250.73f, tolerance, true));
        builder.add(new TestInput(250.74f, tolerance, true));
        builder.add(new TestInput(250.75f, tolerance, true)); // Equals value
        builder.add(new TestInput(250.76f, tolerance, true));
        builder.add(new TestInput(250.77f, tolerance, true));
        builder.add(new TestInput(250.78f, tolerance, false));
        builder.add(new TestInput(250.8f, tolerance, false));
        builder.add(new TestInput(251f, tolerance, false));
        builder.add(new TestInput(300.0f, tolerance, false));
        builder.add(new TestInput(Float.MAX_VALUE, tolerance, false));

        // Tolerance 0.0%
        tolerance = new Tolerance(0.0, ToleranceType.PERCENTAGE);
        builder.add(new TestInput(Float.MIN_VALUE, tolerance, false));
        builder.add(new TestInput(-1.0f, tolerance, false));
        builder.add(new TestInput(0.0f, tolerance, false));
        builder.add(new TestInput(0.01f, tolerance, false));
        builder.add(new TestInput(0.02f, tolerance, false));
        builder.add(new TestInput(0.1f, tolerance, false));
        builder.add(new TestInput(0.2f, tolerance, false));
        builder.add(new TestInput(1.0f, tolerance, false));
        builder.add(new TestInput(248.2424f, tolerance, false));
        builder.add(new TestInput(248.2425f, tolerance, false));
        builder.add(new TestInput(248.2426f, tolerance, false));
        builder.add(new TestInput(250f, tolerance, false));
        builder.add(new TestInput(250.7f, tolerance, false));
        builder.add(new TestInput(250.72f, tolerance, false));
        builder.add(new TestInput(250.73f, tolerance, false));
        builder.add(new TestInput(250.74f, tolerance, false));
        builder.add(new TestInput(250.75f, tolerance, true)); // Equals value
        builder.add(new TestInput(250.76f, tolerance, false));
        builder.add(new TestInput(250.77f, tolerance, false));
        builder.add(new TestInput(250.78f, tolerance, false));
        builder.add(new TestInput(250.8f, tolerance, false));
        builder.add(new TestInput(251f, tolerance, false));
        builder.add(new TestInput(253.2574f, tolerance, false));
        builder.add(new TestInput(253.2575f, tolerance, false));
        builder.add(new TestInput(253.2576f, tolerance, false));
        builder.add(new TestInput(300.0f, tolerance, false));
        builder.add(new TestInput(Float.MAX_VALUE, tolerance, false));
        // Tolerance 1.0%
        tolerance = new Tolerance(0.01, ToleranceType.PERCENTAGE);
        builder.add(new TestInput(Float.MIN_VALUE, tolerance, false));
        builder.add(new TestInput(-1.0f, tolerance, false));
        builder.add(new TestInput(0.0f, tolerance, false));
        builder.add(new TestInput(0.01f, tolerance, false));
        builder.add(new TestInput(0.02f, tolerance, false));
        builder.add(new TestInput(0.1f, tolerance, false));
        builder.add(new TestInput(0.2f, tolerance, false));
        builder.add(new TestInput(1.0f, tolerance, false));
        builder.add(new TestInput(248.2424f, tolerance, false));
        builder.add(new TestInput(248.2425f, tolerance, true));
        builder.add(new TestInput(248.2426f, tolerance, true));
        builder.add(new TestInput(250f, tolerance, true));
        builder.add(new TestInput(250.7f, tolerance, true));
        builder.add(new TestInput(250.72f, tolerance, true));
        builder.add(new TestInput(250.73f, tolerance, true));
        builder.add(new TestInput(250.74f, tolerance, true));
        builder.add(new TestInput(250.75f, tolerance, true)); // Equals value
        builder.add(new TestInput(250.76f, tolerance, true));
        builder.add(new TestInput(250.77f, tolerance, true));
        builder.add(new TestInput(250.78f, tolerance, true));
        builder.add(new TestInput(250.8f, tolerance, true));
        builder.add(new TestInput(251f, tolerance, true));
        builder.add(new TestInput(253.2574f, tolerance, true));
        builder.add(new TestInput(253.2575f, tolerance, true));
        builder.add(new TestInput(253.2576f, tolerance, false));
        builder.add(new TestInput(300.0f, tolerance, false));
        builder.add(new TestInput(Float.MAX_VALUE, tolerance, false));
        // Tolerance 100.0%
        tolerance = new Tolerance(1.00, ToleranceType.PERCENTAGE);
        builder.add(new TestInput(Float.MIN_VALUE, tolerance, true));
        builder.add(new TestInput(-1.0f, tolerance, true));
        builder.add(new TestInput(0.0f, tolerance, true));
        builder.add(new TestInput(0.01f, tolerance, true));
        builder.add(new TestInput(0.02f, tolerance, true));
        builder.add(new TestInput(0.1f, tolerance, true));
        builder.add(new TestInput(0.2f, tolerance, true));
        builder.add(new TestInput(1.0f, tolerance, true));
        builder.add(new TestInput(248.2424f, tolerance, true));
        builder.add(new TestInput(248.2425f, tolerance, true));
        builder.add(new TestInput(248.2426f, tolerance, true));
        builder.add(new TestInput(250f, tolerance, true));
        builder.add(new TestInput(250.7f, tolerance, true));
        builder.add(new TestInput(250.72f, tolerance, true));
        builder.add(new TestInput(250.73f, tolerance, true));
        builder.add(new TestInput(250.74f, tolerance, true));
        builder.add(new TestInput(250.75f, tolerance, true)); // Equals value
        builder.add(new TestInput(250.76f, tolerance, true));
        builder.add(new TestInput(250.77f, tolerance, true));
        builder.add(new TestInput(250.78f, tolerance, true));
        builder.add(new TestInput(250.8f, tolerance, true));
        builder.add(new TestInput(251f, tolerance, true));
        builder.add(new TestInput(253.2574f, tolerance, true));
        builder.add(new TestInput(253.2575f, tolerance, true));
        builder.add(new TestInput(253.2576f, tolerance, true));
        builder.add(new TestInput(300.0f, tolerance, true));
        builder.add(new TestInput(Float.MAX_VALUE, tolerance, true));

        final ImmutableList<TestInput> testInputs = builder.build();

        testProperty(testInputs, PERSON_TYPE_URI, HAS_WEIGHT);
    }

    @Test
    public void testIntegerProperty() throws SmartUriException {
        System.out.println("Integer Property Test");
        final ImmutableList.Builder<TestInput> builder = ImmutableList.builder();
        // Tolerance 0.0
        Tolerance tolerance = new Tolerance(0.0, ToleranceType.DIFFERENCE);
        builder.add(new TestInput(Integer.MIN_VALUE, tolerance, false));
        builder.add(new TestInput(-1, tolerance, false));
        builder.add(new TestInput(0, tolerance, false));
        builder.add(new TestInput(1, tolerance, false));
        builder.add(new TestInput(49997, tolerance, false));
        builder.add(new TestInput(49998, tolerance, false));
        builder.add(new TestInput(49999, tolerance, false));
        builder.add(new TestInput(50000, tolerance, true)); // Equals value
        builder.add(new TestInput(50001, tolerance, false));
        builder.add(new TestInput(50002, tolerance, false));
        builder.add(new TestInput(50003, tolerance, false));
        builder.add(new TestInput(60000, tolerance, false));
        builder.add(new TestInput(Integer.MAX_VALUE, tolerance, false));
        // Tolerance 1.0
        tolerance = new Tolerance(1.0, ToleranceType.DIFFERENCE);
        builder.add(new TestInput(Integer.MIN_VALUE, tolerance, false));
        builder.add(new TestInput(-1, tolerance, false));
        builder.add(new TestInput(0, tolerance, false));
        builder.add(new TestInput(1, tolerance, false));
        builder.add(new TestInput(49997, tolerance, false));
        builder.add(new TestInput(49998, tolerance, false));
        builder.add(new TestInput(49999, tolerance, true));
        builder.add(new TestInput(50000, tolerance, true)); // Equals value
        builder.add(new TestInput(50001, tolerance, true));
        builder.add(new TestInput(50002, tolerance, false));
        builder.add(new TestInput(50003, tolerance, false));
        builder.add(new TestInput(60000, tolerance, false));
        builder.add(new TestInput(Integer.MAX_VALUE, tolerance, false));
        // Tolerance 2.0
        tolerance = new Tolerance(2.0, ToleranceType.DIFFERENCE);
        builder.add(new TestInput(Integer.MIN_VALUE, tolerance, false));
        builder.add(new TestInput(-1, tolerance, false));
        builder.add(new TestInput(0, tolerance, false));
        builder.add(new TestInput(1, tolerance, false));
        builder.add(new TestInput(49997, tolerance, false));
        builder.add(new TestInput(49998, tolerance, true));
        builder.add(new TestInput(49999, tolerance, true));
        builder.add(new TestInput(50000, tolerance, true)); // Equals value
        builder.add(new TestInput(50001, tolerance, true));
        builder.add(new TestInput(50002, tolerance, true));
        builder.add(new TestInput(50003, tolerance, false));
        builder.add(new TestInput(60000, tolerance, false));
        builder.add(new TestInput(Integer.MAX_VALUE, tolerance, false));

        // Tolerance 0.0%
        tolerance = new Tolerance(0.0, ToleranceType.PERCENTAGE);
        builder.add(new TestInput(Integer.MIN_VALUE, tolerance, false));
        builder.add(new TestInput(-1, tolerance, false));
        builder.add(new TestInput(0, tolerance, false));
        builder.add(new TestInput(1, tolerance, false));
        builder.add(new TestInput(48999, tolerance, false));
        builder.add(new TestInput(49000, tolerance, false));
        builder.add(new TestInput(49001, tolerance, false));
        builder.add(new TestInput(49499, tolerance, false));
        builder.add(new TestInput(49500, tolerance, false));
        builder.add(new TestInput(49501, tolerance, false));
        builder.add(new TestInput(49997, tolerance, false));
        builder.add(new TestInput(49998, tolerance, false));
        builder.add(new TestInput(49999, tolerance, false));
        builder.add(new TestInput(50000, tolerance, true)); // Equals value
        builder.add(new TestInput(50001, tolerance, false));
        builder.add(new TestInput(50002, tolerance, false));
        builder.add(new TestInput(50003, tolerance, false));
        builder.add(new TestInput(50499, tolerance, false));
        builder.add(new TestInput(50500, tolerance, false));
        builder.add(new TestInput(50501, tolerance, false));
        builder.add(new TestInput(50999, tolerance, false));
        builder.add(new TestInput(51000, tolerance, false));
        builder.add(new TestInput(51001, tolerance, false));
        builder.add(new TestInput(60000, tolerance, false));
        builder.add(new TestInput(Integer.MAX_VALUE, tolerance, false));
        // Tolerance 1.0%
        tolerance = new Tolerance(0.01, ToleranceType.PERCENTAGE);
        builder.add(new TestInput(Integer.MIN_VALUE, tolerance, false));
        builder.add(new TestInput(-1, tolerance, false));
        builder.add(new TestInput(0, tolerance, false));
        builder.add(new TestInput(1, tolerance, false));
        builder.add(new TestInput(48999, tolerance, false));
        builder.add(new TestInput(49000, tolerance, false));
        builder.add(new TestInput(49001, tolerance, false));
        builder.add(new TestInput(49499, tolerance, false));
        builder.add(new TestInput(49500, tolerance, true));
        builder.add(new TestInput(49501, tolerance, true));
        builder.add(new TestInput(49997, tolerance, true));
        builder.add(new TestInput(49998, tolerance, true));
        builder.add(new TestInput(49999, tolerance, true));
        builder.add(new TestInput(50000, tolerance, true)); // Equals value
        builder.add(new TestInput(50001, tolerance, true));
        builder.add(new TestInput(50002, tolerance, true));
        builder.add(new TestInput(50003, tolerance, true));
        builder.add(new TestInput(50499, tolerance, true));
        builder.add(new TestInput(50500, tolerance, true));
        builder.add(new TestInput(50501, tolerance, false));
        builder.add(new TestInput(50999, tolerance, false));
        builder.add(new TestInput(51000, tolerance, false));
        builder.add(new TestInput(51001, tolerance, false));
        builder.add(new TestInput(60000, tolerance, false));
        builder.add(new TestInput(Integer.MAX_VALUE, tolerance, false));
        // Tolerance 100.0%
        tolerance = new Tolerance(1.00, ToleranceType.PERCENTAGE);
        builder.add(new TestInput(Integer.MIN_VALUE, tolerance, true));
        builder.add(new TestInput(-1, tolerance, true));
        builder.add(new TestInput(0, tolerance, true));
        builder.add(new TestInput(1, tolerance, true));
        builder.add(new TestInput(48999, tolerance, true));
        builder.add(new TestInput(49000, tolerance, true));
        builder.add(new TestInput(49001, tolerance, true));
        builder.add(new TestInput(49499, tolerance, true));
        builder.add(new TestInput(49500, tolerance, true));
        builder.add(new TestInput(49501, tolerance, true));
        builder.add(new TestInput(49997, tolerance, true));
        builder.add(new TestInput(49998, tolerance, true));
        builder.add(new TestInput(49999, tolerance, true));
        builder.add(new TestInput(50000, tolerance, true)); // Equals value
        builder.add(new TestInput(50001, tolerance, true));
        builder.add(new TestInput(50002, tolerance, true));
        builder.add(new TestInput(50003, tolerance, true));
        builder.add(new TestInput(50499, tolerance, true));
        builder.add(new TestInput(50500, tolerance, true));
        builder.add(new TestInput(50501, tolerance, true));
        builder.add(new TestInput(50999, tolerance, true));
        builder.add(new TestInput(51000, tolerance, true));
        builder.add(new TestInput(51001, tolerance, true));
        builder.add(new TestInput(60000, tolerance, true));
        builder.add(new TestInput(Integer.MAX_VALUE, tolerance, true));

        final ImmutableList<TestInput> testInputs = builder.build();

        testProperty(testInputs, PERSON_TYPE_URI, HAS_INCOME);
    }

    @Test
    public void testLongProperty() throws SmartUriException {
        System.out.println("Long Property Test");
        final ImmutableList.Builder<TestInput> builder = ImmutableList.builder();
        // Tolerance 0.0
        Tolerance tolerance = new Tolerance(0.0, ToleranceType.DIFFERENCE);
        builder.add(new TestInput(Long.MIN_VALUE, tolerance, false));
        builder.add(new TestInput(-1L, tolerance, false));
        builder.add(new TestInput(0L, tolerance, false));
        builder.add(new TestInput(1L, tolerance, false));
        builder.add(new TestInput(123456789009L, tolerance, false));
        builder.add(new TestInput(123456789010L, tolerance, false));
        builder.add(new TestInput(123456789011L, tolerance, false));
        builder.add(new TestInput(123456789012L, tolerance, true)); // Equals value
        builder.add(new TestInput(123456789013L, tolerance, false));
        builder.add(new TestInput(123456789014L, tolerance, false));
        builder.add(new TestInput(123456789015L, tolerance, false));
        builder.add(new TestInput(223456789012L, tolerance, false));
        builder.add(new TestInput(Long.MAX_VALUE, tolerance, false));
        // Tolerance 1.0
        tolerance = new Tolerance(1.0, ToleranceType.DIFFERENCE);
        builder.add(new TestInput(Long.MIN_VALUE, tolerance, false));
        builder.add(new TestInput(-1L, tolerance, false));
        builder.add(new TestInput(0L, tolerance, false));
        builder.add(new TestInput(1L, tolerance, false));
        builder.add(new TestInput(123456789009L, tolerance, false));
        builder.add(new TestInput(123456789010L, tolerance, false));
        builder.add(new TestInput(123456789011L, tolerance, true));
        builder.add(new TestInput(123456789012L, tolerance, true)); // Equals value
        builder.add(new TestInput(123456789013L, tolerance, true));
        builder.add(new TestInput(123456789014L, tolerance, false));
        builder.add(new TestInput(123456789015L, tolerance, false));
        builder.add(new TestInput(223456789012L, tolerance, false));
        builder.add(new TestInput(Long.MAX_VALUE, tolerance, false));
        // Tolerance 2.0
        tolerance = new Tolerance(2.0, ToleranceType.DIFFERENCE);
        builder.add(new TestInput(Long.MIN_VALUE, tolerance, false));
        builder.add(new TestInput(-1L, tolerance, false));
        builder.add(new TestInput(0L, tolerance, false));
        builder.add(new TestInput(1L, tolerance, false));
        builder.add(new TestInput(123456789009L, tolerance, false));
        builder.add(new TestInput(123456789010L, tolerance, true));
        builder.add(new TestInput(123456789011L, tolerance, true));
        builder.add(new TestInput(123456789012L, tolerance, true));// Equals value
        builder.add(new TestInput(123456789013L, tolerance, true));
        builder.add(new TestInput(123456789014L, tolerance, true));
        builder.add(new TestInput(123456789015L, tolerance, false));
        builder.add(new TestInput(223456789012L, tolerance, false));
        builder.add(new TestInput(Long.MAX_VALUE, tolerance, false));

        // Tolerance 0.0%
        tolerance = new Tolerance(0.0, ToleranceType.PERCENTAGE);
        builder.add(new TestInput(Long.MIN_VALUE, tolerance, false));
        builder.add(new TestInput(-1L, tolerance, false));
        builder.add(new TestInput(0L, tolerance, false));
        builder.add(new TestInput(1L, tolerance, false));
        builder.add(new TestInput(122222221121L, tolerance, false));
        builder.add(new TestInput(122222221122L, tolerance, false));
        builder.add(new TestInput(122222221123L, tolerance, false));
        builder.add(new TestInput(123456789009L, tolerance, false));
        builder.add(new TestInput(123456789010L, tolerance, false));
        builder.add(new TestInput(123456789011L, tolerance, false));
        builder.add(new TestInput(123456789012L, tolerance, true));// Equals value
        builder.add(new TestInput(123456789013L, tolerance, false));
        builder.add(new TestInput(123456789014L, tolerance, false));
        builder.add(new TestInput(123456789015L, tolerance, false));
        builder.add(new TestInput(124691356901L, tolerance, false));
        builder.add(new TestInput(124691356902L, tolerance, false));
        builder.add(new TestInput(124691356903L, tolerance, false));
        builder.add(new TestInput(223456789012L, tolerance, false));
        builder.add(new TestInput(Long.MAX_VALUE, tolerance, false));
        // Tolerance 1.0%
        tolerance = new Tolerance(0.01, ToleranceType.PERCENTAGE);
        builder.add(new TestInput(Long.MIN_VALUE, tolerance, false));
        builder.add(new TestInput(-1L, tolerance, false));
        builder.add(new TestInput(0L, tolerance, false));
        builder.add(new TestInput(1L, tolerance, false));
        builder.add(new TestInput(122222221121L, tolerance, false));
        builder.add(new TestInput(122222221122L, tolerance, true));
        builder.add(new TestInput(122222221123L, tolerance, true));
        builder.add(new TestInput(123456789009L, tolerance, true));
        builder.add(new TestInput(123456789010L, tolerance, true));
        builder.add(new TestInput(123456789011L, tolerance, true));
        builder.add(new TestInput(123456789012L, tolerance, true));// Equals value
        builder.add(new TestInput(123456789013L, tolerance, true));
        builder.add(new TestInput(123456789014L, tolerance, true));
        builder.add(new TestInput(123456789015L, tolerance, true));
        builder.add(new TestInput(124691356901L, tolerance, true));
        builder.add(new TestInput(124691356902L, tolerance, true));
        builder.add(new TestInput(124691356903L, tolerance, false));
        builder.add(new TestInput(223456789012L, tolerance, false));
        builder.add(new TestInput(Long.MAX_VALUE, tolerance, false));
        // Tolerance 100.0%
        tolerance = new Tolerance(1.00, ToleranceType.PERCENTAGE);
        builder.add(new TestInput(Long.MIN_VALUE, tolerance, true));
        builder.add(new TestInput(-1L, tolerance, true));
        builder.add(new TestInput(0L, tolerance, true));
        builder.add(new TestInput(1L, tolerance, true));
        builder.add(new TestInput(122222221121L, tolerance, true));
        builder.add(new TestInput(122222221122L, tolerance, true));
        builder.add(new TestInput(122222221123L, tolerance, true));
        builder.add(new TestInput(123456789009L, tolerance, true));
        builder.add(new TestInput(123456789010L, tolerance, true));
        builder.add(new TestInput(123456789011L, tolerance, true));
        builder.add(new TestInput(123456789012L, tolerance, true));// Equals value
        builder.add(new TestInput(123456789013L, tolerance, true));
        builder.add(new TestInput(123456789014L, tolerance, true));
        builder.add(new TestInput(123456789015L, tolerance, true));
        builder.add(new TestInput(124691356901L, tolerance, true));
        builder.add(new TestInput(124691356902L, tolerance, true));
        builder.add(new TestInput(124691356903L, tolerance, true));
        builder.add(new TestInput(223456789012L, tolerance, true));
        builder.add(new TestInput(Long.MAX_VALUE, tolerance, true));

        final ImmutableList<TestInput> testInputs = builder.build();

        testProperty(testInputs, PERSON_TYPE_URI, HAS_LICENSE_NUMBER);
    }

    @Test
    public void testShortProperty() throws SmartUriException {
        System.out.println("Short Property Test");
        final ImmutableList.Builder<TestInput> builder = ImmutableList.builder();
        // Tolerance 0.0
        Tolerance tolerance = new Tolerance(0.0, ToleranceType.DIFFERENCE);
        builder.add(new TestInput(Short.MIN_VALUE, tolerance, false));
        builder.add(new TestInput((short) -1, tolerance, false));
        builder.add(new TestInput((short) 0, tolerance, false));
        builder.add(new TestInput((short) 1, tolerance, false));
        builder.add(new TestInput((short) 37, tolerance, false));
        builder.add(new TestInput((short) 38, tolerance, false));
        builder.add(new TestInput((short) 39, tolerance, false));
        builder.add(new TestInput((short) 40, tolerance, true)); // Equals value
        builder.add(new TestInput((short) 41, tolerance, false));
        builder.add(new TestInput((short) 42, tolerance, false));
        builder.add(new TestInput((short) 43, tolerance, false));
        builder.add(new TestInput((short) 100, tolerance, false));
        builder.add(new TestInput(Short.MAX_VALUE, tolerance, false));
        // Tolerance 1.0
        tolerance = new Tolerance(1.0, ToleranceType.DIFFERENCE);
        builder.add(new TestInput(Short.MIN_VALUE, tolerance, false));
        builder.add(new TestInput((short) -1, tolerance, false));
        builder.add(new TestInput((short) 0, tolerance, false));
        builder.add(new TestInput((short) 1, tolerance, false));
        builder.add(new TestInput((short) 37, tolerance, false));
        builder.add(new TestInput((short) 38, tolerance, false));
        builder.add(new TestInput((short) 39, tolerance, true));
        builder.add(new TestInput((short) 40, tolerance, true)); // Equals value
        builder.add(new TestInput((short) 41, tolerance, true));
        builder.add(new TestInput((short) 42, tolerance, false));
        builder.add(new TestInput((short) 43, tolerance, false));
        builder.add(new TestInput((short) 100, tolerance, false));
        builder.add(new TestInput(Short.MAX_VALUE, tolerance, false));
        // Tolerance 2.0
        tolerance = new Tolerance(2.0, ToleranceType.DIFFERENCE);
        builder.add(new TestInput(Short.MIN_VALUE, tolerance, false));
        builder.add(new TestInput((short) -1, tolerance, false));
        builder.add(new TestInput((short) 0, tolerance, false));
        builder.add(new TestInput((short) 1, tolerance, false));
        builder.add(new TestInput((short) 37, tolerance, false));
        builder.add(new TestInput((short) 38, tolerance, true));
        builder.add(new TestInput((short) 39, tolerance, true));
        builder.add(new TestInput((short) 40, tolerance, true)); // Equals value
        builder.add(new TestInput((short) 41, tolerance, true));
        builder.add(new TestInput((short) 42, tolerance, true));
        builder.add(new TestInput((short) 43, tolerance, false));
        builder.add(new TestInput((short) 100, tolerance, false));
        builder.add(new TestInput(Short.MAX_VALUE, tolerance, false));

        // Tolerance 0.0%
        tolerance = new Tolerance(0.00, ToleranceType.PERCENTAGE);
        builder.add(new TestInput(Short.MIN_VALUE, tolerance, false));
        builder.add(new TestInput((short) -1, tolerance, false));
        builder.add(new TestInput((short) 0, tolerance, false));
        builder.add(new TestInput((short) 1, tolerance, false));
        builder.add(new TestInput((short) 37, tolerance, false));
        builder.add(new TestInput((short) 38, tolerance, false));
        builder.add(new TestInput((short) 39, tolerance, false));
        builder.add(new TestInput((short) 40, tolerance, true)); // Equals value
        builder.add(new TestInput((short) 41, tolerance, false));
        builder.add(new TestInput((short) 42, tolerance, false));
        builder.add(new TestInput((short) 43, tolerance, false));
        builder.add(new TestInput((short) 100, tolerance, false));
        builder.add(new TestInput(Short.MAX_VALUE, tolerance, false));
        // Tolerance 10.0%
        tolerance = new Tolerance(0.10, ToleranceType.PERCENTAGE);
        builder.add(new TestInput(Short.MIN_VALUE, tolerance, false));
        builder.add(new TestInput((short) -1, tolerance, false));
        builder.add(new TestInput((short) 0, tolerance, false));
        builder.add(new TestInput((short) 1, tolerance, false));
        builder.add(new TestInput((short) 35, tolerance, false));
        builder.add(new TestInput((short) 36, tolerance, true));
        builder.add(new TestInput((short) 37, tolerance, true));
        builder.add(new TestInput((short) 38, tolerance, true));
        builder.add(new TestInput((short) 39, tolerance, true));
        builder.add(new TestInput((short) 40, tolerance, true)); // Equals value
        builder.add(new TestInput((short) 41, tolerance, true));
        builder.add(new TestInput((short) 42, tolerance, true));
        builder.add(new TestInput((short) 43, tolerance, true));
        builder.add(new TestInput((short) 44, tolerance, true));
        builder.add(new TestInput((short) 45, tolerance, false));
        builder.add(new TestInput((short) 100, tolerance, false));
        builder.add(new TestInput(Short.MAX_VALUE, tolerance, false));
        // Tolerance 100.0%
        tolerance = new Tolerance(1.00, ToleranceType.PERCENTAGE);
        builder.add(new TestInput(Short.MIN_VALUE, tolerance, true));
        builder.add(new TestInput((short) -1, tolerance, true));
        builder.add(new TestInput((short) 0, tolerance, true));
        builder.add(new TestInput((short) 1, tolerance, true));
        builder.add(new TestInput((short) 35, tolerance, true));
        builder.add(new TestInput((short) 36, tolerance, true));
        builder.add(new TestInput((short) 37, tolerance, true));
        builder.add(new TestInput((short) 38, tolerance, true));
        builder.add(new TestInput((short) 39, tolerance, true));
        builder.add(new TestInput((short) 40, tolerance, true)); // Equals value
        builder.add(new TestInput((short) 41, tolerance, true));
        builder.add(new TestInput((short) 42, tolerance, true));
        builder.add(new TestInput((short) 43, tolerance, true));
        builder.add(new TestInput((short) 44, tolerance, true));
        builder.add(new TestInput((short) 45, tolerance, true));
        builder.add(new TestInput((short) 100, tolerance, true));
        builder.add(new TestInput(Short.MAX_VALUE, tolerance, true));

        final ImmutableList<TestInput> testInputs = builder.build();

        testProperty(testInputs, PERSON_TYPE_URI, HAS_AGE);
    }

    @Test
    public void testStringProperty() throws SmartUriException {
        System.out.println("String Property Test");
        final ImmutableList.Builder<TestInput> builder = ImmutableList.builder();
        // Tolerance 0.0
        Tolerance tolerance = new Tolerance(0.0, ToleranceType.DIFFERENCE);
        builder.add(new TestInput("123 Wrong St. Washington, DC 20024", tolerance, false));
        builder.add(new TestInput("321 Fake St. Washington, DC 20024", tolerance, false));
        builder.add(new TestInput("123 Faky St. Washington, DC 20024", tolerance, false));
        builder.add(new TestInput("123 Fake St Washington, DC 20024", tolerance, false));
        builder.add(new TestInput("123 Fake St. Washington, DC 20023", tolerance, false));
        builder.add(new TestInput("124 Fake St. Washington, DC 20024", tolerance, false));
        builder.add(new TestInput("98765 Dummy Rd. Madeuptown, ZZ 99999", tolerance, false));
        builder.add(new TestInput("123 Fake St. Washington, DC 20024", tolerance, true)); // Equals value
        // Tolerance 1.0
        tolerance = new Tolerance(1.0, ToleranceType.DIFFERENCE);
        builder.add(new TestInput("123 Wrong St. Washington, DC 20024", tolerance, false));
        builder.add(new TestInput("321 Fake St. Washington, DC 20024", tolerance, false));
        builder.add(new TestInput("123 Faky St. Washington, DC 20024", tolerance, true));
        builder.add(new TestInput("123 Fake St Washington, DC 20024", tolerance, true));
        builder.add(new TestInput("123 Fake St. Washington, DC 20023", tolerance, true));
        builder.add(new TestInput("124 Fake St. Washington, DC 20024", tolerance, true));
        builder.add(new TestInput("98765 Dummy Rd. Madeuptown, ZZ 99999", tolerance, false));
        builder.add(new TestInput("123 Fake St. Washington, DC 20024", tolerance, true)); // Equals value
        // Tolerance 2.0
        tolerance = new Tolerance(2.0, ToleranceType.DIFFERENCE);
        builder.add(new TestInput("123 Wrong St. Washington, DC 20024", tolerance, false));
        builder.add(new TestInput("321 Fake St. Washington, DC 20024", tolerance, true));
        builder.add(new TestInput("123 Faky St. Washington, DC 20024", tolerance, true));
        builder.add(new TestInput("123 Fake St Washington, DC 20024", tolerance, true));
        builder.add(new TestInput("123 Fake St. Washington, DC 20023", tolerance, true));
        builder.add(new TestInput("124 Fake St. Washington, DC 20024", tolerance, true));
        builder.add(new TestInput("98765 Dummy Rd. Madeuptown, ZZ 99999", tolerance, false));
        builder.add(new TestInput("123 Fake St. Washington, DC 20024", tolerance, true)); // Equals value

        // Tolerance 0.0%
        tolerance = new Tolerance(0.00, ToleranceType.PERCENTAGE);
        builder.add(new TestInput("123 Wrong St. Washington, DC 20024", tolerance, false));
        builder.add(new TestInput("321 Fake St. Washington, DC 20024", tolerance, false));
        builder.add(new TestInput("123 Faky St. Washington, DC 20024", tolerance, false));
        builder.add(new TestInput("123 Fake St Washington, DC 20024", tolerance, false));
        builder.add(new TestInput("123 Fake St. Washington, DC 20023", tolerance, false));
        builder.add(new TestInput("124 Fake St. Washington, DC 20024", tolerance, false));
        builder.add(new TestInput("98765 Dummy Rd. Madeuptown, ZZ 99999", tolerance, false));
        builder.add(new TestInput("123 Fake St. Washington, DC 20024", tolerance, true)); // Equals value
        // Tolerance 5.0%
        tolerance = new Tolerance(0.05, ToleranceType.PERCENTAGE);
        builder.add(new TestInput("123 Wrong St. Washington, DC 20024", tolerance, false));
        builder.add(new TestInput("321 Fake St. Washington, DC 20024", tolerance, false));
        builder.add(new TestInput("123 Faky St. Washington, DC 20024", tolerance, true));
        builder.add(new TestInput("123 Fake St Washington, DC 20024", tolerance, true));
        builder.add(new TestInput("123 Fake St. Washington, DC 20023", tolerance, true));
        builder.add(new TestInput("124 Fake St. Washington, DC 20024", tolerance, true));
        builder.add(new TestInput("98765 Dummy Rd. Madeuptown, ZZ 99999", tolerance, false));
        builder.add(new TestInput("123 Fake St. Washington, DC 20024", tolerance, true)); // Equals value
        // Tolerance 100.0%
        tolerance = new Tolerance(1.00, ToleranceType.PERCENTAGE);
        builder.add(new TestInput("123 Wrong St. Washington, DC 20024", tolerance, true));
        builder.add(new TestInput("321 Fake St. Washington, DC 20024", tolerance, true));
        builder.add(new TestInput("123 Faky St. Washington, DC 20024", tolerance, true));
        builder.add(new TestInput("123 Fake St Washington, DC 20024", tolerance, true));
        builder.add(new TestInput("123 Fake St. Washington, DC 20023", tolerance, true));
        builder.add(new TestInput("124 Fake St. Washington, DC 20024", tolerance, true));
        builder.add(new TestInput("98765 Dummy Rd. Madeuptown, ZZ 99999", tolerance, true));
        builder.add(new TestInput("123 Fake St. Washington, DC 20024", tolerance, true)); // Equals value

        final ImmutableList<TestInput> testInputs = builder.build();

        testProperty(testInputs, PERSON_TYPE_URI, HAS_ADDRESS);
    }

    @Test
    public void testUriProperty() throws SmartUriException {
        System.out.println("URI Property Test");
        final ImmutableList.Builder<TestInput> builder = ImmutableList.builder();
        // Tolerance 0.0
        Tolerance tolerance = new Tolerance(0.0, ToleranceType.DIFFERENCE);
        builder.add(new TestInput(VF.createIRI("mailto:bob.smitch01@gmail.com"), tolerance, false));
        builder.add(new TestInput(VF.createIRI("mailto:bobsmitch00@gmail.com"), tolerance, false));
        builder.add(new TestInput(VF.createIRI("mailto:bob.smitch00@yahoo.com"), tolerance, false));
        builder.add(new TestInput(VF.createIRI("mailto:bob.smitch00@hotmail.com"), tolerance, false));
        builder.add(new TestInput(VF.createIRI("mailto:susan.smitch00@gmail.com"), tolerance, false));
        builder.add(new TestInput(VF.createIRI("mailto:ron.smitch00@gmail.com"), tolerance, false));
        builder.add(new TestInput(VF.createIRI("mailto:bob.smitch00@gmail.org"), tolerance, false));
        builder.add(new TestInput(VF.createIRI("mailto:susan.dillon@yahoo.org"), tolerance, false));
        builder.add(new TestInput(VF.createIRI("mailto:bob.smitch00@gmail.com"), tolerance, true)); // Equals value
        // Tolerance 1.0
        tolerance = new Tolerance(1.0, ToleranceType.DIFFERENCE);
        builder.add(new TestInput(VF.createIRI("mailto:bob.smitch01@gmail.com"), tolerance, true));
        builder.add(new TestInput(VF.createIRI("mailto:bobsmitch00@gmail.com"), tolerance, true));
        builder.add(new TestInput(VF.createIRI("mailto:bob.smitch00@yahoo.com"), tolerance, false));
        builder.add(new TestInput(VF.createIRI("mailto:bob.smitch00@hotmail.com"), tolerance, false));
        builder.add(new TestInput(VF.createIRI("mailto:susan.smitch00@gmail.com"), tolerance, false));
        builder.add(new TestInput(VF.createIRI("mailto:ron.smitch00@gmail.com"), tolerance, false));
        builder.add(new TestInput(VF.createIRI("mailto:bob.smitch00@gmail.org"), tolerance, false));
        builder.add(new TestInput(VF.createIRI("mailto:susan.dillon@yahoo.org"), tolerance, false));
        builder.add(new TestInput(VF.createIRI("mailto:bob.smitch00@gmail.com"), tolerance, true)); // Equals value
        // Tolerance 2.0
        tolerance = new Tolerance(2.0, ToleranceType.DIFFERENCE);
        builder.add(new TestInput(VF.createIRI("mailto:bob.smitch01@gmail.com"), tolerance, true));
        builder.add(new TestInput(VF.createIRI("mailto:bobsmitch00@gmail.com"), tolerance, true));
        builder.add(new TestInput(VF.createIRI("mailto:bob.smitch00@yahoo.com"), tolerance, false));
        builder.add(new TestInput(VF.createIRI("mailto:bob.smitch00@hotmail.com"), tolerance, false));
        builder.add(new TestInput(VF.createIRI("mailto:susan.smitch00@gmail.com"), tolerance, false));
        builder.add(new TestInput(VF.createIRI("mailto:ron.smitch00@gmail.com"), tolerance, true));
        builder.add(new TestInput(VF.createIRI("mailto:bob.smitch00@gmail.org"), tolerance, false));
        builder.add(new TestInput(VF.createIRI("mailto:susan.dillon@yahoo.org"), tolerance, false));
        builder.add(new TestInput(VF.createIRI("mailto:bob.smitch00@gmail.com"), tolerance, true)); // Equals value

        // Tolerance 0.0%
        tolerance = new Tolerance(0.00, ToleranceType.PERCENTAGE);
        builder.add(new TestInput(VF.createIRI("mailto:bob.smitch01@gmail.com"), tolerance, false));
        builder.add(new TestInput(VF.createIRI("mailto:bobsmitch00@gmail.com"), tolerance, false));
        builder.add(new TestInput(VF.createIRI("mailto:bob.smitch00@yahoo.com"), tolerance, false));
        builder.add(new TestInput(VF.createIRI("mailto:bob.smitch00@hotmail.com"), tolerance, false));
        builder.add(new TestInput(VF.createIRI("mailto:susan.smitch00@gmail.com"), tolerance, false));
        builder.add(new TestInput(VF.createIRI("mailto:ron.smitch00@gmail.com"), tolerance, false));
        builder.add(new TestInput(VF.createIRI("mailto:bob.smitch00@gmail.org"), tolerance, false));
        builder.add(new TestInput(VF.createIRI("mailto:susan.dillon@yahoo.org"), tolerance, false));
        builder.add(new TestInput(VF.createIRI("mailto:bob.smitch00@gmail.com"), tolerance, true)); // Equals value
        // Tolerance 5.0%
        tolerance = new Tolerance(0.05, ToleranceType.PERCENTAGE);
        builder.add(new TestInput(VF.createIRI("mailto:bob.smitch01@gmail.com"), tolerance, true));
        builder.add(new TestInput(VF.createIRI("mailto:bobsmitch00@gmail.com"), tolerance, true));
        builder.add(new TestInput(VF.createIRI("mailto:bob.smitch00@yahoo.com"), tolerance, false));
        builder.add(new TestInput(VF.createIRI("mailto:bob.smitch00@hotmail.com"), tolerance, false));
        builder.add(new TestInput(VF.createIRI("mailto:susan.smitch00@gmail.com"), tolerance, false));
        builder.add(new TestInput(VF.createIRI("mailto:ron.smitch00@gmail.com"), tolerance, false));
        builder.add(new TestInput(VF.createIRI("mailto:bob.smitch00@gmail.org"), tolerance, false));
        builder.add(new TestInput(VF.createIRI("mailto:susan.dillon@yahoo.org"), tolerance, false));
        builder.add(new TestInput(VF.createIRI("mailto:bob.smitch00@gmail.com"), tolerance, true)); // Equals value
        // Tolerance 100.0%
        tolerance = new Tolerance(1.00, ToleranceType.PERCENTAGE);
        builder.add(new TestInput(VF.createIRI("mailto:bob.smitch01@gmail.com"), tolerance, true));
        builder.add(new TestInput(VF.createIRI("mailto:bobsmitch00@gmail.com"), tolerance, true));
        builder.add(new TestInput(VF.createIRI("mailto:bob.smitch00@yahoo.com"), tolerance, true));
        builder.add(new TestInput(VF.createIRI("mailto:bob.smitch00@hotmail.com"), tolerance, true));
        builder.add(new TestInput(VF.createIRI("mailto:susan.smitch00@gmail.com"), tolerance, true));
        builder.add(new TestInput(VF.createIRI("mailto:ron.smitch00@gmail.com"), tolerance, true));
        builder.add(new TestInput(VF.createIRI("mailto:bob.smitch00@gmail.org"), tolerance, true));
        builder.add(new TestInput(VF.createIRI("mailto:susan.dillon@yahoo.org"), tolerance, true));
        builder.add(new TestInput(VF.createIRI("mailto:bob.smitch00@gmail.com"), tolerance, true)); // Equals value

        final ImmutableList<TestInput> testInputs = builder.build();

        testProperty(testInputs, PERSON_TYPE_URI, HAS_EMAIL_ADDRESS);
    }

    @Test
    public void testEquivalentTermsMap() throws SmartUriException {
        System.out.println("Equivalent Terms Map Test");

        final Map<String, List<String>> equivalentTermsMap = new LinkedHashMap<>();
        equivalentTermsMap.put("blue",
            Lists.newArrayList(
                // Color equivalents
                "cyan",
                "indigo",
                "peacock",
                "spruce",
                "denim",
                "slate",
                "cobalt",
                "azure",
                "stone",
                "admiral",
                "sky",
                "teal",
                "cerulean",
                "aegean",
                "sapphire",
                "navy",
                "ocean",
                "lapis",
                "berry",
                "arctic",
                // Mood equivalents
                "depressed",
                "down",
                "sad",
                "unhappy",
                "melancholy",
                "miserable",
                "gloomy",
                "dejected",
                "dispirited",
                "downhearted",
                "downcast",
                "despondent",
                "low",
                "glum"
            )
        );
        // Make this term bi-directional with "blue"
        equivalentTermsMap.put("sad",
            Lists.newArrayList(
                "blue"
            )
        );
        // Make this term bi-directional with "blue"
        equivalentTermsMap.put("navy",
            Lists.newArrayList(
                "blue"
            )
        );
        // This is a one-way equivalent term. Not defined in "blue"'s list of
        // equivalents but defined here.
        equivalentTermsMap.put("puritanical",
            Lists.newArrayList(
                "blue"
            )
        );
        final ImmutableList.Builder<TestInput> builder = ImmutableList.builder();
        // Tolerance 1.0 - tolerance doesn't apply to equivalents but is still needed for the test
        final Tolerance tolerance = new Tolerance(1.0, ToleranceType.DIFFERENCE);
        // Color equivalents
        builder.add(new TestInput("blue", tolerance, true)); // Equals value
        builder.add(new TestInput("Blue", tolerance, true));
        builder.add(new TestInput("BLUE", tolerance, true));
        builder.add(new TestInput("cyan", tolerance, true));
        builder.add(new TestInput("indigo", tolerance, true));
        builder.add(new TestInput("peacock", tolerance, true));
        builder.add(new TestInput("spruce", tolerance, true));
        builder.add(new TestInput("denim", tolerance, true));
        builder.add(new TestInput("slate", tolerance, true));
        builder.add(new TestInput("cobalt", tolerance, true));
        builder.add(new TestInput("azure", tolerance, true));
        builder.add(new TestInput("stone", tolerance, true));
        builder.add(new TestInput("admiral", tolerance, true));
        builder.add(new TestInput("sky", tolerance, true));
        builder.add(new TestInput("teal", tolerance, true));
        builder.add(new TestInput("cerulean", tolerance, true));
        builder.add(new TestInput("aegean", tolerance, true));
        builder.add(new TestInput("sapphire", tolerance, true));
        builder.add(new TestInput("navy", tolerance, true));
        builder.add(new TestInput("ocean", tolerance, true));
        builder.add(new TestInput("lapis", tolerance, true));
        builder.add(new TestInput("berry", tolerance, true));
        builder.add(new TestInput("arctic", tolerance, true));
        // Mood equivalents
        builder.add(new TestInput("depressed", tolerance, true));
        builder.add(new TestInput("down", tolerance, true));
        builder.add(new TestInput("sad", tolerance, true));
        builder.add(new TestInput("unhappy", tolerance, true));
        builder.add(new TestInput("melancholy", tolerance, true));
        builder.add(new TestInput("miserable", tolerance, true));
        builder.add(new TestInput("gloomy", tolerance, true));
        builder.add(new TestInput("dejected", tolerance, true));
        builder.add(new TestInput("dispirited", tolerance, true));
        builder.add(new TestInput("downhearted", tolerance, true));
        builder.add(new TestInput("downcast", tolerance, true));
        builder.add(new TestInput("despondent", tolerance, true));
        builder.add(new TestInput("low", tolerance, true));
        builder.add(new TestInput("glum", tolerance, true));
        // Color nonequivalents
        builder.add(new TestInput("white", tolerance, false));
        builder.add(new TestInput("tan", tolerance, false));
        builder.add(new TestInput("yellow", tolerance, false));
        builder.add(new TestInput("orange", tolerance, false));
        builder.add(new TestInput("red", tolerance, false));
        builder.add(new TestInput("pink", tolerance, false));
        builder.add(new TestInput("purple", tolerance, false));
        builder.add(new TestInput("green", tolerance, false));
        builder.add(new TestInput("brown", tolerance, false));
        builder.add(new TestInput("grey", tolerance, false));
        builder.add(new TestInput("black", tolerance, false));
        // Mood nonequivalents
        builder.add(new TestInput("happy", tolerance, false));
        builder.add(new TestInput("cheerful", tolerance, false));
        builder.add(new TestInput("gleeful", tolerance, false));
        builder.add(new TestInput("merry", tolerance, false));
        builder.add(new TestInput("delighted", tolerance, false));
        // Equivalents but not mapped as such
        builder.add(new TestInput("aquamarine", tolerance, false));
        builder.add(new TestInput("ultramarine", tolerance, false));
        builder.add(new TestInput("mournful", tolerance, false));
        builder.add(new TestInput("sorrowful", tolerance, false));
        // Nonequivalent - not bi-directional
        builder.add(new TestInput("puritanical", tolerance, false));

        final ImmutableList<TestInput> testInputs = builder.build();


        testProperty(testInputs, PERSON_TYPE_URI, HAS_EYE_COLOR, equivalentTermsMap);
    }

    @Test
    public void testCreateEntityNearDuplicate() throws EntityStorageException, TypeStorageException, ObjectStorageException {
        // Create the types the Entity uses.
        final TypeStorage typeStorage = new MongoTypeStorage(super.getMongoClient(), RYA_INSTANCE_NAME);
        final Type personType = createPersonType();
        final Type employeeType = createEmployeeType();
        typeStorage.create(personType);
        typeStorage.create(employeeType);
        final Optional<Type> storedPersonType = typeStorage.get(personType.getId());
        final Optional<Type> storedEmployeeType = typeStorage.get(employeeType.getId());

        assertTrue(storedPersonType.isPresent());
        assertTrue(storedEmployeeType.isPresent());

        // Create it.
        final DuplicateDataConfig duplicateDataConfig = new DuplicateDataConfig(
            new Tolerance(0.0, ToleranceType.DIFFERENCE), // boolean
            new Tolerance(0.0, ToleranceType.DIFFERENCE), // byte
            new Tolerance(500.0, ToleranceType.DIFFERENCE), // date
            new Tolerance(0.0001, ToleranceType.PERCENTAGE), // double
            new Tolerance(0.0001, ToleranceType.PERCENTAGE), // float
            new Tolerance(1.0, ToleranceType.DIFFERENCE), // integer
            new Tolerance(1.0, ToleranceType.DIFFERENCE), // long
            new Tolerance(1.0, ToleranceType.DIFFERENCE), // short
            new Tolerance(1.0, ToleranceType.DIFFERENCE), // string
            new Tolerance(1.0, ToleranceType.DIFFERENCE), // uri
            new HashMap<String, List<String>>(),
            true);

        final DuplicateDataDetector duplicateDataDetector = new DuplicateDataDetector(duplicateDataConfig);
        final EntityStorage entityStorage = new MongoEntityStorage(super.getMongoClient(), RYA_INSTANCE_NAME, duplicateDataDetector);
        final Entity bobEntity = createBobEntity();
        entityStorage.create(bobEntity);

        assertTrue(entityStorage.get(bobEntity.getSubject()).isPresent());

        final Builder duplicateBobBuilder = Entity.builder(createBobEntity());
        duplicateBobBuilder.setSubject(createRyaUri("Robert"));
        // Modify a property for each type that is within tolerance
        duplicateBobBuilder.setProperty(PERSON_TYPE_URI, new Property(HAS_AGE, shortRyaType((short) 41)));
        duplicateBobBuilder.setProperty(PERSON_TYPE_URI, new Property(HAS_WEIGHT, floatRyaType(250.76f)));
        duplicateBobBuilder.setProperty(PERSON_TYPE_URI, new Property(HAS_HEIGHT, doubleRyaType(72.499)));
        duplicateBobBuilder.setProperty(PERSON_TYPE_URI, new Property(HAS_INCOME, intRyaType(50001)));
        duplicateBobBuilder.setProperty(PERSON_TYPE_URI, new Property(HAS_NUMBER_OF_CHILDREN, byteRyaType((byte) 2)));
        duplicateBobBuilder.setProperty(PERSON_TYPE_URI, new Property(HAS_LICENSE_NUMBER, longRyaType(123456789013L)));
        duplicateBobBuilder.setProperty(PERSON_TYPE_URI, new Property(HAS_DATE_OF_BIRTH, dateRyaType(new DateTime(NOW.getTime() - 1).minusYears(40))));
        duplicateBobBuilder.setProperty(PERSON_TYPE_URI, new Property(HAS_EXPIRATION_DATE, dateRyaType(new Date(NOW.getTime() - 1))));
        duplicateBobBuilder.setProperty(PERSON_TYPE_URI, new Property(HAS_GLASSES, booleanRyaType(true)));
        duplicateBobBuilder.setProperty(PERSON_TYPE_URI, new Property(HAS_EMAIL_ADDRESS, uriRyaType(VF.createIRI("mailto:bob.smitch01@gmail.com"))));
        duplicateBobBuilder.setProperty(PERSON_TYPE_URI, new Property(HAS_ADDRESS, stringRyaType("124 Fake St. Washington, DC 20024")));
        duplicateBobBuilder.setProperty(EMPLOYEE_TYPE_URI, new Property(HAS_EXTENSION, shortRyaType((short) 556)));
        final Entity duplicateBobEntity = duplicateBobBuilder.build();

        // Try to create another entity that's considered a duplicate.
        // It will NOT be be created.
        boolean hasDuplicate = false;
        try {
            entityStorage.create(duplicateBobEntity);
        } catch(final EntityNearDuplicateException e) {
            hasDuplicate = true;
        }
        assertTrue(hasDuplicate);
        assertFalse(entityStorage.get(duplicateBobEntity.getSubject()).isPresent());

        final Builder notDuplicateBobBuilder = Entity.builder(createBobEntity());
        notDuplicateBobBuilder.setSubject(createRyaUri("Not Bob"));
        // Modify a property for each type that is within tolerance
        notDuplicateBobBuilder.setProperty(PERSON_TYPE_URI, new Property(HAS_AGE, shortRyaType((short) 50)));
        notDuplicateBobBuilder.setProperty(PERSON_TYPE_URI, new Property(HAS_WEIGHT, floatRyaType(300.0f)));
        notDuplicateBobBuilder.setProperty(PERSON_TYPE_URI, new Property(HAS_HEIGHT, doubleRyaType(100.0)));
        notDuplicateBobBuilder.setProperty(PERSON_TYPE_URI, new Property(HAS_INCOME, intRyaType(60000)));
        notDuplicateBobBuilder.setProperty(PERSON_TYPE_URI, new Property(HAS_NUMBER_OF_CHILDREN, byteRyaType((byte) 5)));
        notDuplicateBobBuilder.setProperty(PERSON_TYPE_URI, new Property(HAS_LICENSE_NUMBER, longRyaType(9L)));
        notDuplicateBobBuilder.setProperty(PERSON_TYPE_URI, new Property(HAS_DATE_OF_BIRTH, dateRyaType(new DateTime(NOW.getTime() - 10000000L).minusYears(40))));
        notDuplicateBobBuilder.setProperty(PERSON_TYPE_URI, new Property(HAS_EXPIRATION_DATE, dateRyaType(new Date(NOW.getTime() - 10000000L))));
        notDuplicateBobBuilder.setProperty(PERSON_TYPE_URI, new Property(HAS_GLASSES, booleanRyaType(false)));
        notDuplicateBobBuilder.setProperty(PERSON_TYPE_URI, new Property(HAS_EMAIL_ADDRESS, uriRyaType(VF.createIRI("mailto:bad.email.address@gmail.com"))));
        notDuplicateBobBuilder.setProperty(PERSON_TYPE_URI, new Property(HAS_ADDRESS, stringRyaType("123456789 Fake St. Washington, DC 20024")));
        notDuplicateBobBuilder.setProperty(EMPLOYEE_TYPE_URI, new Property(HAS_EXTENSION, shortRyaType((short) 1000)));
        final Entity notDuplicateBobEntity = notDuplicateBobBuilder.build();

        // Try to create another entity that's NOT considered a duplicate.
        // It will be created.
        try {
            entityStorage.create(notDuplicateBobEntity);
        } catch(final EntityNearDuplicateException e) {
            fail();
        }
        assertTrue(entityStorage.get(notDuplicateBobEntity.getSubject()).isPresent());
    }

    @Test
    public void testCreateEntityNearDuplicateConfigDisabled() throws EntityStorageException, TypeStorageException, ConfigurationException, ObjectStorageException {
        // Create the types the Entity uses.
        final TypeStorage typeStorage = new MongoTypeStorage(super.getMongoClient(), RYA_INSTANCE_NAME);
        final Type personType = createPersonType();
        final Type employeeType = createEmployeeType();
        typeStorage.create(personType);
        typeStorage.create(employeeType);
        final Optional<Type> storedPersonType = typeStorage.get(personType.getId());
        final Optional<Type> storedEmployeeType = typeStorage.get(employeeType.getId());

        assertTrue(storedPersonType.isPresent());
        assertTrue(storedEmployeeType.isPresent());

        // Create it.
        final DuplicateDataConfig duplicateDataConfig = new DuplicateDataConfig(
            new Tolerance(0.0, ToleranceType.DIFFERENCE), // boolean
            new Tolerance(0.0, ToleranceType.DIFFERENCE), // byte
            new Tolerance(500.0, ToleranceType.DIFFERENCE), // date
            new Tolerance(0.0001, ToleranceType.PERCENTAGE), // double
            new Tolerance(0.0001, ToleranceType.PERCENTAGE), // float
            new Tolerance(1.0, ToleranceType.DIFFERENCE), // integer
            new Tolerance(1.0, ToleranceType.DIFFERENCE), // long
            new Tolerance(1.0, ToleranceType.DIFFERENCE), // short
            new Tolerance(1.0, ToleranceType.DIFFERENCE), // string
            new Tolerance(1.0, ToleranceType.DIFFERENCE), // uri
            new HashMap<String, List<String>>(),
            false);

        final DuplicateDataDetector duplicateDataDetector = new DuplicateDataDetector(duplicateDataConfig);
        final EntityStorage entityStorage = new MongoEntityStorage(super.getMongoClient(), RYA_INSTANCE_NAME, duplicateDataDetector);
        final Entity bobEntity = createBobEntity();
        entityStorage.create(bobEntity);

        assertTrue(entityStorage.get(bobEntity.getSubject()).isPresent());

        final Builder duplicateBobBuilder = Entity.builder(createBobEntity());
        duplicateBobBuilder.setSubject(createRyaUri("Robert"));
        // Modify a property for each type that is within tolerance
        duplicateBobBuilder.setProperty(PERSON_TYPE_URI, new Property(HAS_AGE, shortRyaType((short) 41)));
        duplicateBobBuilder.setProperty(PERSON_TYPE_URI, new Property(HAS_WEIGHT, floatRyaType(250.76f)));
        duplicateBobBuilder.setProperty(PERSON_TYPE_URI, new Property(HAS_HEIGHT, doubleRyaType(72.499)));
        duplicateBobBuilder.setProperty(PERSON_TYPE_URI, new Property(HAS_INCOME, intRyaType(50001)));
        duplicateBobBuilder.setProperty(PERSON_TYPE_URI, new Property(HAS_NUMBER_OF_CHILDREN, byteRyaType((byte) 2)));
        duplicateBobBuilder.setProperty(PERSON_TYPE_URI, new Property(HAS_LICENSE_NUMBER, longRyaType(123456789013L)));
        duplicateBobBuilder.setProperty(PERSON_TYPE_URI, new Property(HAS_DATE_OF_BIRTH, dateRyaType(new DateTime(NOW.getTime() - 1).minusYears(40))));
        duplicateBobBuilder.setProperty(PERSON_TYPE_URI, new Property(HAS_EXPIRATION_DATE, dateRyaType(new Date(NOW.getTime() - 1))));
        duplicateBobBuilder.setProperty(PERSON_TYPE_URI, new Property(HAS_GLASSES, booleanRyaType(true)));
        duplicateBobBuilder.setProperty(PERSON_TYPE_URI, new Property(HAS_EMAIL_ADDRESS, uriRyaType(VF.createIRI("mailto:bob.smitch01@gmail.com"))));
        duplicateBobBuilder.setProperty(PERSON_TYPE_URI, new Property(HAS_ADDRESS, stringRyaType("124 Fake St. Washington, DC 20024")));
        duplicateBobBuilder.setProperty(EMPLOYEE_TYPE_URI, new Property(HAS_EXTENSION, shortRyaType((short) 556)));
        final Entity duplicateBobEntity = duplicateBobBuilder.build();

        // Try to create another entity that's considered a duplicate.
        // Data duplication detection is disabled so it will be created.
        try {
            entityStorage.create(duplicateBobEntity);
        } catch(final EntityNearDuplicateException e) {
            fail();
        }
        assertTrue(entityStorage.get(duplicateBobEntity.getSubject()).isPresent());

        final Builder notDuplicateBobBuilder = Entity.builder(createBobEntity());
        notDuplicateBobBuilder.setSubject(createRyaUri("Not Bob"));
        // Modify a property for each type that is within tolerance
        notDuplicateBobBuilder.setProperty(PERSON_TYPE_URI, new Property(HAS_AGE, shortRyaType((short) 50)));
        notDuplicateBobBuilder.setProperty(PERSON_TYPE_URI, new Property(HAS_WEIGHT, floatRyaType(300.0f)));
        notDuplicateBobBuilder.setProperty(PERSON_TYPE_URI, new Property(HAS_HEIGHT, doubleRyaType(100.0)));
        notDuplicateBobBuilder.setProperty(PERSON_TYPE_URI, new Property(HAS_INCOME, intRyaType(60000)));
        notDuplicateBobBuilder.setProperty(PERSON_TYPE_URI, new Property(HAS_NUMBER_OF_CHILDREN, byteRyaType((byte) 5)));
        notDuplicateBobBuilder.setProperty(PERSON_TYPE_URI, new Property(HAS_LICENSE_NUMBER, longRyaType(9L)));
        notDuplicateBobBuilder.setProperty(PERSON_TYPE_URI, new Property(HAS_DATE_OF_BIRTH, dateRyaType(new DateTime(NOW.getTime() - 10000000L).minusYears(40))));
        notDuplicateBobBuilder.setProperty(PERSON_TYPE_URI, new Property(HAS_EXPIRATION_DATE, dateRyaType(new Date(NOW.getTime() - 10000000L))));
        notDuplicateBobBuilder.setProperty(PERSON_TYPE_URI, new Property(HAS_GLASSES, booleanRyaType(false)));
        notDuplicateBobBuilder.setProperty(PERSON_TYPE_URI, new Property(HAS_EMAIL_ADDRESS, uriRyaType(VF.createIRI("mailto:bad.email.address@gmail.com"))));
        notDuplicateBobBuilder.setProperty(PERSON_TYPE_URI, new Property(HAS_ADDRESS, stringRyaType("123456789 Fake St. Washington, DC 20024")));
        notDuplicateBobBuilder.setProperty(EMPLOYEE_TYPE_URI, new Property(HAS_EXTENSION, shortRyaType((short) 1000)));
        final Entity notDuplicateBobEntity = notDuplicateBobBuilder.build();

        // Try to create another entity that's NOT considered a duplicate.
        // Data duplication detection is disabled so it will be created.
        try {
            entityStorage.create(notDuplicateBobEntity);
        } catch(final EntityNearDuplicateException e) {
            fail();
        }
        assertTrue(entityStorage.get(notDuplicateBobEntity.getSubject()).isPresent());
    }

    /**
     * Creates two entities to test where one property is different from the
     * other.  Multiple different values for the property are provided by
     * {@code testInputs}.
     * @param testInputs the {@link List} of {@link TestInput} to insert into
     * the property.
     * @param typeIdUri the type ID {@link RyaURI} that the property falls
     * under. (not {@code null})
     * @param propertyNameUri the property name {@link RyaURI}.
     * (not {@code null})
     * @throws SmartUriException
     */
    private static void testProperty(final List<TestInput> testInputs, final RyaURI typeIdUri, final RyaURI propertyNameUri) throws SmartUriException {
        testProperty(testInputs, typeIdUri, propertyNameUri, new LinkedHashMap<>());
    }

    /**
     * Creates two entities to test where one property is different from the
     * other.  Multiple different values for the property are provided by
     * {@code testInputs}.
     * @param testInputs the {@link List} of {@link TestInput} to insert into
     * the property.
     * @param typeIdUri the type ID {@link RyaURI} that the property falls
     * under. (not {@code null})
     * @param propertyNameUri the property name {@link RyaURI}.
     * (not {@code null})
     * @param equivalentTermsMap the {@link Map} of terms that are considered
     * equivalent to each other.
     * @throws SmartUriException
     */
    private static void testProperty(final List<TestInput> testInputs, final RyaURI typeIdUri, final RyaURI propertyNameUri, final Map<String, List<String>> equivalentTermsMap) throws SmartUriException {
        requireNonNull(typeIdUri);
        requireNonNull(propertyNameUri);

        final Entity entity1 = createBobEntity();

        int count = 0;
        for (final TestInput testInput : testInputs) {
            System.out.println("Test #" + count + ": " + testInput.toString());
            final Object testValue = testInput.getValue();
            final Tolerance tolerance = testInput.getTolerance();
            final boolean expected = testInput.getExpected();

            final Builder builder = new Builder(entity1);
            final RyaType ryaType = RyaTypeUtils.getRyaTypeForClass(testValue.getClass(), testValue);
            builder.setProperty(typeIdUri, new Property(propertyNameUri, ryaType));
            final Entity entity2 = builder.build();

            final DuplicateDataDetector duplicateDataDetector = new DuplicateDataDetector(tolerance, equivalentTermsMap);
            final boolean areDuplicates = duplicateDataDetector.compareEntities(entity1, entity2);

            final String originalValue = entity1.lookupTypeProperty(typeIdUri, propertyNameUri).get().getValue().getData();
            final String message = createErrorMessage(originalValue, testValue, expected, areDuplicates, tolerance);
            assertEquals(message, expected, areDuplicates);
            count++;
        }
    }

    /**
     * Represents the values/tolerances to use for a test along with the
     * expected result.
     */
    private static class TestInput {
        private final Object value;
        private final Tolerance tolerance;
        private final boolean expected;

        /**
         * Creates a new instance of {@link TestInput}.
         * @param value the value to test.
         * @param tolerance the {@link Tolerance} to use.
         * @param expected the expected value to be returned.
         */
        public TestInput(final Object value, final Tolerance tolerance, final boolean expected) {
            this.value = value;
            this.tolerance = tolerance;
            this.expected = expected;
        }

        /**
         * @return the value to test.
         */
        public Object getValue() {
            return value;
        }

        /**
         * @return the {@link Tolerance} to use.
         */
        public Tolerance getTolerance() {
            return tolerance;
        }

        /**
         * @return the expected value to be returned.
         */
        public boolean getExpected() {
            return expected;
        }

        @Override
        public String toString() {
            return ReflectionToStringBuilder.toString(this);
        }
    }
}