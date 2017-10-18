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
package org.apache.rya.indexing.mongo;

import java.net.URISyntaxException;
import java.util.*;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.rya.api.domain.RyaSchema;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.api.resolver.RyaToRdfConversions;
import org.apache.rya.indexing.entity.model.Entity;
import org.apache.rya.indexing.entity.model.Property;
import org.apache.rya.indexing.entity.model.Type;
import org.apache.rya.indexing.entity.model.TypedEntity;
import org.apache.rya.indexing.entity.query.EntityQueryNode;
import org.apache.rya.indexing.entity.storage.mongo.ConvertingCursor;
import org.apache.rya.indexing.mongodb.MongoDbSmartUri;
import org.apache.rya.indexing.smarturi.SmartUriAdapter;
import org.apache.rya.indexing.smarturi.SmartUriException;
import org.apache.rya.mongodb.MongoTestBase;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.impl.URIImpl;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.eclipse.rdf4j.query.algebra.helpers.StatementPatternCollector;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import static org.apache.rya.api.domain.RyaTypeUtils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for MongoDB based Smart URI.
 */
public class MongoDbSmartUriIT extends MongoTestBase {
    private static final String NAMESPACE = RyaSchema.NAMESPACE;
    private static final ValueFactory VALUE_FACTORY = SimpleValueFactory.getInstance();

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

    // Entities
    private static final Entity BOB_ENTITY = createBobEntity();

    // Types
    private static final Type PERSON_TYPE = createPersonType();
    private static final Type EMPLOYEE_TYPE = createEmployeeType();

    private static MongoDbSmartUri smartUriConverter;

    @Before
    public void setup() throws Exception {
        smartUriConverter = new MongoDbSmartUri(conf);
    }

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
        return RdfToRyaConversions.convertURI(VALUE_FACTORY.createIRI(namespace, localName));
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
            .setProperty(PERSON_TYPE_URI, new Property(HAS_DATE_OF_BIRTH, dateRyaType(new DateTime().minusYears(40))))
            .setProperty(PERSON_TYPE_URI, new Property(HAS_EXPIRATION_DATE, dateRyaType(new Date())))
            .setProperty(PERSON_TYPE_URI, new Property(HAS_GLASSES, booleanRyaType(true)))
            .setProperty(PERSON_TYPE_URI, new Property(HAS_EMAIL_ADDRESS, uriRyaType(new URIImpl("mailto:bob.smitch00@gmail.com"))))
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
         final Type personType = new Type(PERSON_TYPE_URI,
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
                .build());
         return personType;
    }

    private static Type createEmployeeType() {
        final Type employeeType = new Type(EMPLOYEE_TYPE_URI,
            ImmutableSet.<RyaURI>builder()
                .add(HAS_POSITION_TITLE)
                .add(HAS_WORK_ADDRESS)
                .add(HAS_EXTENSION)
                .add(HAS_OFFICE_ROOM_NUMBER)
                .build());
        return employeeType;
    }

    private static String getRyaUriLocalName(final RyaURI ryaUri) {
        return new URIImpl(ryaUri.getData()).getLocalName();
    }

    @Test
    public void testSerializeDeserialize() throws SmartUriException, URISyntaxException {
        final IRI smartUri = SmartUriAdapter.serializeUriEntity(BOB_ENTITY);
        final Entity resultEntity = SmartUriAdapter.deserializeUriEntity(smartUri);
        assertEquals(BOB_ENTITY.getSubject(), resultEntity.getSubject());
    }

    @Test
    public void testStorage() throws SmartUriException, RuntimeException {
        smartUriConverter.storeEntity(BOB_ENTITY);

        final String sparql = "SELECT * WHERE { " +
            "<" + BOB.getData() + "> <" + RDF.TYPE + "> <" + PERSON_TYPE.getId().getData() + "> . " +
            "<" + BOB.getData() + "> <" + HAS_SSN.getData() + "> ?ssn . " +
            "<" + BOB.getData() + "> <" + HAS_AGE.getData() + "> ?age . " +
            "<" + BOB.getData() + "> <" + HAS_WEIGHT.getData() + "> ?weight . " +
            "<" + BOB.getData() + "> <" + HAS_ADDRESS.getData() + "> ?address . " +
        "}";

        final StatementPatternCollector spCollector = new StatementPatternCollector();
        new SPARQLParser().parseQuery(sparql, null).getTupleExpr().visit(spCollector);
        final List<StatementPattern> patterns = spCollector.getStatementPatterns();
        final EntityQueryNode entityQueryNode = new EntityQueryNode(PERSON_TYPE, patterns, smartUriConverter.getEntityStorage());
        final QueryBindingSet queryBindingSet = new QueryBindingSet();
        final Property ssnProperty = BOB_ENTITY.lookupTypeProperty(PERSON_TYPE, HAS_SSN).get();
        queryBindingSet.addBinding(HAS_SSN.getData(), RyaToRdfConversions.convertValue(ssnProperty.getValue()));

        final CloseableIteration<BindingSet, QueryEvaluationException> iter = entityQueryNode.evaluate(queryBindingSet);
        int count = 0;
        // These should match what was used in the SPARQL query.
        final List<String> queryParamNames = Lists.newArrayList("ssn", "age", "weight", "address");
        while (iter.hasNext()) {
            final BindingSet bs = iter.next();
            assertTrue(bs.getBindingNames().containsAll(queryParamNames));
            count++;
        }
        assertEquals(count, 1);
    }

    @Test
    public void testUpdate() throws SmartUriException {
        smartUriConverter.storeEntity(BOB_ENTITY);

        // New properties to add
        final RyaURI hasNickName = createRyaUri("hasNickName");
        final RyaURI hasWindowOffice = createRyaUri("hasWindowOffice");

        final Entity.Builder builder = Entity.builder(BOB_ENTITY);
        builder.setProperty(PERSON_TYPE_URI, new Property(HAS_AGE, shortRyaType((short) 41)));
        builder.setProperty(PERSON_TYPE_URI, new Property(hasNickName, stringRyaType("Bobby")));
        builder.setProperty(EMPLOYEE_TYPE_URI, new Property(HAS_POSITION_TITLE, stringRyaType("Assistant Regional Manager")));
        builder.setProperty(EMPLOYEE_TYPE_URI, new Property(hasWindowOffice, booleanRyaType(true)));
        builder.setVersion(BOB_ENTITY.getVersion() + 1);
        builder.rebuildSmartUri();

        final Entity newBobEntity = builder.build();

        smartUriConverter.updateEntity(BOB_ENTITY, newBobEntity);

        final Entity resultEntity = smartUriConverter.queryEntity(BOB_ENTITY.getSubject());
        assertEquals(newBobEntity.getVersion(), resultEntity.getVersion());
        assertEquals(newBobEntity.lookupTypeProperty(PERSON_TYPE, HAS_AGE), resultEntity.lookupTypeProperty(PERSON_TYPE, HAS_AGE));
        assertEquals(newBobEntity.lookupTypeProperty(PERSON_TYPE, hasNickName), resultEntity.lookupTypeProperty(PERSON_TYPE, hasNickName));
        assertEquals(newBobEntity.lookupTypeProperty(EMPLOYEE_TYPE, HAS_POSITION_TITLE), resultEntity.lookupTypeProperty(EMPLOYEE_TYPE, HAS_POSITION_TITLE));
        assertEquals(newBobEntity.lookupTypeProperty(EMPLOYEE_TYPE, hasWindowOffice), resultEntity.lookupTypeProperty(EMPLOYEE_TYPE, hasWindowOffice));
        assertEquals(newBobEntity.getSmartUri(), resultEntity.getSmartUri());
        final String resultUriString = resultEntity.getSmartUri().stringValue();
        assertTrue(resultUriString.contains(getRyaUriLocalName(hasWindowOffice)));
        assertTrue(resultUriString.contains(getRyaUriLocalName(hasNickName)));
    }

    @Test
    public void testQuery() throws SmartUriException {
        smartUriConverter.storeEntity(BOB_ENTITY);

        // Look up Person Type Entities that match Bob's SSN property
        final Set<Property> properties = new LinkedHashSet<>();
        properties.add(BOB_ENTITY.lookupTypeProperty(PERSON_TYPE, HAS_SSN).get());
        final Map<IRI, Value> map = SmartUriAdapter.propertiesToMap(properties);

        final ConvertingCursor<TypedEntity> cursor = smartUriConverter.queryEntity(PERSON_TYPE, map);
        int count = 0;
        while (cursor.hasNext()) {
            final TypedEntity typedEntity = cursor.next();
            System.out.println(typedEntity);
            count++;
        }
        assertEquals(count, 1);
    }
}