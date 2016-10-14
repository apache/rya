package org.apache.rya.mongodb.instance;

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

import static org.junit.Assert.assertEquals;

import java.util.Date;

import org.junit.Test;

import com.google.common.base.Optional;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

import org.apache.rya.api.instance.RyaDetails;
import org.apache.rya.api.instance.RyaDetails.EntityCentricIndexDetails;
import org.apache.rya.api.instance.RyaDetails.FreeTextIndexDetails;
import org.apache.rya.api.instance.RyaDetails.GeoIndexDetails;
import org.apache.rya.api.instance.RyaDetails.JoinSelectivityDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails.FluoDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails.PCJDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails.PCJDetails.PCJUpdateStrategy;
import org.apache.rya.api.instance.RyaDetails.ProspectorDetails;
import org.apache.rya.api.instance.RyaDetails.TemporalIndexDetails;
import org.apache.rya.mongodb.instance.MongoDetailsAdapter.MalformedRyaDetailsException;

/**
 * Tests the methods of {@link MongoDetailsAdapter}.
 */
public class MongoDetailsAdapterTest {

    @Test
    public void ryaDetailsToMongoTest() {
        // Convert the Details into a Mongo DB OBject.
        final RyaDetails details = RyaDetails.builder()
            .setRyaInstanceName("test")
            .setRyaVersion("1")
            .setEntityCentricIndexDetails(new EntityCentricIndexDetails(true))
            .setGeoIndexDetails(new GeoIndexDetails(true))
            .setPCJIndexDetails(
                PCJIndexDetails.builder()
                .setEnabled(true)
                .setFluoDetails(new FluoDetails("fluo"))
                .addPCJDetails(
                        PCJDetails.builder()
                        .setId("pcj_0")
                        .setUpdateStrategy(PCJUpdateStrategy.BATCH)
                        .setLastUpdateTime(new Date(0L)))
                .addPCJDetails(
                        PCJDetails.builder()
                        .setId("pcj_1")
                        .setUpdateStrategy(PCJUpdateStrategy.BATCH)
                        .setLastUpdateTime(new Date(1L))))
            .setTemporalIndexDetails(new TemporalIndexDetails(true))
            .setFreeTextDetails(new FreeTextIndexDetails(true))
            .setProspectorDetails(new ProspectorDetails(Optional.fromNullable(new Date(0L))))
            .setJoinSelectivityDetails(new JoinSelectivityDetails(Optional.fromNullable(new Date(1L))))
            .build();

        final BasicDBObject actual = MongoDetailsAdapter.toDBObject(details);

        // Ensure it matches the expected object.
        final DBObject expected = (DBObject) JSON.parse(
            "{ "
            + "instanceName : \"test\","
            + "version : \"1\","
            + "entityCentricDetails : true,"
            + "geoDetails : true,"
            + "pcjDetails : {"
            +    "enabled : true ,"
            +    "fluoName : \"fluo\","
            +    "pcjs : [ "
            +       "{"
            +          "id : \"pcj_0\","
            +          "updateStrategy : \"BATCH\","
            +          "lastUpdate : { $date : \"1970-01-01T00:00:00.000Z\"}"
            +       "},"
            +       "{"
            +          "id : \"pcj_1\","
            +          "updateStrategy : \"BATCH\","
            +          "lastUpdate : { $date : \"1970-01-01T00:00:00.001Z\"}"
            +       "}]"
            + "},"
            + "temporalDetails : true,"
            + "freeTextDetails : true,"
            + "prospectorDetails : { $date : \"1970-01-01T00:00:00.000Z\"},"
            + "joinSelectivitiyDetails : { $date : \"1970-01-01T00:00:00.001Z\"}"
          + "}"
        );

        assertEquals(expected.toString(), actual.toString());
    }

    @Test
    public void mongoToRyaDetailsTest() throws MalformedRyaDetailsException {
        // Convert the Mongo object into a RyaDetails.
        final BasicDBObject mongo = (BasicDBObject) JSON.parse(
            "{ "
            + "instanceName : \"test\","
            + "version : \"1\","
            + "entityCentricDetails : true,"
            + "geoDetails : true,"
            + "pcjDetails : {"
            +    "enabled : true ,"
            +    "fluoName : \"fluo\","
            +    "pcjs : [ "
            +       "{"
            +          "id : \"pcj_0\","
            +          "updateStrategy : \"BATCH\","
            +          "lastUpdate : { $date : \"1970-01-01T00:00:00.000Z\"}"
            +       "},"
            +       "{"
            +          "id : \"pcj_1\","
            +          "updateStrategy : \"BATCH\","
            +          "lastUpdate : { $date : \"1970-01-01T00:00:00.001Z\"}"
            +       "}]"
            + "},"
            + "temporalDetails : true,"
            + "freeTextDetails : true,"
            + "prospectorDetails : { $date : \"1970-01-01T00:00:00.000Z\"},"
            + "joinSelectivitiyDetails : { $date : \"1970-01-01T00:00:00.001Z\"}"
          + "}"
        );

        final RyaDetails actual = MongoDetailsAdapter.toRyaDetails(mongo);

        // Ensure it matches the expected object.
        final RyaDetails expected = RyaDetails.builder()
            .setRyaInstanceName("test")
            .setRyaVersion("1")
            .setEntityCentricIndexDetails(new EntityCentricIndexDetails(true))
            .setGeoIndexDetails(new GeoIndexDetails(true))
            .setPCJIndexDetails(
                PCJIndexDetails.builder()
                    .setEnabled(true)
                    .setFluoDetails(new FluoDetails("fluo"))
                    .addPCJDetails(
                        PCJDetails.builder()
                            .setId("pcj_0")
                            .setUpdateStrategy(PCJUpdateStrategy.BATCH)
                            .setLastUpdateTime(new Date(0L)))
                    .addPCJDetails(
                            PCJDetails.builder()
                                .setId("pcj_1")
                                .setUpdateStrategy(PCJUpdateStrategy.BATCH)
                                .setLastUpdateTime(new Date(1L))))
            .setTemporalIndexDetails(new TemporalIndexDetails(true))
            .setFreeTextDetails(new FreeTextIndexDetails(true))
            .setProspectorDetails(new ProspectorDetails(Optional.<Date>fromNullable(new Date(0L))))
            .setJoinSelectivityDetails(new JoinSelectivityDetails(Optional.<Date>fromNullable(new Date(1L))))
            .build();

        assertEquals(expected, actual);
    }

    @Test
    public void absentOptionalToRyaDetailsTest() throws MalformedRyaDetailsException {
        // Convert the Mongo object into a RyaDetails.
        final BasicDBObject mongo = (BasicDBObject) JSON.parse(
                "{ "
                + "instanceName : \"test\","
                + "version : \"1\","
                + "entityCentricDetails : true,"
                + "geoDetails : false,"
                + "pcjDetails : {"
                +    "enabled : false,"
                +    "fluoName : \"fluo\","
                +    "pcjs : [ "
                +       "{"
                +          "id : \"pcj_1\","
                +       "}"
                +    "]"
                + "},"
                + "temporalDetails : false,"
                + "freeTextDetails : true,"
                + "prospectorDetails : null,"
                + "joinSelectivitiyDetails : null"
              + "}"
            );
        final RyaDetails actual = MongoDetailsAdapter.toRyaDetails(mongo);

        // Ensure it matches the expected object.
        final RyaDetails expected = RyaDetails.builder()
            .setRyaInstanceName("test")
            .setRyaVersion("1")
            .setEntityCentricIndexDetails(new EntityCentricIndexDetails(true))
            .setGeoIndexDetails(new GeoIndexDetails(false))
            .setPCJIndexDetails(
                    PCJIndexDetails.builder()
                    .setEnabled(false)
                    .setFluoDetails(new FluoDetails("fluo"))
                    .addPCJDetails(
                        PCJDetails.builder()
                            .setId("pcj_1")
                            .setLastUpdateTime(null)))
            .setTemporalIndexDetails(new TemporalIndexDetails(false))
            .setFreeTextDetails(new FreeTextIndexDetails(true))
            .setProspectorDetails(new ProspectorDetails(Optional.<Date>absent()))
            .setJoinSelectivityDetails(new JoinSelectivityDetails(Optional.<Date>absent()))
            .build();

        assertEquals(expected, actual);
    }

    @Test
    public void absentOptionalToMongoTest() {
        // Convert the Details into a Mongo DB OBject.
        final RyaDetails details = RyaDetails.builder()
            .setRyaInstanceName("test")
            .setRyaVersion("1")
            .setEntityCentricIndexDetails(new EntityCentricIndexDetails(true))
            .setGeoIndexDetails(new GeoIndexDetails(false))
            .setPCJIndexDetails(
                PCJIndexDetails.builder()
                    .setEnabled(true)
                    .setFluoDetails(new FluoDetails("fluo")))
            .setTemporalIndexDetails(new TemporalIndexDetails(false))
            .setFreeTextDetails(new FreeTextIndexDetails(true))
            .setProspectorDetails(new ProspectorDetails(Optional.<Date>absent()))
            .setJoinSelectivityDetails(new JoinSelectivityDetails(Optional.<Date>absent()))
            .build();

        final DBObject actual = MongoDetailsAdapter.toDBObject(details);

        // Ensure it matches the expected object.
        final BasicDBObject expected = (BasicDBObject) JSON.parse(
                "{ "
                + "instanceName : \"test\","
                + "version : \"1\","
                + "entityCentricDetails : true,"
                + "geoDetails : false,"
                + "pcjDetails : {"
                +    "enabled : true,"
                +    "fluoName : \"fluo\","
                +    "pcjs : [ ]"
                + "},"
                + "temporalDetails : false,"
                + "freeTextDetails : true"
              + "}"
            );
        assertEquals(expected, actual);
    }

    @Test
    public void toDBObject_pcjDetails() {
        final PCJDetails details = PCJDetails.builder()
                .setId("pcjId")
                .setLastUpdateTime( new Date() )
                .setUpdateStrategy( PCJUpdateStrategy.INCREMENTAL )
                .build();

        // Convert it into a Mongo DB Object.
        final BasicDBObject dbo = (BasicDBObject) MongoDetailsAdapter.toDBObject(details);

        // Convert the dbo back into the original object.
        final PCJDetails restored = MongoDetailsAdapter.toPCJDetails(dbo).build();

        // Ensure the restored value matches the original.
        assertEquals(details, restored);
    }

    @Test
    public void toDBObject_pcjDetails_missing_optionals() {
        final PCJDetails details = PCJDetails.builder()
                .setId("pcjId")
                .build();

        // Convert it into a Mongo DB Object.
        final BasicDBObject dbo = (BasicDBObject) MongoDetailsAdapter.toDBObject(details);

        // Convert the dbo back into the original object.
        final PCJDetails restored = MongoDetailsAdapter.toPCJDetails(dbo).build();

        // Ensure the restored value matches the original.
        assertEquals(details, restored);
    }
}