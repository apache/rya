package mvm.rya.mongodb.instance;

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

import mvm.rya.api.instance.RyaDetails;
import mvm.rya.api.instance.RyaDetails.EntityCentricIndexDetails;
import mvm.rya.api.instance.RyaDetails.FreeTextIndexDetails;
import mvm.rya.api.instance.RyaDetails.GeoIndexDetails;
import mvm.rya.api.instance.RyaDetails.JoinSelectivityDetails;
import mvm.rya.api.instance.RyaDetails.PCJIndexDetails;
import mvm.rya.api.instance.RyaDetails.PCJIndexDetails.FluoDetails;
import mvm.rya.api.instance.RyaDetails.PCJIndexDetails.PCJDetails;
import mvm.rya.api.instance.RyaDetails.PCJIndexDetails.PCJDetails.PCJUpdateStrategy;
import mvm.rya.api.instance.RyaDetails.ProspectorDetails;
import mvm.rya.api.instance.RyaDetails.TemporalIndexDetails;
import mvm.rya.mongodb.instance.MongoDetailsAdapter.MalformedRyaDetailsException;

public class MongoDetailsAdapterTest {
    @Test
    public void ryaDetailsToMongoTest() {
        final PCJIndexDetails.Builder pcjBuilder = PCJIndexDetails.builder()
        .setEnabled(true)
        .setFluoDetails(new FluoDetails("fluo"));
        for(int ii = 0; ii < 2; ii++) {
            pcjBuilder.addPCJDetails(
            PCJDetails.builder()
            .setId("pcj_"+ii)
            .setUpdateStrategy(PCJUpdateStrategy.BATCH)
            .setLastUpdateTime(new Date(ii))
            .build());
        }

        final RyaDetails details = RyaDetails.builder()
        .setRyaInstanceName("test")
        .setRyaVersion("1")
        .setEntityCentricIndexDetails(new EntityCentricIndexDetails(true))
        .setGeoIndexDetails(new GeoIndexDetails(true))
        .setPCJIndexDetails(pcjBuilder.build())
        .setTemporalIndexDetails(new TemporalIndexDetails(true))
        .setFreeTextDetails(new FreeTextIndexDetails(true))
        .setProspectorDetails(new ProspectorDetails(Optional.<Date>fromNullable(new Date(0L))))
        .setJoinSelectivityDetails(new JoinSelectivityDetails(Optional.<Date>fromNullable(new Date(1L))))
        .build();

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
        final BasicDBObject actual = MongoDetailsAdapter.toDBObject(details);
        System.out.println(expected.toString());
        System.out.println("***");
        System.out.println(actual.toString());
        assertEquals(expected.toString(), actual.toString());
    }

    @Test
    public void mongoToRyaDetailsTest() throws MalformedRyaDetailsException {
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
        final PCJIndexDetails.Builder pcjBuilder = PCJIndexDetails.builder()
        .setEnabled(true)
        .setFluoDetails(new FluoDetails("fluo"));
        for(int ii = 0; ii < 2; ii++) {
            pcjBuilder.addPCJDetails(
            PCJDetails.builder()
            .setId("pcj_"+ii)
            .setUpdateStrategy(PCJUpdateStrategy.BATCH)
            .setLastUpdateTime(new Date(ii))
            .build());
        }

        final RyaDetails expected = RyaDetails.builder()
        .setRyaInstanceName("test")
        .setRyaVersion("1")
        .setEntityCentricIndexDetails(new EntityCentricIndexDetails(true))
        .setGeoIndexDetails(new GeoIndexDetails(true))
        .setPCJIndexDetails(pcjBuilder.build())
        .setTemporalIndexDetails(new TemporalIndexDetails(true))
        .setFreeTextDetails(new FreeTextIndexDetails(true))
        .setProspectorDetails(new ProspectorDetails(Optional.<Date>fromNullable(new Date(0L))))
        .setJoinSelectivityDetails(new JoinSelectivityDetails(Optional.<Date>fromNullable(new Date(1L))))
        .build();

        final RyaDetails actual = MongoDetailsAdapter.toRyaDetails(mongo);
        assertEquals(expected, actual);
    }

    @Test
    public void absentOptionalToRyaDetailsTest() throws MalformedRyaDetailsException {
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
                +          "updateStrategy : \"INCREMENTAL\""
                +       "}"
                +    "]"
                + "},"
                + "temporalDetails : false,"
                + "freeTextDetails : true,"
                + "prospectorDetails : null,"
                + "joinSelectivitiyDetails : null"
              + "}"
            );
        final PCJIndexDetails.Builder pcjBuilder = PCJIndexDetails.builder()
        .setEnabled(false)
        .setFluoDetails(new FluoDetails("fluo"))
        .addPCJDetails(
            PCJDetails.builder()
            .setId("pcj_1")
            .setUpdateStrategy(PCJUpdateStrategy.INCREMENTAL)
            .setLastUpdateTime(null).build());

        final RyaDetails expected = RyaDetails.builder()
        .setRyaInstanceName("test")
        .setRyaVersion("1")
        .setEntityCentricIndexDetails(new EntityCentricIndexDetails(true))
        .setGeoIndexDetails(new GeoIndexDetails(false))
        .setPCJIndexDetails(pcjBuilder.build())
        .setTemporalIndexDetails(new TemporalIndexDetails(false))
        .setFreeTextDetails(new FreeTextIndexDetails(true))
        .setProspectorDetails(new ProspectorDetails(Optional.<Date>absent()))
        .setJoinSelectivityDetails(new JoinSelectivityDetails(Optional.<Date>absent()))
        .build();

        final RyaDetails actual = MongoDetailsAdapter.toRyaDetails(mongo);
        assertEquals(expected, actual);
    }

    @Test
    public void absentOptionalToMongoTest() {
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
        final PCJIndexDetails.Builder pcjBuilder = PCJIndexDetails.builder()
        .setEnabled(true)
        .setFluoDetails(new FluoDetails("fluo"));

        final RyaDetails details = RyaDetails.builder()
        .setRyaInstanceName("test")
        .setRyaVersion("1")
        .setEntityCentricIndexDetails(new EntityCentricIndexDetails(true))
        .setGeoIndexDetails(new GeoIndexDetails(false))
        .setPCJIndexDetails(pcjBuilder.build())
        .setTemporalIndexDetails(new TemporalIndexDetails(false))
        .setFreeTextDetails(new FreeTextIndexDetails(true))
        .setProspectorDetails(new ProspectorDetails(Optional.<Date>absent()))
        .setJoinSelectivityDetails(new JoinSelectivityDetails(Optional.<Date>absent()))
        .build();

        final DBObject actual = MongoDetailsAdapter.toDBObject(details);
        assertEquals(expected, actual);
    }
}