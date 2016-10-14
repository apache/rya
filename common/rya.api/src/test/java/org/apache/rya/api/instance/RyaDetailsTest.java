package org.apache.rya.api.instance;

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

/**
 * Tests the methods of {@link RyaDetails}.
 */
public class RyaDetailsTest {

    @Test
    public void equals() {
        final RyaDetails.Builder builder = RyaDetails.builder();

        builder.setRyaInstanceName("test_instance")
            .setRyaVersion("1.2.3.4")
            .setEntityCentricIndexDetails( new EntityCentricIndexDetails(true) )
            .setGeoIndexDetails( new GeoIndexDetails(true) )
            .setTemporalIndexDetails( new TemporalIndexDetails(true) )
            .setFreeTextDetails( new FreeTextIndexDetails(true) )
            .setPCJIndexDetails(
                    PCJIndexDetails.builder()
                        .setEnabled(true)
                        .setFluoDetails( new FluoDetails("test_instance_rya_pcj_updater") )
                        .addPCJDetails(
                                PCJDetails.builder()
                                    .setId("pcj 1")
                                    .setUpdateStrategy(PCJUpdateStrategy.BATCH)
                                    .setLastUpdateTime( new Date() ))
                        .addPCJDetails(
                                PCJDetails.builder()
                                    .setId("pcj 2")
                                    .setUpdateStrategy(PCJUpdateStrategy.INCREMENTAL)))
            .setProspectorDetails( new ProspectorDetails(Optional.of(new Date())) )
            .setJoinSelectivityDetails( new JoinSelectivityDetails(Optional.of(new Date())) );

        final RyaDetails details1 = builder.build();
        final RyaDetails details2 = builder.build();
        assertEquals(details1, details2);
    }

    @Test
    public void hashcode() {
        final RyaDetails.Builder builder = RyaDetails.builder();

        builder.setRyaInstanceName("test_instance")
            .setRyaVersion("1.2.3.4")
            .setEntityCentricIndexDetails( new EntityCentricIndexDetails(true) )
            .setGeoIndexDetails( new GeoIndexDetails(true) )
            .setTemporalIndexDetails( new TemporalIndexDetails(true) )
            .setFreeTextDetails( new FreeTextIndexDetails(true) )
            .setPCJIndexDetails(
                    PCJIndexDetails.builder()
                        .setEnabled(true)
                        .setFluoDetails( new FluoDetails("test_instance_rya_pcj_updater") )
                        .addPCJDetails(
                                PCJDetails.builder()
                                    .setId("pcj 1")
                                    .setUpdateStrategy(PCJUpdateStrategy.BATCH)
                                    .setLastUpdateTime( new Date() ))
                        .addPCJDetails(
                                PCJDetails.builder()
                                    .setId("pcj 2")
                                    .setUpdateStrategy(PCJUpdateStrategy.INCREMENTAL)))
            .setProspectorDetails( new ProspectorDetails(Optional.of(new Date())) )
            .setJoinSelectivityDetails( new JoinSelectivityDetails(Optional.of(new Date())) );

        final RyaDetails details1 = builder.build();
        final RyaDetails details2 = builder.build();
        assertEquals(details1.hashCode(), details2.hashCode());
    }

    @Test
    public void constructor() {
        final RyaDetails originalDetails = RyaDetails.builder()
            .setRyaInstanceName("test_instance")
            .setRyaVersion("1.2.3.4")
            .setEntityCentricIndexDetails( new EntityCentricIndexDetails(true) )
            .setGeoIndexDetails( new GeoIndexDetails(true) )
            .setTemporalIndexDetails( new TemporalIndexDetails(true) )
            .setFreeTextDetails( new FreeTextIndexDetails(true) )
            .setPCJIndexDetails(
                    PCJIndexDetails.builder()
                        .setEnabled(true)
                        .setFluoDetails( new FluoDetails("test_instance_rya_pcj_updater") )
                        .addPCJDetails(
                                PCJDetails.builder()
                                    .setId("pcj 1")
                                    .setUpdateStrategy(PCJUpdateStrategy.BATCH)
                                    .setLastUpdateTime( new Date() ))
                        .addPCJDetails(
                                PCJDetails.builder()
                                    .setId("pcj 2")
                                    .setUpdateStrategy(PCJUpdateStrategy.INCREMENTAL)))
            .setProspectorDetails( new ProspectorDetails(Optional.of(new Date())) )
            .setJoinSelectivityDetails( new JoinSelectivityDetails(Optional.of(new Date())) )
            .build();

        // Create a new Builder using another RyaDetails object.
        final RyaDetails.Builder builder = new RyaDetails.Builder( originalDetails );

        // Show it builds the object that was passed into it.
        assertEquals(originalDetails, builder.build());
    }
}