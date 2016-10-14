/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.shell.util;

import static org.junit.Assert.assertEquals;

import java.util.Date;
import java.util.TimeZone;

import org.junit.Test;

import com.google.common.base.Optional;

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

/**
 * Tests the methods of {@link RyaDetailsFormatter}.
 */
public class RyaDetailsFormatterTest {

    @Test
    public void format() {
        // This test failed if the default timezone was not EST, so now it's fixed at EST.
        TimeZone.setDefault(TimeZone.getTimeZone("America/New_York"));
        // Create the object that will be formatted.
        final RyaDetails details = RyaDetails.builder().setRyaInstanceName("test_instance")
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
                                    .setLastUpdateTime( new Date(1252521351L) ))
                        .addPCJDetails(
                                PCJDetails.builder()
                                    .setId("pcj 2")
                                    .setUpdateStrategy(PCJUpdateStrategy.INCREMENTAL)))
            .setProspectorDetails( new ProspectorDetails(Optional.of(new Date(12525211L))) )
            .setJoinSelectivityDetails( new JoinSelectivityDetails(Optional.of(new Date(125221351L))) )
            .build();

        final String formatted = new RyaDetailsFormatter().format(details);

        // Verify the created object matches the expected result.
        final String expected =
                "General Metadata:\n" +
                "  Instance Name: test_instance\n" +
                "  RYA Version: 1.2.3.4\n" +
                "Secondary Indicies:\n" +
                "  Entity Centric Index:\n" +
                "    Enabled: true\n" +
                "  Geospatial Index:\n" +
                "    Enabled: true\n" +
                "  Free Text Index:\n" +
                "    Enabled: true\n" +
                "  Temporal Index:\n" +
                "    Enabled: true\n" +
                "  PCJ Index:\n" +
                "    Enabled: true\n" +
                "    Fluo App Name: test_instance_rya_pcj_updater\n" +
                "    PCJs:\n" +
                "      ID: pcj 1\n" +
                "        Update Strategy: BATCH\n" +
                "        Last Update Time: Thu Jan 15 06:55:21 EST 1970\n" +
                "      ID: pcj 2\n" +
                "        Update Strategy: INCREMENTAL\n" +
                "        Last Update Time: unavailable\n" +
                "Statistics:\n" +
                "  Prospector:\n" +
                "    Last Update Time: Wed Dec 31 22:28:45 EST 1969\n" +
                "  Join Selectivity:\n" +
                "    Last Updated Time: Fri Jan 02 05:47:01 EST 1970\n";

        assertEquals(expected, formatted);
    }
}