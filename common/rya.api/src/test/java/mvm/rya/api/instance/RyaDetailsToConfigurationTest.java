package mvm.rya.api.instance;

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

import static mvm.rya.api.instance.ConfigurationFields.USE_ENTITY;
import static mvm.rya.api.instance.ConfigurationFields.USE_FREETEXT;
import static mvm.rya.api.instance.ConfigurationFields.USE_GEO;
import static mvm.rya.api.instance.ConfigurationFields.USE_PCJ;
import static mvm.rya.api.instance.ConfigurationFields.USE_TEMPORAL;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.google.common.base.Optional;

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

public class RyaDetailsToConfigurationTest {
    @Test
    public void populateConfigTest() {
        final RyaDetails.Builder builder = RyaDetails.builder();

        builder.setRyaInstanceName("test_instance")
            .setRyaVersion("1.2.3.4")
            .setEntityCentricIndexDetails( new EntityCentricIndexDetails(true) )
            .setGeoIndexDetails( new GeoIndexDetails(true) )
            .setTemporalIndexDetails( new TemporalIndexDetails(true) )
            .setFreeTextDetails( new FreeTextIndexDetails(false) )
            .setPCJIndexDetails(
                    PCJIndexDetails.builder()
                        .setEnabled(true)
                        .setFluoDetails( new FluoDetails("test_instance_rya_pcj_updater") )
                        .addPCJDetails(
                                PCJDetails.builder()
                                    .setId("pcj 1")
                                    .setUpdateStrategy(PCJUpdateStrategy.BATCH)
                                    .setLastUpdateTime( new Date() )
                                    .build())
                        .addPCJDetails(
                                PCJDetails.builder()
                                    .setId("pcj 2")
                                    .setUpdateStrategy(PCJUpdateStrategy.INCREMENTAL)
                                    .build())
                        .build())
            .setProspectorDetails( new ProspectorDetails(Optional.of(new Date())) )
            .setJoinSelectivityDetails( new JoinSelectivityDetails(Optional.of(new Date())) );
        final Configuration conf = new Configuration();
        RyaDetailsToConfiguration.addRyaDetailsToConfiguration(builder.build(), conf);

        //defaults are set to cause the assert to fail
        assertTrue(conf.getBoolean(USE_ENTITY, false));
        assertFalse(conf.getBoolean(USE_FREETEXT, true));
        assertTrue(conf.getBoolean(USE_GEO, false));
        assertTrue(conf.getBoolean(USE_TEMPORAL, false));
        assertTrue(conf.getBoolean(USE_PCJ, false));
    }
}
