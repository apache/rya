package org.apache.rya.accumulo;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.rya.accumulo.instance.AccumuloRyaInstanceDetailsRepository;
import org.apache.rya.api.instance.RyaDetails;
import org.apache.rya.api.instance.RyaDetails.EntityCentricIndexDetails;
import org.apache.rya.api.instance.RyaDetails.FreeTextIndexDetails;
import org.apache.rya.api.instance.RyaDetails.JoinSelectivityDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails;
import org.apache.rya.api.instance.RyaDetails.ProspectorDetails;
import org.apache.rya.api.instance.RyaDetails.TemporalIndexDetails;
import org.apache.rya.api.instance.RyaDetailsRepository;
import org.junit.rules.ExternalResource;

import com.google.common.base.Optional;

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
public class RyaTestInstanceRule extends ExternalResource {

    private static final MiniAccumuloClusterInstance cluster = MiniAccumuloSingleton.getInstance();
    private static final AtomicInteger ryaInstanceNameCounter = new AtomicInteger(1);
    private static final AtomicInteger userId = new AtomicInteger(1);

    private final boolean install;
    private String ryaInstanceName;

    public RyaTestInstanceRule(boolean install) {
        this.install = install;
    }

    public String getRyaInstanceName() {
        if (ryaInstanceName == null) {
            throw new IllegalStateException("Cannot get rya instance name outside of a test execution.");
        }
        return ryaInstanceName;
    }

    public String createUniqueUser() {
        int id = userId.getAndIncrement();
        return "user_" + id;
    }

    @Override
    protected void before() throws Throwable {

        // Get the next Rya instance name.
        ryaInstanceName = "testInstance_" + ryaInstanceNameCounter.getAndIncrement();

        if (install) {
            // Create Rya Details for the instance name.
            final RyaDetailsRepository detailsRepo = new AccumuloRyaInstanceDetailsRepository(cluster.getConnector(), ryaInstanceName);

            final RyaDetails details = RyaDetails.builder()
                    .setRyaInstanceName(ryaInstanceName)
                    .setRyaVersion("0.0.0.0")
                    .setFreeTextDetails(new FreeTextIndexDetails(true))
                    .setEntityCentricIndexDetails(new EntityCentricIndexDetails(true))
                    //RYA-215                .setGeoIndexDetails( new GeoIndexDetails(true) )
                    .setTemporalIndexDetails(new TemporalIndexDetails(true))
                    .setPCJIndexDetails(PCJIndexDetails.builder().setEnabled(true))
                    .setJoinSelectivityDetails(new JoinSelectivityDetails(Optional.absent()))
                    .setProspectorDetails(new ProspectorDetails(Optional.absent()))
                    .build();

            detailsRepo.initialize(details);
        }
    }

    @Override
    protected void after() {
        ryaInstanceName = null;
        // TODO consider teardown of instance (probably requires additional features)
    }

}
