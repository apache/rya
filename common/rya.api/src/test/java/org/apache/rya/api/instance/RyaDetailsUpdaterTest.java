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
package org.apache.rya.api.instance;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Date;

import org.junit.Test;

import com.google.common.base.Optional;

import org.apache.rya.api.instance.RyaDetails.EntityCentricIndexDetails;
import org.apache.rya.api.instance.RyaDetails.FreeTextIndexDetails;
import org.apache.rya.api.instance.RyaDetails.GeoIndexDetails;
import org.apache.rya.api.instance.RyaDetails.JoinSelectivityDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails;
import org.apache.rya.api.instance.RyaDetails.ProspectorDetails;
import org.apache.rya.api.instance.RyaDetails.TemporalIndexDetails;
import org.apache.rya.api.instance.RyaDetailsRepository.ConcurrentUpdateException;
import org.apache.rya.api.instance.RyaDetailsRepository.NotInitializedException;
import org.apache.rya.api.instance.RyaDetailsRepository.RyaDetailsRepositoryException;
import org.apache.rya.api.instance.RyaDetailsUpdater.RyaDetailsMutator;
import org.apache.rya.api.instance.RyaDetailsUpdater.RyaDetailsMutator.CouldNotApplyMutationException;

/**
 * Tests the methods of {@link RyaDetailsUpdater}.
 */
public class RyaDetailsUpdaterTest {

    @Test
    public void update() throws RyaDetailsRepositoryException, CouldNotApplyMutationException {
        // Setup initial details and mock a repository that returns them.
        final RyaDetails originalDetails = RyaDetails.builder()
                .setRyaInstanceName("instanceName")
                .setRyaVersion("0.0.0.0")
                .setFreeTextDetails( new FreeTextIndexDetails(true) )
                .setEntityCentricIndexDetails( new EntityCentricIndexDetails(true) )
                .setGeoIndexDetails( new GeoIndexDetails(true) )
                .setTemporalIndexDetails( new TemporalIndexDetails(true) )
                .setPCJIndexDetails(
                        PCJIndexDetails.builder()
                            .setEnabled(true) )
                .setJoinSelectivityDetails( new JoinSelectivityDetails( Optional.<Date>absent() ) )
                .setProspectorDetails( new ProspectorDetails( Optional.<Date>absent() ))
                .build();

        final RyaDetailsRepository detailsRepo = mock( RyaDetailsRepository.class );
        when( detailsRepo.getRyaInstanceDetails() ).thenReturn( originalDetails );

        // Use an updater to change the Rya version number.
        new RyaDetailsUpdater(detailsRepo).update(new RyaDetailsMutator() {
            @Override
            public RyaDetails mutate(final RyaDetails old) {
                return RyaDetails.builder(old)
                        .setRyaVersion("1.1.1.1")
                        .build();
            }
        });

        // Verify the repository was asked to update the details.
        final RyaDetails mutatedDetails = RyaDetails.builder(originalDetails)
                .setRyaVersion("1.1.1.1")
                .build();
        verify(detailsRepo, times(1)).update( eq(originalDetails), eq(mutatedDetails) );
    }

    @Test
    public void update_concurrentUpdateEncountered() throws NotInitializedException, RyaDetailsRepositoryException, CouldNotApplyMutationException {
        // Setup initial details and mock a repository that returns them.
        final RyaDetails originalDetails = RyaDetails.builder()
                .setRyaInstanceName("instanceName")
                .setRyaVersion("0.0.0.0")
                .setFreeTextDetails( new FreeTextIndexDetails(true) )
                .setEntityCentricIndexDetails( new EntityCentricIndexDetails(true) )
                .setGeoIndexDetails( new GeoIndexDetails(true) )
                .setTemporalIndexDetails( new TemporalIndexDetails(true) )
                .setPCJIndexDetails(
                        PCJIndexDetails.builder()
                            .setEnabled(true) )
                .setJoinSelectivityDetails( new JoinSelectivityDetails( Optional.<Date>absent() ) )
                .setProspectorDetails( new ProspectorDetails( Optional.<Date>absent() ))
                .build();

        final RyaDetails otherUsersUpdate = RyaDetails.builder(originalDetails)
                .setRyaVersion("1.1.1.1")
                .build();

        // The first time get detail is called, we get the original state for the details.
        // When the mutator tries to update using those, it throws an exception.
        // The second iteration, the other user's state is updated.
        // When the mutator tries to update again, it succeeds.
        final RyaDetailsRepository detailsRepo = mock( RyaDetailsRepository.class );

        when( detailsRepo.getRyaInstanceDetails() )
            .thenReturn( originalDetails )
            .thenReturn( otherUsersUpdate );

        doThrow( ConcurrentUpdateException.class ).when( detailsRepo ).update( eq(originalDetails), any(RyaDetails.class) );

        // Run the test.
        new RyaDetailsUpdater(detailsRepo).update(new RyaDetailsMutator() {
            @Override
            public RyaDetails mutate(final RyaDetails old) {
                return RyaDetails.builder(old)
                        .setTemporalIndexDetails( new TemporalIndexDetails(false) )
                        .build();
            }
        });

        // Verify the intended mutation eventually gets committed.
        final RyaDetails finalDetails = RyaDetails.builder(originalDetails)
                .setRyaVersion("1.1.1.1")
                .setTemporalIndexDetails( new TemporalIndexDetails(false) )
                .build();

        verify(detailsRepo, times(1)).update( eq(otherUsersUpdate), eq(finalDetails) );
    }
}