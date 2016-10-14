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
package org.apache.rya.indexing.external;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.apache.rya.indexing.pcj.update.PrecomputedJoinUpdater;
import org.junit.Test;

import com.google.common.base.Supplier;

import org.apache.rya.indexing.external.PrecomputedJoinIndexerConfig.PrecomputedJoinUpdaterType;
import org.apache.rya.indexing.external.fluo.FluoPcjUpdater;
import org.apache.rya.indexing.external.fluo.FluoPcjUpdaterSupplier;

/**
 * Tests the methods of {@link PrecomputedJoinUpdaterSupplier}.
 */
public class PrecomputedJoinUpdaterSupplierTest {

    @Test(expected = NullPointerException.class)
    public void notConfigured() {
        // Create a supplier that does not return any configuration.
        final Supplier<Configuration> configSupplier = mock(Supplier.class);
        final PrecomputedJoinUpdaterSupplier updaterSupplier = new PrecomputedJoinUpdaterSupplier(configSupplier, mock(FluoPcjUpdaterSupplier.class));

        // Try to get the updater.
        updaterSupplier.get();
    }

    @Test(expected = IllegalArgumentException.class)
    public void updaterTypeNotSet() {
        // Create a supplier that is not configured to know which type of updater to use.
        final Supplier<Configuration> configSupplier = mock(Supplier.class);
        when(configSupplier.get()).thenReturn( new Configuration() );
        final PrecomputedJoinUpdaterSupplier updaterSupplier = new PrecomputedJoinUpdaterSupplier(configSupplier, mock(FluoPcjUpdaterSupplier.class));

        // Try to get the updater.
        updaterSupplier.get();
    }

    @Test
    public void configuredForFluo() {
        // Create a supplier that is configured to use a Fluo updater.
        final Supplier<Configuration> configSupplier = mock(Supplier.class);
        final Configuration config = new Configuration();
        config.set(PrecomputedJoinIndexerConfig.PCJ_UPDATER_TYPE, PrecomputedJoinUpdaterType.FLUO.toString());
        when(configSupplier.get()).thenReturn( config );

        final FluoPcjUpdaterSupplier fluoSupplier = mock(FluoPcjUpdaterSupplier.class);
        final FluoPcjUpdater mockFluoUpdater = mock(FluoPcjUpdater.class);
        when(fluoSupplier.get()).thenReturn(mockFluoUpdater);

        final PrecomputedJoinUpdaterSupplier updaterSupplier = new PrecomputedJoinUpdaterSupplier(configSupplier, fluoSupplier);

        // Ensure the mock FluoPcjUpdater is what was returned.
        final PrecomputedJoinUpdater updater = updaterSupplier.get();
        assertEquals(mockFluoUpdater, updater);
    }
}