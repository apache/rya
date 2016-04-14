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
package mvm.rya.indexing.external;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage;
import org.junit.Test;

import com.google.common.base.Supplier;

import mvm.rya.indexing.external.PrecomputedJoinIndexerConfig.PrecomputedJoinStorageType;
import mvm.rya.indexing.external.accumulo.AccumuloPcjStorage;
import mvm.rya.indexing.external.accumulo.AccumuloPcjStorageSupplier;

/**
 * Tests the methods of {@link PrecomputedJoinStorageSupplier}.
 */
public class PrecomputedJoinStorageSupplierTest {

    @Test(expected = NullPointerException.class)
    public void notConfigured() {
        // Create a supplier that does not return any configuration.
        final Supplier<Configuration> configSupplier = mock(Supplier.class);
        final PrecomputedJoinStorageSupplier storageSupplier = new PrecomputedJoinStorageSupplier(configSupplier, mock(AccumuloPcjStorageSupplier.class));

        // Try to get the storage.
        storageSupplier.get();
    }

    @Test(expected = IllegalArgumentException.class)
    public void storageTypeNotSet() {
        // Create a supplier that does not return any configuration.
        final Supplier<Configuration> configSupplier = mock(Supplier.class);
        when(configSupplier.get()).thenReturn( new Configuration() );
        final PrecomputedJoinStorageSupplier storageSupplier = new PrecomputedJoinStorageSupplier(configSupplier, mock(AccumuloPcjStorageSupplier.class));

        // Try to get the storage.
        storageSupplier.get();
    }

    @Test
    public void configuredForAccumulo() {
        // Create a supplier that does not return any configuration.
        final Supplier<Configuration> configSupplier = mock(Supplier.class);
        final Configuration config = new Configuration();
        config.set(PrecomputedJoinIndexerConfig.PCJ_STORAGE_TYPE, PrecomputedJoinStorageType.ACCUMULO.toString());
        when(configSupplier.get()).thenReturn( config );

        final AccumuloPcjStorageSupplier accumuloSupplier = mock(AccumuloPcjStorageSupplier.class);
        final AccumuloPcjStorage mockAccumuloStorage = mock(AccumuloPcjStorage.class);
        when(accumuloSupplier.get()).thenReturn(mockAccumuloStorage);

        final PrecomputedJoinStorageSupplier storageSupplier = new PrecomputedJoinStorageSupplier(configSupplier, accumuloSupplier);

        // Ensure the mock AccumuloPcjStorage is what was returned.
        final PrecomputedJoinStorage storage = storageSupplier.get();
        assertEquals(mockAccumuloStorage, storage);
    }
}