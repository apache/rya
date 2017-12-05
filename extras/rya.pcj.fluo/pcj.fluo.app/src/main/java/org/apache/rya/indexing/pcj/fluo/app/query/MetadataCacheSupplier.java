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
package org.apache.rya.indexing.pcj.fluo.app.query;

import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  Manages the creation of the {@link FluoQueryMetadataCache} in the Fluo application.
 *  This supplier enforces singleton like behavior in that it will only create the cache if it
 *  doesn't already exist.  The FluoQueryMetadataCache is not a singleton in itself.
 */
public class MetadataCacheSupplier {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataCacheSupplier.class);
    private static FluoQueryMetadataCache CACHE;
    private static boolean initialized = false;
    private static final int DEFAULT_CAPACITY = 10000;
    private static final int DEFAULT_CONCURRENCY = 8;
    private static final ReentrantLock lock = new ReentrantLock();

    /**
     * Returns an existing cache with the specified instance name, or creates a cache. The created cache will have the
     * indicated capacity and concurrencyLevel if one is provided.
     *
     * @param capacity - capacity used to create a new cache
     * @param concurrencyLevel - concurrencyLevel used to create a new cache
     */
    public static FluoQueryMetadataCache getOrCreateCache(int capacity, int concurrencyLevel) {
        lock.lock();
        try {
            if (!initialized) {
                LOG.debug("Cache has not been initialized.  Initializing cache with capacity: {} and concurrencylevel: {}", capacity,
                        concurrencyLevel);
                CACHE = new FluoQueryMetadataCache(new FluoQueryMetadataDAO(), capacity, concurrencyLevel);
                initialized = true;
            } else {
                LOG.warn(
                        "A cache has already been initialized, so a cache with capacity: {} and concurrency level: {} will not be created.  Returning existing cache with capacity: {} and concurrencylevel: {}",
                        capacity, concurrencyLevel, CACHE.getCapacity(), CACHE.getConcurrencyLevel());
            }
            return CACHE;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Creates a FluoQueryMetadataCache with a default size of 10000 entries and a default concurrency level of 8.
     *
     * @return - FluoQueryMetadataCache with default instance name and default capacity and concurrency
     */
    public static FluoQueryMetadataCache getOrCreateCache() {
        return getOrCreateCache(DEFAULT_CAPACITY, DEFAULT_CONCURRENCY);
    }

    /**
     * Clears contents of cache and makes supplier uninitialized so that it creates a new cache.
     * This is useful for integration tests.
     */
    public static void clear() {
        lock.lock();
        try{
            if(initialized) {
                CACHE.clear();
                CACHE = null;
                initialized = false;
            }
        } finally {
            lock.unlock();
        }
    }

}
