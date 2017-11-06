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
 *  Manages the creation of the {@link StatementPatternIdCache} in the Fluo application.
 *  This supplier enforces singleton like behavior in that it will only create the cache if it
 *  doesn't already exist.  The StatementPatternIdCache is not a singleton in itself.
 */
public class StatementPatternIdCacheSupplier {

    private static final Logger LOG = LoggerFactory.getLogger(StatementPatternIdCacheSupplier.class);
    private static boolean initialized = false;
    private static StatementPatternIdCache CACHE;
    private static final ReentrantLock lock = new ReentrantLock();

    /**
     * Returns an existing cache if one has been created, otherwise creates a new cache.
     *
     * @return - existing StatementPatternIdCache or new cache if one didn't already exist
     */
    public static StatementPatternIdCache getOrCreateCache() {
        lock.lock();
        try {
            if (!initialized) {
                LOG.debug("Cache has not been initialized.  Initializing StatementPatternIdCache");
                CACHE = new StatementPatternIdCache();
                initialized = true;
            } else {
                LOG.debug("A StatementPatternIdCache has already been initialized.");
            }
            return CACHE;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Deletes stored cache and flags Supplier as uninitialized.
     */
    public static void clear() {
        lock.lock();
        try {
            if (initialized) {
                CACHE.clear();
                CACHE = null;
                initialized = false;
            }
        } finally {
            lock.unlock();
        }
    }
}
