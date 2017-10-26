package org.apache.rya.indexing.pcj.fluo.app.query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetadataCacheSupplier {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataCacheSupplier.class);
    private static FluoQueryMetadataCache CACHE;
    private static boolean initialized = false;
    private static final int DEFAULT_CAPACITY = 10000;
    private static final int DEFAULT_CONCURRENCY = 8;

    /**
     * Returns an existing cache with the specified instance name, or creates a cache. The created cache will have the
     * indicated capacity and concurrencyLevel if one is provided.
     *
     * @param capacity - capacity used to create a new cache
     * @param concurrencyLevel - concurrencyLevel used to create a new cache
     */
    public static FluoQueryMetadataCache getOrCreateCache(int capacity, int concurrencyLevel) {
        if (!initialized) {
            LOG.debug("Cache has not been initialized.  Initializing cache with capacity: {} and concurrencylevel: {}", capacity,
                    concurrencyLevel);
            CACHE = new FluoQueryMetadataCache(new FluoQueryMetadataDAO(), capacity, concurrencyLevel);
            initialized = true;
        } else {
            LOG.debug("Cache has already been initialized.  Returning cache with capacity: {} and concurrencylevel: {}",
                    CACHE.getCapacity(), CACHE.getConcurrencyLevel());
        }
        return CACHE;
    }

    /**
     * Returns cache with the name {@link FluoQueryMetadataCache#FLUO_CACHE_INSTANCE} if it exists, otherwise creates it
     * with a default size of 10000 entries and a default concurrency level of 8.
     *
     * @return - FluoQueryMetadataCache with default instance name and default capacity and concurrency
     */
    public static FluoQueryMetadataCache getOrCreateCache() {
        return getOrCreateCache(DEFAULT_CAPACITY, DEFAULT_CONCURRENCY);
    }

}
