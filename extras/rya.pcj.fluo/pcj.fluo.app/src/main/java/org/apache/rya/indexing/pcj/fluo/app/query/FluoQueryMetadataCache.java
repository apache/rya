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
 */package org.apache.rya.indexing.pcj.fluo.app.query;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.fluo.api.client.SnapshotBase;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.rya.indexing.pcj.fluo.app.NodeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;


/**
 * Wrapper for {@link FluoQueryMetadataDAO} that caches any metadata that has been retrieved from Fluo. This class first
 * checks the cache to see if the metadata is present before delegating to the underlying DAO method to retrieve the
 * data.  The cache has a fixed capacity (determined at construction time), and evicts the least recently used entries
 * when space is needed.
 *
 */
public class FluoQueryMetadataCache extends FluoQueryMetadataDAO {

    private static final Logger LOG = LoggerFactory.getLogger(FluoQueryMetadataCache.class);

    private final FluoQueryMetadataDAO dao;
    private final Cache<String, CommonNodeMetadata> commonNodeMetadataCache;
    private final Cache<String, Bytes> metadataCache;
    private int capacity;
    private int concurrencyLevel;

    /**
     * Creates a FluoQueryMetadataCache with the specified capacity. Old, unused results are evicted as necessary.
     * @param capacity - max size of the cache
     * @param concurrencyLevel - indicates how the cache will be partitioned to that different threads can access those
     * partitions in a non-serialized manner
     * @throws IllegalArgumentException if dao is null, capacity <= 0, or concurrencyLevel <= 0
     */
    public FluoQueryMetadataCache(FluoQueryMetadataDAO dao, int capacity, int concurrencyLevel) {
        checkNotNull(dao);
        checkArgument(capacity > 0);
        checkArgument(concurrencyLevel > 0);
        this.dao = dao;
        commonNodeMetadataCache = CacheBuilder.newBuilder().concurrencyLevel(concurrencyLevel).maximumSize(capacity).build();
        metadataCache = CacheBuilder.newBuilder().concurrencyLevel(concurrencyLevel).maximumSize(capacity).build();
        this.capacity = capacity;
        this.concurrencyLevel = concurrencyLevel;
    }

    /**
     * @return - capacity of this cache in terms of max number of entries
     */
    public int getCapacity() {
        return capacity;
    }

    /**
     * @return - concurrencyLevel of this cache,in terms of number of partitions that distinct threads can operate on
     *         without waiting for other threads
     */
    public int getConcurrencyLevel() {
        return concurrencyLevel;
    }


    /**
     * @throws IllegalArgumentException if tx or nodeId is null, and if {@link NodeType#fromNodeId(String)}
     * does not return an Optional containing {@link NodeType#STATEMENT_PATTERN}.
     */
    @Override
    public StatementPatternMetadata readStatementPatternMetadata(SnapshotBase tx, String nodeId) {
        checkNotNull(nodeId);
        checkNotNull(tx);
        Optional<NodeType> type = NodeType.fromNodeId(nodeId);
        checkArgument(type.isPresent() && type.get() == NodeType.STATEMENT_PATTERN);
        try {
            LOG.debug("Retrieving Metadata from Cache: {}", nodeId);
            return (StatementPatternMetadata) commonNodeMetadataCache.get(nodeId, () -> {
                LOG.debug("Seeking Metadata from Fluo Table: {}", nodeId);
                return dao.readStatementPatternMetadata(tx, nodeId);
            });
        } catch (Exception e) {
            throw new RuntimeException("Unable to access StatementPatternMetadata for nodeId: " + nodeId, e);
        }
    }

    /**
     * @throws IllegalArgumentException if tx or nodeId is null, and if {@link NodeType#fromNodeId(String)}
     * does not return an Optional containing {@link NodeType#JOIN}.
     */
    @Override
    public JoinMetadata readJoinMetadata(SnapshotBase tx, String nodeId) {
        checkNotNull(nodeId);
        checkNotNull(tx);
        Optional<NodeType> type = NodeType.fromNodeId(nodeId);
        checkArgument(type.isPresent() && type.get() == NodeType.JOIN);
        try {
            LOG.debug("Retrieving Metadata from Cache: {}.", nodeId);
            return (JoinMetadata) commonNodeMetadataCache.get(nodeId, () -> {
                LOG.debug("Seeking Metadata from Fluo Table: {}.", nodeId);
                return dao.readJoinMetadata(tx, nodeId);
            });
        } catch (Exception e) {
            throw new RuntimeException("Unable to access JoinMetadata for nodeId: " + nodeId, e);
        }
    }

    /**
     * @throws IllegalArgumentException if tx or nodeId is null, and if {@link NodeType#fromNodeId(String)}
     * does not return an Optional containing {@link NodeType#FILTER}.
     */
    @Override
    public FilterMetadata readFilterMetadata(SnapshotBase tx, String nodeId) {
        checkNotNull(nodeId);
        checkNotNull(tx);
        Optional<NodeType> type = NodeType.fromNodeId(nodeId);
        checkArgument(type.isPresent() && type.get() == NodeType.FILTER);
        try {
            LOG.debug("Retrieving Metadata from Cache: {}", nodeId);
            return (FilterMetadata) commonNodeMetadataCache.get(nodeId, () -> {
                LOG.debug("Seeking Metadata from Fluo Table: {}.", nodeId);
                return dao.readFilterMetadata(tx, nodeId);
            });
        } catch (Exception e) {
            throw new RuntimeException("Unable to access FilterMetadata for nodeId: " + nodeId, e);
        }
    }

    /**
     * @throws IllegalArgumentException if tx or nodeId is null, and if {@link NodeType#fromNodeId(String)}
     * does not return an Optional containing {@link NodeType#PROJECTION}.
     */
    @Override
    public ProjectionMetadata readProjectionMetadata(SnapshotBase tx, String nodeId) {
        checkNotNull(nodeId);
        checkNotNull(tx);
        Optional<NodeType> type = NodeType.fromNodeId(nodeId);
        checkArgument(type.isPresent() && type.get() == NodeType.PROJECTION);
        try {
            LOG.debug("Retrieving Metadata from Cache: {}", nodeId);
            return (ProjectionMetadata) commonNodeMetadataCache.get(nodeId, () -> {
                LOG.debug("Seeking Metadata from Fluo Table: {}.", nodeId);
                return dao.readProjectionMetadata(tx, nodeId);
            });
        } catch (Exception e) {
            throw new RuntimeException("Unable to access ProjectionMetadata for nodeId: " + nodeId, e);
        }
    }

    /**
     * @throws IllegalArgumentException if tx or nodeId is null, and if {@link NodeType#fromNodeId(String)}
     * does not return an Optional containing {@link NodeType#AGGREGATION}.
     */
    @Override
    public AggregationMetadata readAggregationMetadata(SnapshotBase tx, String nodeId) {
        checkNotNull(nodeId);
        checkNotNull(tx);
        Optional<NodeType> type = NodeType.fromNodeId(nodeId);
        checkArgument(type.isPresent() && type.get() == NodeType.AGGREGATION);
        try {
            LOG.debug("Retrieving Metadata from Cache: {}", nodeId);
            return (AggregationMetadata) commonNodeMetadataCache.get(nodeId, () -> {
                LOG.debug("Seeking Metadata from Fluo Table: {}.", nodeId);
                return dao.readAggregationMetadata(tx, nodeId);
            });
        } catch (Exception e) {
            throw new RuntimeException("Unable to access AggregationMetadata for nodeId: " + nodeId, e);
        }
    }

    /**
     * @throws IllegalArgumentException if tx or nodeId is null, and if {@link NodeType#fromNodeId(String)}
     * does not return an Optional containing {@link NodeType#CONSTRUCT}.
     */
    @Override
    public ConstructQueryMetadata readConstructQueryMetadata(SnapshotBase tx, String nodeId) {
        checkNotNull(nodeId);
        checkNotNull(tx);
        Optional<NodeType> type = NodeType.fromNodeId(nodeId);
        checkArgument(type.isPresent() && type.get() == NodeType.CONSTRUCT);
        try {
            LOG.debug("Retrieving Metadata from Cache: {}", nodeId);
            return (ConstructQueryMetadata) commonNodeMetadataCache.get(nodeId, () -> {
                LOG.debug("Seeking Metadata from Fluo Table: {}.", nodeId);
                return dao.readConstructQueryMetadata(tx, nodeId);
            });
        } catch (Exception e) {
            throw new RuntimeException("Unable to access ConstructQueryMetadata for nodeId: " + nodeId, e);
        }
    }

    /**
     * @throws IllegalArgumentException if tx or nodeId is null, and if {@link NodeType#fromNodeId(String)}
     * does not return an Optional containing {@link NodeType#PERIODIC_QUERY}.
     */
    @Override
    public PeriodicQueryMetadata readPeriodicQueryMetadata(SnapshotBase tx, String nodeId) {
        checkNotNull(nodeId);
        checkNotNull(tx);
        Optional<NodeType> type = NodeType.fromNodeId(nodeId);
        checkArgument(type.isPresent() && type.get() == NodeType.PERIODIC_QUERY);
        try {
            LOG.debug("Retrieving Metadata from Cache: {}", nodeId);
            return (PeriodicQueryMetadata) commonNodeMetadataCache.get(nodeId, () -> {
                LOG.debug("Seeking Metadata from Fluo Table: {}.", nodeId);
                return dao.readPeriodicQueryMetadata(tx, nodeId);
            });
        } catch (Exception e) {
            throw new RuntimeException("Unable to access PeriodicQueryMetadata for nodeId: " + nodeId, e);
        }
    }

    /**
     * @throws IllegalArgumentException if tx or nodeId is null, and if {@link NodeType#fromNodeId(String)}
     * does not return an Optional containing {@link NodeType#QUERY}.
     */
    @Override
    public QueryMetadata readQueryMetadata(SnapshotBase tx, String nodeId) {
        checkNotNull(nodeId);
        checkNotNull(tx);
        Optional<NodeType> type = NodeType.fromNodeId(nodeId);
        checkArgument(type.isPresent() && type.get() == NodeType.QUERY);
        try {
            LOG.debug("Retrieving Metadata from Cache: {}", nodeId);
            return (QueryMetadata) commonNodeMetadataCache.get(nodeId, () -> {
                LOG.debug("Seeking Metadata from Fluo Table: {}.", nodeId);
                return dao.readQueryMetadata(tx, nodeId);
            });
        } catch (Exception e) {
            throw new RuntimeException("Unable to access QueryMetadata for nodeId: " + nodeId, e);
        }
    }

    /**
     * Reads specific metadata entries from the cache.  This method will retrieve the entry
     * from the Fluo table if it does not already exist in the cache.
     * @param tx - Transaction for interacting with Fluo
     * @param rowId - rowId for metadata entry
     * @param column - column of metadata entry
     * @return - value associated with the metadata entry
     */
    public Bytes readMetadadataEntry(SnapshotBase tx, String rowId, Column column) {
        checkNotNull(rowId);
        checkNotNull(tx);
        checkNotNull(column);
        Optional<NodeType> type = NodeType.fromNodeId(rowId);
        checkArgument(type.isPresent() && type.get().getMetaDataColumns().contains(column));
        try {
            return metadataCache.get(getKey(rowId, column), () -> tx.get(Bytes.of(rowId), column));
        } catch (Exception e) {
            throw new RuntimeException("Unable to access Metadata Entry with rowId: " + rowId + " and column: " + column, e);
        }
    }

    /**
     * Deletes contents of cache.
     */
    public void clear() {
        commonNodeMetadataCache.asMap().clear();
        metadataCache.asMap().clear();
    }

    private String getKey(String row, Column column) {
        return row + ":" + column.getsQualifier() + ":" + column.getsQualifier();
    }
}
