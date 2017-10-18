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
package org.apache.rya.indexing.entity;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.rya.indexing.entity.model.Entity;
import org.apache.rya.indexing.entity.query.EntityQueryNode;
import org.apache.rya.indexing.entity.storage.EntityStorage;
import org.apache.rya.indexing.entity.storage.EntityStorage.EntityStorageException;
import org.apache.rya.indexing.entity.storage.TypeStorage;
import org.apache.rya.indexing.entity.update.mongo.MongoEntityIndexer;
import org.apache.rya.indexing.external.matching.*;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.TupleExpr;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Optimizes a query over {@link Entity}s.
 */
public class EntityIndexOptimizer extends AbstractExternalSetOptimizer<EntityQueryNode> implements Configurable {
    private static final Logger log = Logger.getLogger(EntityIndexOptimizer.class);
    private static final EntityExternalSetMatcherFactory MATCHER_FACTORY = new EntityExternalSetMatcherFactory();

    private final MongoEntityIndexer indexer;
    private EntityIndexSetProvider provider;
    private Configuration conf;

    private EntityStorage entityStorage;
    private TypeStorage typeStorage;

    /**
     * Creates a new {@link EntityIndexOptimizer}.
     */
    public EntityIndexOptimizer() {
        indexer = new MongoEntityIndexer();

        //There is no rater for entity query nodes.
        useOptimal = false;
    }

    @Override
    public void setConf(final Configuration conf) {
        this.conf = conf;
        indexer.setConf(conf);

        typeStorage = indexer.getTypeStorage(conf);
        try {
            entityStorage = indexer.getEntityStorage(conf);
        } catch (final EntityStorageException e) {
            log.error("Error getting entity storage", e);
        }

        provider = new EntityIndexSetProvider(typeStorage, entityStorage);
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void optimize(TupleExpr tupleExpr, final Dataset dataset, final BindingSet bindings) {
        checkNotNull(tupleExpr);
        checkNotNull(indexer);

        // first standardize query by pulling all filters to top of query if
        // they exist using TopOfQueryFilterRelocator
        tupleExpr = TopOfQueryFilterRelocator.moveFiltersToTop(tupleExpr);
        super.optimize(tupleExpr, null, null);
    }

    @VisibleForTesting
    public EntityStorage getEntityStorage() {
        return entityStorage;
    }

    @VisibleForTesting
    public TypeStorage getTypeStorage() {
        return typeStorage;
    }

    /**
     * Visits a node of the query tree.  Checks to see if the node belongs
     * or is of an {@link Entity}.  If it is, prepares it, so after the query
     * is checked, the nodes can be replaced with {@link EntityQueryNode}s.
     */

    @Override
    protected ExternalSetMatcher<EntityQueryNode> getMatcher(final QuerySegment<EntityQueryNode> segment) {
        return MATCHER_FACTORY.getMatcher(segment);
    }

    @Override
    protected ExternalSetProvider<EntityQueryNode> getProvider() {
        return provider;
    }

    @Override
    protected Optional<QueryNodeListRater> getNodeListRater(final QuerySegment<EntityQueryNode> segment) {
        return Optional.absent();
    }
}
