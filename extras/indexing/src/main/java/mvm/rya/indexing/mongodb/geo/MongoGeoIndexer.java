package mvm.rya.indexing.mongodb.geo;
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

import static mvm.rya.indexing.mongodb.geo.GeoMongoDBStorageStrategy.GeoQueryType.EQUALS;
import static mvm.rya.indexing.mongodb.geo.GeoMongoDBStorageStrategy.GeoQueryType.INTERSECTS;
import static mvm.rya.indexing.mongodb.geo.GeoMongoDBStorageStrategy.GeoQueryType.WITHIN;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.openrdf.model.Statement;
import org.openrdf.query.QueryEvaluationException;

import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.vividsolutions.jts.geom.Geometry;

import info.aduna.iteration.CloseableIteration;
import mvm.rya.indexing.GeoIndexer;
import mvm.rya.indexing.StatementConstraints;
import mvm.rya.indexing.accumulo.ConfigUtils;
import mvm.rya.indexing.mongodb.AbstractMongoIndexer;
import mvm.rya.indexing.mongodb.geo.GeoMongoDBStorageStrategy.GeoQuery;
import mvm.rya.mongodb.MongoDBRdfConfiguration;

public class MongoGeoIndexer extends AbstractMongoIndexer<GeoMongoDBStorageStrategy> implements GeoIndexer {
    private static final String COLLECTION_SUFFIX = "geo";
    private static final Logger logger = Logger.getLogger(MongoGeoIndexer.class);

    public MongoGeoIndexer(final MongoClient mongoClient) {
        super(mongoClient);
    }

    @Override
    protected void init() throws NumberFormatException, IOException{
        super.init();
        predicates = ConfigUtils.getGeoPredicates(conf);
        storageStrategy = new GeoMongoDBStorageStrategy(Double.valueOf(conf.get(MongoDBRdfConfiguration.MONGO_GEO_MAXDISTANCE, "1e-10")));
        storageStrategy.createIndices(collection);
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryEquals(
            final Geometry query, final StatementConstraints constraints) {
        final DBObject queryObj = storageStrategy.getQuery(new GeoQuery(EQUALS, query));
        return withConstraints(constraints, queryObj);
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryDisjoint(
            final Geometry query, final StatementConstraints constraints) {
        throw new UnsupportedOperationException(
                "Disjoint queries are not supported in Mongo DB.");
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryIntersects(
            final Geometry query, final StatementConstraints constraints) {
        final DBObject queryObj = storageStrategy.getQuery(new GeoQuery(INTERSECTS, query));
        return withConstraints(constraints, queryObj);
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryTouches(
            final Geometry query, final StatementConstraints constraints) {
        throw new UnsupportedOperationException(
                "Touches queries are not supported in Mongo DB.");
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryCrosses(
            final Geometry query, final StatementConstraints constraints) {
        throw new UnsupportedOperationException(
                "Crosses queries are not supported in Mongo DB.");
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryWithin(
            final Geometry query, final StatementConstraints constraints) {
        final DBObject queryObj = storageStrategy.getQuery(new GeoQuery(WITHIN, query));
        return withConstraints(constraints, queryObj);
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryContains(
            final Geometry query, final StatementConstraints constraints) {
        throw new UnsupportedOperationException(
                "Contains queries are not supported in Mongo DB.");
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryOverlaps(
            final Geometry query, final StatementConstraints constraints) {
        throw new UnsupportedOperationException(
                "Overlaps queries are not supported in Mongo DB.");
    }

    @Override
    public String getCollectionName() {
        return ConfigUtils.getTablePrefix(conf)  + COLLECTION_SUFFIX;
    }
}