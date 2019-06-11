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
package org.apache.rya.indexing.geotemporal.mongo;

import static org.apache.rya.indexing.mongodb.geo.GeoMongoDBStorageStrategy.GeoQueryType.EQUALS;
import static org.apache.rya.indexing.mongodb.geo.GeoMongoDBStorageStrategy.GeoQueryType.INTERSECTS;
import static org.apache.rya.indexing.mongodb.geo.GeoMongoDBStorageStrategy.GeoQueryType.WITHIN;
import static org.apache.rya.indexing.mongodb.temporal.TemporalMongoDBStorageStrategy.INSTANT;
import static org.apache.rya.indexing.mongodb.temporal.TemporalMongoDBStorageStrategy.INTERVAL_END;
import static org.apache.rya.indexing.mongodb.temporal.TemporalMongoDBStorageStrategy.INTERVAL_START;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;

import org.apache.log4j.Logger;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.resolver.RyaToRdfConversions;
import org.apache.rya.indexing.GeoConstants;
import org.apache.rya.indexing.IndexingExpr;
import org.apache.rya.indexing.TemporalInstant;
import org.apache.rya.indexing.TemporalInstantRfc3339;
import org.apache.rya.indexing.TemporalInterval;
import org.apache.rya.indexing.accumulo.geo.GeoParseUtils;
import org.apache.rya.indexing.geotemporal.GeoTemporalIndexException;
import org.apache.rya.indexing.geotemporal.GeoTemporalIndexer.GeoPolicy;
import org.apache.rya.indexing.geotemporal.GeoTemporalIndexer.TemporalPolicy;
import org.apache.rya.indexing.mongodb.IndexingMongoDBStorageStrategy;
import org.apache.rya.indexing.mongodb.geo.GeoMongoDBStorageStrategy;
import org.apache.rya.indexing.mongodb.geo.GeoMongoDBStorageStrategy.GeoQuery;
import org.apache.rya.indexing.mongodb.geo.GmlParser;
import org.apache.rya.indexing.mongodb.temporal.TemporalMongoDBStorageStrategy;
import org.apache.rya.mongodb.document.operators.query.QueryBuilder;
import org.bson.Document;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.joda.time.DateTime;

import com.mongodb.client.MongoCollection;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

/**
 * Storage adapter for serializing Geo Temporal statements into mongo objects.
 * This includes adapting the {@link IndexingExpr}s for the GeoTemporal indexer.
 */
public class GeoTemporalMongoDBStorageStrategy extends IndexingMongoDBStorageStrategy {
    private static final Logger LOG = Logger.getLogger(GeoTemporalMongoDBStorageStrategy.class);
    private static final String GEO_KEY = "location";
    private static final String TIME_KEY = "time";
    private final TemporalMongoDBStorageStrategy temporalStrategy;
    private final GeoMongoDBStorageStrategy geoStrategy;

    public GeoTemporalMongoDBStorageStrategy() {
        geoStrategy = new GeoMongoDBStorageStrategy(0.0);
        temporalStrategy = new TemporalMongoDBStorageStrategy();
    }

    @Override
    public void createIndices(final MongoCollection<Document> coll){
        coll.createIndex(new Document(GEO_KEY, "2dsphere"));
        coll.createIndex(new Document(TIME_KEY, 1));
    }

    public Document getFilterQuery(final Collection<IndexingExpr> geoFilters, final Collection<IndexingExpr> temporalFilters) throws GeoTemporalIndexException {
        final QueryBuilder builder = QueryBuilder.start();
        if(!geoFilters.isEmpty()) {
            final Document[] geo = getGeoObjs(geoFilters);
            if(!temporalFilters.isEmpty()) {
                final Document[] temporal = getTemporalObjs(temporalFilters);
                builder.and(oneOrAnd(geo), oneOrAnd(temporal));
                return builder.get();
            } else {
                return oneOrAnd(geo);
            }
        } else if(!temporalFilters.isEmpty()) {
            final Document[] temporal = getTemporalObjs(temporalFilters);
            return oneOrAnd(temporal);
        } else {
            return builder.get();
        }
    }

    private Document oneOrAnd(final Document[] docs) {
        if(docs.length == 1) {
            return docs[0];
        }
        return QueryBuilder.start()
            .and(docs)
            .get();
    }

    @Override
    public Document serialize(final RyaStatement ryaStatement) {
        final Document doc = new Document("_id", ryaStatement.getSubject().hashCode());
        final IRI obj = ryaStatement.getObject().getDataType();

        if(obj.equals(GeoConstants.GEO_AS_WKT) || obj.equals(GeoConstants.GEO_AS_GML) ||
           obj.equals(GeoConstants.XMLSCHEMA_OGC_GML) || obj.equals(GeoConstants.XMLSCHEMA_OGC_WKT)) {
            try {
                final Statement statement = RyaToRdfConversions.convertStatement(ryaStatement);
                final Geometry geo = GeoParseUtils.getGeometry(statement, new GmlParser());
                if (geo.getNumPoints() > 1) {
                    doc.append(GEO_KEY, geoStrategy.getCorrespondingPoints(geo));
                } else {
                    doc.append(GEO_KEY, geoStrategy.getDBPoint(geo));
                }
            } catch (final ParseException e) {
                LOG.error("Could not create geometry for statement " + ryaStatement, e);
                return null;
            }
        } else {
            doc.append(TIME_KEY, temporalStrategy.getTimeValue(ryaStatement.getObject().getData()));
        }
        return doc;
    }

    private Document[] getGeoObjs(final Collection<IndexingExpr> geoFilters) {
        final List<Document> objs = new ArrayList<>();
        geoFilters.forEach(filter -> {
            final GeoPolicy policy = GeoPolicy.fromURI(filter.getFunction());
            final WKTReader reader = new WKTReader();
            final String geoStr = ((Value) filter.getArguments()[0]).stringValue();
            try {
                //This method is what is used in the GeoIndexer.
                final Geometry geo = reader.read(geoStr);
                objs.add(getGeoObject(geo, policy));
            } catch (final GeoTemporalIndexException | UnsupportedOperationException | ParseException e) {
                LOG.error("Unable to parse '" + geoStr + "'.", e);
            }
        });
        return objs.toArray(new Document[]{});
    }

    private Document[] getTemporalObjs(final Collection<IndexingExpr> temporalFilters) {
        final List<Document> objs = new ArrayList<>();
        temporalFilters.forEach(filter -> {
            final TemporalPolicy policy = TemporalPolicy.fromURI(filter.getFunction());
            final String timeStr = ((Value) filter.getArguments()[0]).stringValue();
            final Matcher matcher = TemporalInstantRfc3339.PATTERN.matcher(timeStr);
            if(matcher.find()) {
                final TemporalInterval interval = TemporalInstantRfc3339.parseInterval(timeStr);
                if(policy == TemporalPolicy.INSTANT_AFTER_INSTANT  ||
                   policy == TemporalPolicy.INSTANT_BEFORE_INSTANT ||
                   policy == TemporalPolicy.INSTANT_EQUALS_INSTANT) {
                     if(interval == null) {
                         LOG.error("Cannot perform temporal interval based queries on an instant.");
                     }
                 }
                objs.add(getTemporalObject(interval, policy));
            } else {
                final TemporalInstant instant = new TemporalInstantRfc3339(DateTime.parse(timeStr));
                if(policy != TemporalPolicy.INSTANT_AFTER_INSTANT  &&
                   policy != TemporalPolicy.INSTANT_BEFORE_INSTANT &&
                   policy != TemporalPolicy.INSTANT_EQUALS_INSTANT) {
                    LOG.error("Cannot perform temporal instant based queries on an interval.");
                }
                objs.add(getTemporalObject(instant, policy));
            }
        });
        return objs.toArray(new Document[]{});
    }

    private Document getGeoObject (final Geometry geo, final GeoPolicy policy) throws GeoTemporalIndexException {
        switch(policy) {
            case CONTAINS:
                throw new UnsupportedOperationException("Contains queries are not supported in Mongo DB.");
            case CROSSES:
                throw new UnsupportedOperationException("Crosses queries are not supported in Mongo DB.");
            case DISJOINT:
                throw new UnsupportedOperationException("Disjoint queries are not supported in Mongo DB.");
            case EQUALS:
                try {
                    return geoStrategy.getQuery(new GeoQuery(EQUALS, geo));
                } catch (final MalformedQueryException e) {
                    throw new GeoTemporalIndexException(e.getMessage(), e);
                }
            case INTERSECTS:
                try {
                    return geoStrategy.getQuery(new GeoQuery(INTERSECTS, geo));
                } catch (final MalformedQueryException e) {
                    throw new GeoTemporalIndexException(e.getMessage(), e);
                }
            case OVERLAPS:
                throw new UnsupportedOperationException("Overlaps queries are not supported in Mongo DB.");
            case TOUCHES:
                throw new UnsupportedOperationException("Touches queries are not supported in Mongo DB.");
            case WITHIN:
                try {
                    return geoStrategy.getQuery(new GeoQuery(WITHIN, geo));
                } catch (final MalformedQueryException e) {
                    throw new GeoTemporalIndexException(e.getMessage(), e);
                }
            default:
                return new Document();
        }
    }

    private Document getTemporalObject(final TemporalInstant instant, final TemporalPolicy policy) {
        Document temporalObj;
        switch(policy) {
            case INSTANT_AFTER_INSTANT:
                temporalObj = QueryBuilder.start(INSTANT)
                       .greaterThan(instant.getAsDateTime().toDate())
                       .get();
                break;
            case INSTANT_BEFORE_INSTANT:
                temporalObj = QueryBuilder.start(INSTANT)
                       .lessThan(instant.getAsDateTime().toDate())
                       .get();
                break;
            case INSTANT_EQUALS_INSTANT:
                temporalObj = QueryBuilder.start(INSTANT)
                       .is(instant.getAsDateTime().toDate())
                       .get();
                break;
             default:
                 temporalObj = new Document();
        }
        return temporalObj;
    }

    private Document getTemporalObject(final TemporalInterval interval, final TemporalPolicy policy) {
        final Document temporalObj;
        switch(policy) {
            case INSTANT_AFTER_INTERVAL:
                temporalObj = QueryBuilder.start(INSTANT)
                       .greaterThan(interval.getHasEnd().getAsDateTime().toDate())
                       .get();
                break;
            case INSTANT_BEFORE_INTERVAL:
                temporalObj = QueryBuilder.start(INSTANT)
                       .lessThan(interval.getHasBeginning().getAsDateTime().toDate())
                       .get();
                break;
            case INSTANT_END_INTERVAL:
                temporalObj = QueryBuilder.start(INSTANT)
                       .is(interval.getHasEnd().getAsDateTime().toDate())
                       .get();
                break;
            case INSTANT_IN_INTERVAL:
                temporalObj = QueryBuilder.start(INSTANT)
                       .greaterThan(interval.getHasBeginning().getAsDateTime().toDate())
                       .lessThan(interval.getHasEnd().getAsDateTime().toDate())
                       .get();
                break;
            case INSTANT_START_INTERVAL:
                temporalObj = QueryBuilder.start(INSTANT)
                       .is(interval.getHasBeginning().getAsDateTime().toDate())
                       .get();
                break;
            case INTERVAL_AFTER:
                temporalObj = QueryBuilder.start(INTERVAL_START)
                       .greaterThan(interval.getHasEnd().getAsDateTime().toDate())
                       .get();
                break;
            case INTERVAL_BEFORE:
                temporalObj = QueryBuilder.start(INTERVAL_END)
                       .lessThan(interval.getHasBeginning().getAsDateTime().toDate())
                       .get();
                break;
            case INTERVAL_EQUALS:
                temporalObj = QueryBuilder.start(INTERVAL_START)
                       .is(interval.getHasBeginning().getAsDateTime().toDate())
                       .and(INTERVAL_END)
                       .is(interval.getHasEnd().getAsDateTime().toDate())
                       .get();
                break;
             default:
                 temporalObj = new Document();
        }
        return temporalObj;
    }
}
