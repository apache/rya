package org.apache.rya.indexing.mongodb.geo;

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

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.resolver.RyaToRdfConversions;
import org.apache.rya.indexing.accumulo.geo.GeoParseUtils;
import org.apache.rya.indexing.mongodb.IndexingMongoDBStorageStrategy;
import org.openrdf.model.Statement;
import org.openrdf.query.MalformedQueryException;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

public class GeoMongoDBStorageStrategy extends IndexingMongoDBStorageStrategy {
    private static final Logger LOG = Logger.getLogger(GeoMongoDBStorageStrategy.class);

    private static final String GEO = "location";
    public enum GeoQueryType {
        INTERSECTS {
            @Override
            public String getKeyword() {
                return "$geoIntersects";
            }
        }, WITHIN {
            @Override
            public String getKeyword() {
                return "$geoWithin";
            }
        },
        EQUALS {
            @Override
            public String getKeyword() {
                return "$near";
            }
        };

        public abstract String getKeyword();
    }

    public static class GeoQuery {
        private final GeoQueryType queryType;
        private final Geometry geo;

        public GeoQuery(final GeoQueryType queryType, final Geometry geo) {
            this.queryType = queryType;
            this.geo = geo;
        }

        public GeoQueryType getQueryType() {
            return queryType;
        }
        public Geometry getGeo() {
            return geo;
        }
    }

    private final double maxDistance;

    public GeoMongoDBStorageStrategy(final double maxDistance) {
        this.maxDistance = maxDistance;
    }

    @Override
    public void createIndices(final DBCollection coll){
        coll.createIndex("{" + GEO + " : \"2dsphere\"" );
    }

    public DBObject getQuery(final GeoQuery queryObj) throws MalformedQueryException {
        final Geometry geo = queryObj.getGeo();
        final GeoQueryType queryType = queryObj.getQueryType();
        if(queryType != GeoQueryType.EQUALS && !(geo instanceof Polygon)) {
            //They can also be applied to MultiPolygons, but those are not supported either.
            throw new MalformedQueryException("Mongo Within operations can only be performed on Polygons.");
        }

        BasicDBObject query;
        if (queryType.equals(GeoQueryType.EQUALS)){
            final List<double[]> points = getCorrespondingPoints(geo);
            if (points.size() == 1){
                final List circle = new ArrayList();
                circle.add(points.get(0));
                circle.add(maxDistance);
                final BasicDBObject polygon = new BasicDBObject("$centerSphere", circle);
                query = new BasicDBObject(GEO,  new BasicDBObject(GeoQueryType.WITHIN.getKeyword(), polygon));
            } else {
                query = new BasicDBObject(GEO, points);
            }
        } else {
            query = new BasicDBObject(GEO, new BasicDBObject(queryType.getKeyword(), new BasicDBObject("$polygon", getCorrespondingPoints(geo))));
        }

        return query;
    }

    @Override
    public DBObject serialize(final RyaStatement ryaStatement) {
        // if the object is wkt, then try to index it
        // write the statement data to the fields
        try {
            final Statement statement = RyaToRdfConversions.convertStatement(ryaStatement);
            final Geometry geo = (new WKTReader()).read(GeoParseUtils.getWellKnownText(statement));
            final BasicDBObject base = (BasicDBObject) super.serialize(ryaStatement);
            base.append(GEO, getCorrespondingPoints(geo));
            return base;
        } catch(final ParseException e) {
            LOG.error("Could not create geometry for statement " + ryaStatement, e);
            return null;
        }
    }

    public List<double[]> getCorrespondingPoints(final Geometry geo){
       final List<double[]> points = new ArrayList<double[]>();
        for (final Coordinate coord : geo.getCoordinates()){
            points.add(new double[] {
                coord.x, coord.y
            });
        }
        return points;
    }
}