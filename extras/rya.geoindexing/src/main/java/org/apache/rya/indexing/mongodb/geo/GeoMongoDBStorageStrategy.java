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
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;
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
        }, EQUALS {
            @Override
            public String getKeyword() {
                return "$near";
            }
        }, NEAR {
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

        private final double maxDistance;
        private final double minDistance;

        public GeoQuery(final GeoQueryType queryType, final Geometry geo) {
            this(queryType, geo, 0, 0);
        }

        public GeoQuery(final GeoQueryType queryType, final Geometry geo, final double maxDistance, final double minDistance) {
            this.queryType = queryType;
            this.geo = geo;
            this.maxDistance = maxDistance;
            this.minDistance = minDistance;
        }

        public GeoQueryType getQueryType() {
            return queryType;
        }

        public Geometry getGeo() {
            return geo;
        }

        public double getMaxDistance() {
            return maxDistance;
        }

        public double getMinDistance() {
            return minDistance;
        }
    }

    private final double maxDistance;

    public GeoMongoDBStorageStrategy(final double maxDistance) {
        this.maxDistance = maxDistance;
    }

    @Override
    public void createIndices(final DBCollection coll){
        coll.createIndex(new BasicDBObject(GEO, "2dsphere"));
    }

    public DBObject getQuery(final GeoQuery queryObj) throws MalformedQueryException {
        final Geometry geo = queryObj.getGeo();
        final GeoQueryType queryType = queryObj.getQueryType();
        if (queryType == GeoQueryType.WITHIN && !(geo instanceof Polygon)) {
            //They can also be applied to MultiPolygons, but those are not supported either.
            throw new MalformedQueryException("Mongo Within operations can only be performed on Polygons.");
        } else if(queryType == GeoQueryType.NEAR && !(geo instanceof Point)) {
            //They can also be applied to Point, but those are not supported either.
            throw new MalformedQueryException("Mongo near operations can only be performed on Points.");
        }

        BasicDBObject query;
        if (queryType.equals(GeoQueryType.EQUALS)){
            if(geo.getNumPoints() == 1) {
                final List circle = new ArrayList();
                circle.add(getPoint(geo));
                circle.add(maxDistance);
                final BasicDBObject polygon = new BasicDBObject("$centerSphere", circle);
                query = new BasicDBObject(GEO,  new BasicDBObject(GeoQueryType.WITHIN.getKeyword(), polygon));
            } else {
                query = new BasicDBObject(GEO, getCorrespondingPoints(geo));
            }
        } else if(queryType.equals(GeoQueryType.NEAR)) {
            final BasicDBObject geoDoc = new BasicDBObject("$geometry", getDBPoint(geo));
            if(queryObj.getMaxDistance() != 0) {
                geoDoc.append("$maxDistance", queryObj.getMaxDistance());
            }

            if(queryObj.getMinDistance() != 0) {
                geoDoc.append("$minDistance", queryObj.getMinDistance());
            }
            query = new BasicDBObject(GEO, new BasicDBObject(queryType.getKeyword(), geoDoc));
        } else {
            final BasicDBObject geoDoc = new BasicDBObject("$geometry", getCorrespondingPoints(geo));
            query = new BasicDBObject(GEO, new BasicDBObject(queryType.getKeyword(), geoDoc));
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
            if(geo == null) {
                LOG.error("Failed to parse geo statement: " + statement.toString());
                return null;
            }
            final BasicDBObject base = (BasicDBObject) super.serialize(ryaStatement);
            if (geo.getNumPoints() > 1) {
                base.append(GEO, getCorrespondingPoints(geo));
            } else {
                base.append(GEO, getDBPoint(geo));
            }
            return base;
        } catch(final ParseException e) {
            LOG.error("Could not create geometry for statement " + ryaStatement, e);
            return null;
        }
    }

    public BasicDBObject getCorrespondingPoints(final Geometry geo) {
        //Polygons must be a 3 dimensional array.

        //polygons must be a closed loop
        final BasicDBObjectBuilder geoDocBuilder = new BasicDBObjectBuilder();
        if (geo instanceof Polygon) {
            final Polygon poly = (Polygon) geo;
            final List<List<double[]>> DBpoints = new ArrayList<List<double[]>>();

            // outer shell of the polygon
            final List<double[]> ring = new ArrayList<>();
            for (final Coordinate coord : poly.getExteriorRing().getCoordinates()) {
                ring.add(new double[] { coord.x, coord.y });
            }
            DBpoints.add(ring);

            // each hold in the polygon
            for (int ii = 0; ii < poly.getNumInteriorRing(); ii++) {
                final List<double[]> holeCoords = new ArrayList<>();
                for (final Coordinate coord : poly.getInteriorRingN(ii).getCoordinates()) {
                    holeCoords.add(new double[] { coord.x, coord.y });
                }
                DBpoints.add(holeCoords);
            }
            geoDocBuilder.append("coordinates", DBpoints)
                         .append("type", "Polygon");
        } else {
            final List<double[]> points = getPoints(geo);
            geoDocBuilder.append("coordinates", points)
                         .append("type", "LineString");
        }
        return (BasicDBObject) geoDocBuilder.get();
    }

    private List<double[]> getPoints(final Geometry geo) {
        final List<double[]> points = new ArrayList<double[]>();
        for (final Coordinate coord : geo.getCoordinates()) {
            points.add(new double[] { coord.x, coord.y });
        }
        return points;
    }

    private BasicDBObject getDBPoint(final Geometry geo) {
        return (BasicDBObject) new BasicDBObjectBuilder()
            .append("coordinates", getPoint(geo))
            .append("type", "Point")
            .get();
    }

    private double[] getPoint(final Geometry geo) {
        return new double[] {
                geo.getCoordinate().x,
                geo.getCoordinate().y
        };
    }
}