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

import static java.util.Objects.requireNonNull;

import java.util.Date;
import java.util.List;

import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.indexing.TemporalInstant;
import org.apache.rya.indexing.TemporalInstantRfc3339;
import org.apache.rya.indexing.TemporalInterval;
import org.apache.rya.indexing.entity.storage.mongo.DocumentConverter;
import org.apache.rya.indexing.geotemporal.model.Event;
import org.apache.rya.indexing.mongodb.geo.GeoMongoDBStorageStrategy;
import org.bson.Document;
import org.joda.time.DateTime;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateList;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;

public class EventDocumentConverter implements DocumentConverter<Event>{
    public static final String SUBJECT = "_id";
    public static final String GEO_KEY = "location";
    public static final String INTERVAL_START = "start";
    public static final String INTERVAL_END = "end";
    public static final String INSTANT = "instant";

    private final GeoMongoDBStorageStrategy geoAdapter = new GeoMongoDBStorageStrategy(0.0);

    @Override
    public Document toDocument(final Event event) {
        requireNonNull(event);

        final Document doc = new Document();
        doc.append(SUBJECT, event.getSubject().getData());

        if(event.getGeometry().isPresent()) {
            if (event.getGeometry().get().getNumPoints() > 1) {
                doc.append(GEO_KEY, geoAdapter.getCorrespondingPoints(event.getGeometry().get()));
            } else {
                doc.append(GEO_KEY, geoAdapter.getDBPoint(event.getGeometry().get()));
            }
        }
        if(event.isInstant()) {
            if(event.getInstant().isPresent()) {
                doc.append(INSTANT, event.getInstant().get().getAsDateTime().toDate());
            }
        } else {
            if(event.getInterval().isPresent()) {
                doc.append(INTERVAL_START, event.getInterval().get().getHasBeginning().getAsDateTime().toDate());
                doc.append(INTERVAL_END, event.getInterval().get().getHasEnd().getAsDateTime().toDate());
            }
        }

        return doc;
    }

    @Override
    public Event fromDocument(final Document document) throws DocumentConverterException {
        requireNonNull(document);

        final boolean isInstant;

        // Preconditions.
        if(!document.containsKey(SUBJECT)) {
            throw new DocumentConverterException("Could not convert document '" + document +
                    "' because its '" + SUBJECT + "' field is missing.");
        }

        if(document.containsKey(INSTANT)) {
            isInstant = true;
        } else {
            isInstant = false;
        }

        final String subject = document.getString(SUBJECT);

        final Event.Builder builder = new Event.Builder()
            .setSubject(new RyaURI(subject));

        if(document.containsKey(GEO_KEY)) {
            final Document geoObj = (Document) document.get(GEO_KEY);
            final GeometryFactory geoFact = new GeometryFactory();
            final String typeString = (String) geoObj.get("type");
            final CoordinateList coords = new CoordinateList();
            final Geometry geo;
            if (typeString.equals("Point")) {
                final List<Double> point = (List<Double>) geoObj.get("coordinates");
                final Coordinate coord = new Coordinate(point.get(0), point.get(1));
                geo = geoFact.createPoint(coord);
            } else if (typeString.equals("LineString")) {
                final List<List<Double>> pointsList = (List<List<Double>>) geoObj.get("coordinates");
                for (final List<Double> point : pointsList) {
                    coords.add(new Coordinate(point.get(0), point.get(1)));
                }
                geo = geoFact.createLineString(coords.toCoordinateArray());
            } else {
                final List<List<List<Double>>> pointsList = (List<List<List<Double>>>) geoObj.get("coordinates");
                if(pointsList.size() == 1) {
                    final List<List<Double>> poly = pointsList.get(0);
                    for (final List<Double> point : poly) {
                        coords.add(new Coordinate(point.get(0), point.get(1)));
                    }
                    geo = geoFact.createPolygon(coords.toCoordinateArray());
                } else {
                    final List<List<Double>> first = pointsList.get(0);
                    final CoordinateList shellCoords = new CoordinateList();
                    for (final List<Double> point : pointsList.get(0)) {
                        shellCoords.add(new Coordinate(point.get(0), point.get(1)));
                    }
                    final LinearRing shell = geoFact.createLinearRing(shellCoords.toCoordinateArray());

                    final List<List<List<Double>>> holesPoints = pointsList.subList(1, pointsList.size() - 1);
                    final LinearRing[] holes = new LinearRing[holesPoints.size()];
                    for(int ii = 0; ii < holes.length; ii++) {
                        final List<List<Double>> holePoints = holesPoints.get(ii);
                        final CoordinateList shells = new CoordinateList();
                        for (final List<Double> point : pointsList.get(0)) {
                            shells.add(new Coordinate(point.get(0), point.get(1)));
                        }
                        holes[ii] = geoFact.createLinearRing(shells.toCoordinateArray());
                    }
                    geo = geoFact.createPolygon(shell,
                            holes);
                }
            }
            builder.setGeometry(geo);
        }

        if(isInstant) {
            //we already know the key exists
            final Date date = (Date) document.get(INSTANT);
            final DateTime dt = new DateTime(date.getTime());
            final TemporalInstant instant = new TemporalInstantRfc3339(dt);
            builder.setTemporalInstant(instant);
        } else if(document.containsKey(INTERVAL_START)){
            Date date = (Date) document.get(INTERVAL_START);
            DateTime dt = new DateTime(date.getTime());
            final TemporalInstant begining = new TemporalInstantRfc3339(dt);

            date = (Date) document.get(INTERVAL_END);
            dt = new DateTime(date.getTime());
            final TemporalInstant end = new TemporalInstantRfc3339(dt);

            final TemporalInterval interval = new TemporalInterval(begining, end);
            builder.setTemporalInterval(interval);
        }
        return builder.build();
    }
}
