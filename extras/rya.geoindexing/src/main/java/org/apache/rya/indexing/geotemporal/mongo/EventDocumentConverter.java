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
import org.bson.types.BasicBSONList;
import org.joda.time.DateTime;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

public class EventDocumentConverter implements DocumentConverter<Event>{
    public static final String SUBJECT = "_id";
    public static final String GEO_KEY = "location";
    public static final String INTERVAL_START = "start";
    public static final String INTERVAL_END = "end";
    public static final String INSTANT = "instant";

    private final GeoMongoDBStorageStrategy geoAdapter = new GeoMongoDBStorageStrategy(0);

    @Override
    public Document toDocument(final Event event) {
        requireNonNull(event);

        final Document doc = new Document();
        doc.append(SUBJECT, event.getSubject().getData());

        if(event.getGeometry().isPresent()) {
            final BasicBSONList points = new BasicBSONList();
            for(final double[] point : geoAdapter.getCorrespondingPoints(event.getGeometry().get())) {
                final BasicBSONList pointDoc = new BasicBSONList();
                for(final double p : point) {
                    pointDoc.add(p);
                }
                points.add(pointDoc);
            }

            doc.append(GEO_KEY, points);
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
            final List<List<Double>> pointsList = (List<List<Double>>) document.get(GEO_KEY);
            final Coordinate[] coords = new Coordinate[pointsList.size()];

            int ii = 0;
            for(final List<Double> point : pointsList) {
                coords[ii] = new Coordinate(point.get(0), point.get(1));
                ii++;
            }

            final GeometryFactory geoFact = new GeometryFactory();
            final Geometry geo;
            if(coords.length == 1) {
                geo = geoFact.createPoint(coords[0]);
            } else {
                geo = geoFact.createPolygon(coords);
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
