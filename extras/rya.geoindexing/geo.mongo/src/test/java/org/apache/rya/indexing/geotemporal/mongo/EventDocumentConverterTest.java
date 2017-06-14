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

import static org.junit.Assert.assertEquals;

import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.indexing.TemporalInstant;
import org.apache.rya.indexing.TemporalInstantRfc3339;
import org.apache.rya.indexing.entity.storage.mongo.DocumentConverter.DocumentConverterException;
import org.apache.rya.indexing.geotemporal.model.Event;
import org.apache.rya.indexing.geotemporal.mongo.EventDocumentConverter;
import org.bson.Document;
import org.joda.time.DateTime;
import org.junit.Test;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;

/**
 * Tests the methods of {@link EventDocumentConverter}.
 */
public class EventDocumentConverterTest {
    private static final GeometryFactory GF = new GeometryFactory(new PrecisionModel(), 4326);

    @Test
    public void to_and_from_document() throws DocumentConverterException {
        final Geometry geo = GF.createPoint(new Coordinate(10, 10));
        final TemporalInstant instant = new TemporalInstantRfc3339(DateTime.now());

        // An Event that will be stored.
        final Event event = Event.builder()
                .setSubject(new RyaURI("urn:event/001"))
                .setGeometry(geo)
                .setTemporalInstant(instant)
                .build();

        final Document document = new EventDocumentConverter().toDocument(event);

        // Convert the Document back into an Event.
        final Event converted = new EventDocumentConverter().fromDocument(document);

        // Ensure the original matches the round trip converted Event.
        assertEquals(event, converted);
    }
}