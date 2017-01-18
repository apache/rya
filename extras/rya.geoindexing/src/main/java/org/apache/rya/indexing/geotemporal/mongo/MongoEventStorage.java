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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.indexing.IndexingExpr;
import org.apache.rya.indexing.entity.model.TypedEntity;
import org.apache.rya.indexing.entity.storage.mongo.DocumentConverter.DocumentConverterException;
import org.apache.rya.indexing.geotemporal.GeoTemporalIndexException;
import org.apache.rya.indexing.geotemporal.model.Event;
import org.apache.rya.indexing.geotemporal.storage.EventStorage;
import org.apache.rya.indexing.entity.storage.mongo.MongoEntityStorage;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.mongodb.ErrorCategory;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;

public class MongoEventStorage implements EventStorage {

    protected static final String COLLECTION_NAME = "geotemporal-events";

    private static final EventDocumentConverter EVENT_CONVERTER = new EventDocumentConverter();

    /**
     * A client connected to the Mongo instance that hosts the Rya instance.
     */
    protected final MongoClient mongo;

    /**
     * The name of the Rya instance the {@link TypedEntity}s are for.
     */
    protected final String ryaInstanceName;

    /*
     * Used to get the filter query objects.
     */
    private final GeoTemporalMongoDBStorageStrategy queryAdapter;

    /**
     * Constructs an instance of {@link MongoEntityStorage}.
     *
     * @param mongo - A client connected to the Mongo instance that hosts the Rya instance. (not null)
     * @param ryaInstanceName - The name of the Rya instance the {@link TypedEntity}s are for. (not null)
     */
    public MongoEventStorage(final MongoClient mongo, final String ryaInstanceName) {
        this.mongo = requireNonNull(mongo);
        this.ryaInstanceName = requireNonNull(ryaInstanceName);
        queryAdapter = new GeoTemporalMongoDBStorageStrategy();
    }

    @Override
    public void create(final Event event) throws EventStorageException {
        requireNonNull(event);

        try {
            mongo.getDatabase(ryaInstanceName)
                .getCollection(COLLECTION_NAME)
                .insertOne(EVENT_CONVERTER.toDocument(event));
        } catch(final MongoException e) {
            final ErrorCategory category = ErrorCategory.fromErrorCode( e.getCode() );
            if(category == ErrorCategory.DUPLICATE_KEY) {
                throw new EventAlreadyExistsException("Failed to create Event with Subject '" + event.getSubject().getData() + "'.", e);
            }
            throw new EventStorageException("Failed to create Event with Subject '" + event.getSubject().getData() + "'.", e);
        }
    }

    @Override
    public Optional<Event> get(final RyaURI subject) throws EventStorageException {
        requireNonNull(subject);

        try {
            final Document document = mongo.getDatabase(ryaInstanceName)
                .getCollection(COLLECTION_NAME)
                .find( new BsonDocument(EventDocumentConverter.SUBJECT, new BsonString(subject.getData())) )
                .first();

            return document == null ?
                    Optional.empty() :
                    Optional.of( EVENT_CONVERTER.fromDocument(document) );

        } catch(final MongoException | DocumentConverterException e) {
            throw new EventStorageException("Could not get the Event with Subject '" + subject.getData() + "'.", e);
        }
    }

    @Override
    public Collection<Event> search(final Optional<RyaURI> subject, final Optional<Collection<IndexingExpr>> geoFilters, final Optional<Collection<IndexingExpr>> temporalFilters) throws EventStorageException {
        requireNonNull(subject);

        try {
            final Collection<IndexingExpr> geos = (geoFilters.isPresent() ? geoFilters.get() : new ArrayList<>());
            final Collection<IndexingExpr> tempos = (temporalFilters.isPresent() ? temporalFilters.get() : new ArrayList<>());
            final DBObject filterObj = queryAdapter.getFilterQuery(geos, tempos);

            final BasicDBObjectBuilder builder = BasicDBObjectBuilder
            .start(filterObj.toMap());
            if(subject.isPresent()) {
                builder.append(EventDocumentConverter.SUBJECT, subject.get().getData());
            }
            final MongoCursor<Document> results = mongo.getDatabase(ryaInstanceName)
                .getCollection(COLLECTION_NAME)
                .find( BsonDocument.parse(builder.get().toString()) )
                .iterator();

            final List<Event> events = new ArrayList<>();
            final EventDocumentConverter adapter = new EventDocumentConverter();
            while(results.hasNext()) {
                events.add(adapter.fromDocument(results.next()));
            }
            return events;
        } catch(final MongoException | DocumentConverterException | GeoTemporalIndexException e) {
            throw new EventStorageException("Could not get the Event.", e);
        }
    }

    @Override
    public void update(final Event old, final Event updated) throws StaleUpdateException, EventStorageException {
        requireNonNull(old);
        requireNonNull(updated);

        // The updated entity must have the same Subject as the one it is replacing.
        if(!old.getSubject().equals(updated.getSubject())) {
            throw new EventStorageException("The old Event and the updated Event must have the same Subject. " +
                    "Old Subject: " + old.getSubject().getData() + ", Updated Subject: " + updated.getSubject().getData());
        }

        final Set<Bson> filters = new HashSet<>();

        // Must match the old entity's Subject.
        filters.add( makeSubjectFilter(old.getSubject()) );

        // Do a find and replace.
        final Bson oldEntityFilter = Filters.and(filters);
        final Document updatedDoc = EVENT_CONVERTER.toDocument(updated);

        final MongoCollection<Document> collection = mongo.getDatabase(ryaInstanceName).getCollection(COLLECTION_NAME);
        if(collection.findOneAndReplace(oldEntityFilter, updatedDoc) == null) {
            throw new StaleUpdateException("Could not update the Event with Subject '" + updated.getSubject().getData() + ".");
        }
    }

    @Override
    public boolean delete(final RyaURI subject) throws EventStorageException {
        requireNonNull(subject);

        try {
            final Document deleted = mongo.getDatabase(ryaInstanceName)
                .getCollection(COLLECTION_NAME)
                .findOneAndDelete( makeSubjectFilter(subject) );

            return deleted != null;

        } catch(final MongoException e) {
            throw new EventStorageException("Could not delete the Event with Subject '" + subject.getData() + "'.", e);
        }
    }

    private static Bson makeSubjectFilter(final RyaURI subject) {
        return Filters.eq(EventDocumentConverter.SUBJECT, subject.getData());
    }
}
