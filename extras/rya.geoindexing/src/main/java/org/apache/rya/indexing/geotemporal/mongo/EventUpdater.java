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

import java.util.Optional;

import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.indexing.geotemporal.model.Event;
import org.apache.rya.indexing.geotemporal.storage.EventStorage;
import org.apache.rya.indexing.geotemporal.storage.EventStorage.EventStorageException;
import org.apache.rya.indexing.mongodb.update.MongoDocumentUpdater;
import org.apache.rya.indexing.mongodb.update.RyaObjectStorage.ObjectStorageException;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Performs update operations over an {@link EventStorage}.
 */
@DefaultAnnotation(NonNull.class)
public class EventUpdater implements MongoDocumentUpdater<RyaURI, Event>{
    private final EventStorage events;

    /**
     * Constructs an instance of {@link EventUpdater}
     *
     * @param events - The storage this updater operates over. (not null)
     */
    public EventUpdater(final EventStorage events) {
        this.events = requireNonNull(events);
    }

    @Override
    public Optional<Event> getOld(final RyaURI key) throws EventStorageException {
        try {
            return events.get(key);
        } catch (final ObjectStorageException e) {
            throw new EventStorageException(e.getMessage(), e);
        }
    }

    @Override
    public void create(final Event newObj) throws EventStorageException {
        try {
            events.create(newObj);
        } catch (final ObjectStorageException e) {
            throw new EventStorageException(e.getMessage(), e);
        }
    }

    @Override
    public void update(final Event old, final Event updated) throws EventStorageException {
        try {
            events.update(old, updated);
        } catch (final ObjectStorageException e) {
            throw new EventStorageException(e.getMessage(), e);
        }
    }

    public void delete(final Event event) throws EventStorageException {
        try {
            events.delete(event.getSubject());
        } catch (final ObjectStorageException e) {
            throw new EventStorageException(e.getMessage(), e);
        }
    }
}
