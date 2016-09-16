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
package mvm.rya.indexing.entity.storage.mongo;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.function.Function;

import org.bson.Document;

import com.mongodb.client.MongoCursor;

import mvm.rya.indexing.entity.storage.CloseableIterator;

/**
 * Converts the {@link Document}s that are returned by a {@link MongoCursor}
 * using a {@link DocumentConverter} every time {@link #next()} is invoked.
 *
 * @param <T> - The type of object the Documents are converted into.
 */
public class ConvertingCursor<T> implements CloseableIterator<T> {

    private final Converter<T> converter;
    private final MongoCursor<Document> cursor;

    /**
     * Constructs an instance of {@link ConvertingCursor}.
     *
     * @param converter - Converts the {@link Document}s returned by {@code cursor}. (not null)
     * @param cursor - Retrieves the {@link Document}s from a Mongo DB instance. (not null)
     */
    public ConvertingCursor(Converter<T> converter, MongoCursor<Document> cursor) {
        this.converter = requireNonNull(converter);
        this.cursor = requireNonNull(cursor);
    }

    @Override
    public boolean hasNext() {
        return cursor.hasNext();
    }

    @Override
    public T next() {
        return converter.apply( cursor.next() );
    }

    @Override
    public void close() throws IOException {
        cursor.close();
    }

    /**
     * Converts a {@link Document} into some other object.
     *
     * @param <R> The type of object the Document is converted into.
     */
    @FunctionalInterface
    public static interface Converter<R> extends Function<Document, R> { }
}