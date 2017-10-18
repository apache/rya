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
package org.apache.rya.streams.api.queries;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;

import com.google.common.collect.Lists;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * An in memory implementation of {@link QueryChangeLog}. Anything that is stored in this change log will be
 * lost when it is cleaned up, so it is not appropriate for production environments.
 * </p>
 * Thread safe.
 */
@DefaultAnnotation(NonNull.class)
public class InMemoryQueryChangeLog implements QueryChangeLog {

    private final ReentrantLock lock = new ReentrantLock();
    private final List<ChangeLogEntry<QueryChange>> entries = new ArrayList<>();

    @Override
    public void write(final QueryChange newChange) throws QueryChangeLogException {
        requireNonNull(newChange);

        lock.lock();
        try {
            final int position = entries.size();
            entries.add( new ChangeLogEntry<>(position, newChange) );
        } finally {
            lock.unlock();
        }
    }

    @Override
    public CloseableIteration<ChangeLogEntry<QueryChange>, QueryChangeLogException> readFromStart() throws QueryChangeLogException {
        return readFromPosition(0);
    }

    @Override
    public CloseableIteration<ChangeLogEntry<QueryChange>, QueryChangeLogException> readFromPosition(final long position) throws QueryChangeLogException {
        lock.lock();
        try {
            return new ListIteration<>(entries.subList((int) position, entries.size()));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws Exception {
        // Nothing to do here.
    }

    /**
     * A {@link CloseableIteration} that iterates over a list.
     *
     * @param <T> - The type of object iterated over.
     * @param <E> - The type of exception that may be thrown.
     */
    @DefaultAnnotation(NonNull.class)
    private static class ListIteration <T, E extends Exception> implements CloseableIteration<T, E> {

        private final Iterator<T> it;

        public ListIteration(final List<T> list) {
            it = Lists.newArrayList( list ).iterator();
        }

        @Override
        public boolean hasNext() throws E {
            return it.hasNext();
        }

        @Override
        public T next() throws E {
            return it.next();
        }

        @Override
        public void remove() throws E { }

        @Override
        public void close() throws E { }
    }
}