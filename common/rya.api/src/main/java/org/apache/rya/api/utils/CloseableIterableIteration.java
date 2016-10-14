package org.apache.rya.api.utils;

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



import info.aduna.iteration.CloseableIteration;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.calrissian.mango.collect.CloseableIterable;

/**
 * Date: 1/30/13
 * Time: 2:21 PM
 */
public class CloseableIterableIteration<T, X extends Exception> implements CloseableIteration<T, X> {

    private CloseableIterable<T> closeableIterable;
    private final Iterator<T> iterator;

    private boolean isClosed = false;

    public CloseableIterableIteration(CloseableIterable<T> closeableIterable) {
        this.closeableIterable = closeableIterable;
        iterator = closeableIterable.iterator();
    }

    @Override
    public void close() throws X {
        try {
            isClosed = true;
            closeableIterable.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean hasNext() throws X {
        return iterator.hasNext();
    }

    @Override
    public T next() throws X {
        if (!hasNext() || isClosed) {
            throw new NoSuchElementException();
        }

        return iterator.next();
    }

    @Override
    public void remove() throws X {
        iterator.remove();
    }
}
