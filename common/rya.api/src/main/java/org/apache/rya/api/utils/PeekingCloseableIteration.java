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



import com.google.common.base.Preconditions;
import info.aduna.iteration.CloseableIteration;

/**
 * Date: 7/24/12
 * Time: 4:40 PM
 */
public class PeekingCloseableIteration<E, X extends java.lang.Exception> implements CloseableIteration<E, X> {

    private final CloseableIteration<E, X> iteration;
    private boolean hasPeeked;
    private E peekedElement;

    public PeekingCloseableIteration(CloseableIteration<E, X> iteration) {
        this.iteration = Preconditions.checkNotNull(iteration);
    }

    @Override
    public void close() throws X {
        iteration.close();
    }

    public boolean hasNext() throws X {
        return hasPeeked || iteration.hasNext();
    }

    public E next() throws X {
        if (!hasPeeked) {
            return iteration.next();
        } else {
            E result = peekedElement;
            hasPeeked = false;
            peekedElement = null;
            return result;
        }
    }

    public void remove() throws X {
        Preconditions.checkState(!hasPeeked, "Can't remove after you've peeked at next");
        iteration.remove();
    }

    public E peek() throws X {
        if (!hasPeeked) {
            peekedElement = iteration.next();
            hasPeeked = true;
        }
        return peekedElement;
    }

}
