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
package org.apache.rya.mongodb.iter;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.persist.RyaDAOException;
import org.openrdf.query.BindingSet;

import com.google.common.base.Throwables;

import info.aduna.iteration.CloseableIteration;

public class RyaStatementCursorIterator implements Iterator<RyaStatement>, CloseableIteration<RyaStatement, RyaDAOException> {
    private final CloseableIteration<? extends Entry<RyaStatement, BindingSet>, RyaDAOException> iterator;

    public RyaStatementCursorIterator(CloseableIteration<? extends Entry<RyaStatement, BindingSet>, RyaDAOException> iterator) {
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
        try {
            return iterator.hasNext();
        } catch (RyaDAOException e) {
            Throwables.propagate(e);
        }
        return false;
    }

    @Override
    public RyaStatement next() {
        try {
            return iterator.next().getKey();
        } catch (RyaDAOException e) {
            Throwables.propagate(e);
        }
        return null;
    }

    @Override
    public void close() throws RyaDAOException {
        iterator.close();
    }

    @Override
    public void remove() {
        try {
            iterator.remove();
        } catch (RyaDAOException e) {
            Throwables.propagate(e);
        }
    }
}
