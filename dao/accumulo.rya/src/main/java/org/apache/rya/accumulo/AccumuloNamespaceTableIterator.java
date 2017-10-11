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
package org.apache.rya.accumulo;

import java.io.IOError;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.rya.api.persist.RdfDAOException;
import org.openrdf.model.Namespace;
import org.openrdf.model.impl.NamespaceImpl;

import com.google.common.base.Preconditions;

import info.aduna.iteration.CloseableIteration;

public class AccumuloNamespaceTableIterator<T extends Namespace> implements
        CloseableIteration<Namespace, RdfDAOException> {

    private boolean open = false;
    private final Iterator<Entry<Key, Value>> result;

    public AccumuloNamespaceTableIterator(final Iterator<Entry<Key, Value>> result) throws RdfDAOException {
        Preconditions.checkNotNull(result);
        open = true;
        this.result = result;
    }

    @Override
    public void close() throws RdfDAOException {
        try {
            verifyIsOpen();
            open = false;
        } catch (final IOError e) {
            throw new RdfDAOException(e);
        }
    }

    public void verifyIsOpen() throws RdfDAOException {
        if (!open) {
            throw new RdfDAOException("Iterator not open");
        }
    }

    @Override
    public boolean hasNext() throws RdfDAOException {
        verifyIsOpen();
        return result != null && result.hasNext();
    }

    @Override
    public Namespace next() throws RdfDAOException {
        if (hasNext()) {
            return getNamespace(result);
        }
        return null;
    }

    public static Namespace getNamespace(final Iterator<Entry<Key, Value>> rowResults) {
        for (; rowResults.hasNext(); ) {
            final Entry<Key, Value> next = rowResults.next();
            final Key key = next.getKey();
            final Value val = next.getValue();
            final String cf = key.getColumnFamily().toString();
            final String cq = key.getColumnQualifier().toString();
            return new NamespaceImpl(key.getRow().toString(),
                    new String(val.get(), StandardCharsets.UTF_8));
        }
        return null;
    }

    @Override
    public void remove() throws RdfDAOException {
        next();
    }

    public boolean isOpen() {
        return open;
    }
}
