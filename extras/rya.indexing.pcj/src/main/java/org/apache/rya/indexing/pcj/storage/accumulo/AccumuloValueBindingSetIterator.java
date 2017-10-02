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
package org.apache.rya.indexing.pcj.storage.accumulo;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.fluo.api.data.Bytes;
import org.apache.rya.indexing.pcj.storage.PeriodicQueryResultStorage;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage.CloseableIterator;
import org.openrdf.query.BindingSet;

/**
 * Implementation of CloseableIterator for retrieving results from a {@link PeriodicQueryResultStorage}
 * table.
 */
public class AccumuloValueBindingSetIterator implements CloseableIterator<BindingSet>{

    private final Scanner scanner;
    private final Iterator<Entry<Key, Value>> iter;
    private final VisibilityBindingSetSerDe bsSerDe = new VisibilityBindingSetSerDe();

    public AccumuloValueBindingSetIterator(Scanner scanner) {
        this.scanner = scanner;
        iter = scanner.iterator();
    }

    @Override
    public boolean hasNext() {
        return iter.hasNext();
    }

    @Override
    public BindingSet next() {
        try {
            return bsSerDe.deserialize(Bytes.of(iter.next().getValue().get())).getBindingSet();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        scanner.close();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}