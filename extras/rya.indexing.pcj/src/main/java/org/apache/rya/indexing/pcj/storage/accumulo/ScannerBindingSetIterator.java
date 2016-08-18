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
package org.apache.rya.indexing.pcj.storage.accumulo;

import static java.util.Objects.requireNonNull;

import java.util.Iterator;
import java.util.Map.Entry;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.rya.indexing.pcj.storage.accumulo.BindingSetConverter.BindingSetConversionException;
import org.openrdf.query.BindingSet;

/**
 * Iterates over the results of a {@link Scanner} assuming the results are
 * binding sets that can be converted using a {@link AccumuloPcjSerializer}.
 */
@ParametersAreNonnullByDefault
public class ScannerBindingSetIterator implements Iterator<BindingSet> {

    private static final AccumuloPcjSerializer converter = new AccumuloPcjSerializer();

    private final Iterator<Entry<Key, Value>> accEntries;
    private final VariableOrder varOrder;

    /**
     * Constructs an instance of {@link ScannerBindingSetIterator}.
     *
     * @param scanner - The scanner whose results will be iterated over. (not null)
     * @param varOrder - The variable order of the binding sets the scanner returns. (not null)
     */
    public ScannerBindingSetIterator(final Scanner scanner, final VariableOrder varOrder) {
        requireNonNull(scanner);
        this.accEntries = scanner.iterator();
        this.varOrder = requireNonNull(varOrder);
    }

    @Override
    public boolean hasNext() {
        return accEntries.hasNext();
    }

    @Override
    public BindingSet next() {
        final Entry<Key, Value> entry = accEntries.next();
        final byte[] bindingSetBytes = entry.getKey().getRow().getBytes();
        try {
            return converter.convert(bindingSetBytes, varOrder);
        } catch (final BindingSetConversionException e) {
            throw new RuntimeException("Could not deserialize a BindingSet from Accumulo.", e);
        }
    }
}