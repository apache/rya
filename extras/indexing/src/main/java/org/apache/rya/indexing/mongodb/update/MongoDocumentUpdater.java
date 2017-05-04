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
package org.apache.rya.indexing.mongodb.update;

import static java.util.Objects.requireNonNull;

import java.util.Optional;
import java.util.function.Function;

import org.apache.rya.indexing.mongodb.IndexingException;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Performs an update operation on a Document in mongodb.
 * @param <T> - The key to find the object.
 * @param <V> - The type of object to get updated.
 */
@DefaultAnnotation(NonNull.class)
public interface MongoDocumentUpdater<T, V> {
    public default void update(final T key, final DocumentMutator<V> mutator) throws IndexingException {
        requireNonNull(mutator);

        // Fetch the current state of the Entity.
        boolean completed = false;
        while(!completed) {
            //this cast is safe since the mutator interface is defined below to use Optional<V>
            final Optional<V> old = getOld(key);
            final Optional<V> updated = mutator.apply(old);

            final boolean doWork = updated.isPresent();
            if(doWork) {
                if(!old.isPresent()) {
                    create(updated.get());
                } else {
                    update(old.get(), updated.get());
                }
            }
            completed = true;
        }
    }

    Optional<V> getOld(T key) throws IndexingException;

    void create(final V newObj) throws IndexingException;

    void update(final V old, final V updated) throws IndexingException;

    /**
     * Implementations of this interface are used to update the state of a
     * {@link MongoDocumentUpdater#V} in unison with a {@link MongoDocumentUpdater}.
     * </p>
     * This table describes what the updater will do depending on if the object
     * exists and if an updated object is returned.
     * </p>
     * <table border="1px">
     *     <tr><th>Object Provided</th><th>Update Returned</th><th>Effect</th></tr>
     *     <tr>
     *         <td>true</td>
     *         <td>true</td>
     *         <td>The old Object will be updated using the returned state.</td>
     *     </tr>
     *     <tr>
     *         <td>true</td>
     *         <td>false</td>
     *         <td>No work is performed.</td>
     *     </tr>
     *     <tr>
     *         <td>false</td>
     *         <td>true</td>
     *         <td>A new Object will be created using the returned state.</td>
     *     </tr>
     *     <tr>
     *         <td>false</td>
     *         <td>false</td>
     *         <td>No work is performed.</td>
     *     </tr>
     * </table>
     */
    public interface DocumentMutator<V> extends Function<Optional<V>, Optional<V>> { }
}