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

import java.util.Objects;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * An ordered entry within a change log.
 * </p>
 * Immutable.
 *
 * @param <T> - The type of entry that is in the change log.
 */
@DefaultAnnotation(NonNull.class)
public class ChangeLogEntry<T> {

    private final int position;
    private final T entry;

    /**
     * Constructs an instance of {@link ChangeLogEntry}.
     *
     * @param position - The position of this entry within the change log.
     * @param entry - The value that is stored at this position within the change log. (not null)
     */
    public ChangeLogEntry(final int position, final T entry) {
        this.position = position;
        this.entry = requireNonNull(entry);
    }

    /**
     * @return The position of this entry within the change log.
     */
    public int getPosition() {
        return position;
    }

    /**
     * @return The value that is stored at this position within the change log. (not null)
     */
    public T getEntry() {
        return entry;
    }

    @Override
    public int hashCode() {
        return Objects.hash(position, entry);
    }

    @Override
    public boolean equals(final Object o) {
        if(o instanceof ChangeLogEntry) {
            final ChangeLogEntry<T> other = (ChangeLogEntry<T>) o;
            return position == other.position &&
                    Objects.equals(entry, other.entry);
        }
        return false;
    }
}