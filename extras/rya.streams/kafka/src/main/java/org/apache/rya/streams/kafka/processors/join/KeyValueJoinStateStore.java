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
package org.apache.rya.streams.kafka.processors.join;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.api.utils.CloseableIterator;
import org.apache.rya.streams.kafka.processors.ProcessorResult.BinaryResult;
import org.apache.rya.streams.kafka.processors.ProcessorResult.BinaryResult.Side;
import org.openrdf.query.impl.MapBindingSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A {@link KeyValueStore} implementation of {@link JoinStateStore}.
 * </p>
 * This is a key/value store, so we need to store the {@link VisibilityBindingSet}s using keys that allow us to fetch
 * all binding sets that join from a specific side. We use the following pattern to accomplish this:
 * <pre>
 * [side],[joinVar1 value], [joinVar2 value], ..., [joinVarN value]
 * </pre>
 * This will group all binding sets that have been emitted from a specific side and who have the same join variables
 * next to each other within the store. This isn't enough information to fetch that group, though. We must provide a
 * start and end key to bound the range that is fetched back. To accomplish this, we place a start of range marker
 * as the first key for all unique [side]/[join values] groups, and an end of range marker as the last key for each
 * of those groups.
 * </p>
 * The rows follow this pattern:
 * <pre>
 * [side],[joinVar1 value], [joinVar2 value], ..., [joinVarN value]0x00
 * [side],[joinVar1 value], [joinVar2 value], ..., [joinVarN value],[remainingBindingValues]
 * [side],[joinVar1 value], [joinVar2 value], ..., [joinVarN value]0xFF
 * </pre>
 * </p>
 * When an iterator over the results is returned, it skips over the start and end of range markers.
 */
@DefaultAnnotation(NonNull.class)
public class KeyValueJoinStateStore implements JoinStateStore {

    private static final Logger log = LoggerFactory.getLogger(KeyValueJoinStateStore.class);

    /**
     * This is the minimum value in UTF-8 character.
     */
    private static final String START_RANGE_SUFFIX = new String(new byte[] { 0x00 }, Charsets.UTF_8);

    /**
     * This is the maximum value of a UTF-8 character.
     */
    private static final String END_RANGE_SUFFIX = new String(new byte[] { (byte) 0xFF }, Charsets.UTF_8);

    /**
     * Indicates where the end of the join variables occurs.
     */
    private static final String JOIN_VAR_END_MARKER = new String("~!^~".getBytes(Charsets.UTF_8), Charsets.UTF_8);

    /**
     * A default empty value that is stored for a start of range or end of range marker.
     */
    private static final VisibilityBindingSet RANGE_MARKER_VALUE = new VisibilityBindingSet(new MapBindingSet(), "");

    private final KeyValueStore<String, VisibilityBindingSet> store;
    private final String id;
    private final List<String> joinVars;
    private final List<String> allVars;

    /**
     * Constructs an instance of {@link KeyValueJoinStateStore}.
     *
     * @param store - The state store that will be used. (not null)
     * @param id - The ID used for the state store. (not null)
     * @param joinVars - The variables that are used to build grouping keys. (not null)
     * @param allVars - The variables that are used to build full value keys. (not null)
     * @throws IllegalArgumentException Thrown if {@code allVars} does not start with {@code joinVars}.
     */
    public KeyValueJoinStateStore(
            final KeyValueStore<String, VisibilityBindingSet> store,
            final String id,
            final List<String> joinVars,
            final List<String> allVars) throws IllegalArgumentException {
        this.store = requireNonNull(store);
        this.id = requireNonNull(id);
        this.joinVars = requireNonNull(joinVars);
        this.allVars = requireNonNull(allVars);

        for(int i = 0; i < joinVars.size(); i++) {
            if(!joinVars.get(i).equals(allVars.get(i))) {
                throw new IllegalArgumentException("All vars must be lead by the join vars, but it did not. " +
                        "Join Vars: " + joinVars + ", All Vars: " + allVars);
            }
        }
    }

    @Override
    public void store(final BinaryResult result) {
        requireNonNull(result);

        // The join key prefix is an ordered list of values from the binding set that match the join variables.
        // This is a prefix for every row that holds values for a specific set of join variable values.
        final Side side = result.getSide();
        final VisibilityBindingSet bs = result.getResult();

        final String joinKeyPrefix = makeCommaDelimitedValues(side, joinVars, bs, joinVars.size());

        final List<KeyValue<String, VisibilityBindingSet>> values = new ArrayList<>();

        // For each join variable set, we need a start key for scanning.
        final String startKey = joinKeyPrefix + START_RANGE_SUFFIX;
        values.add( new KeyValue<>(startKey, RANGE_MARKER_VALUE) );

        // The actual value that was emitted as a result.
        final String valueKey = makeCommaDelimitedValues(side, allVars, bs, joinVars.size());

        values.add( new KeyValue<>(valueKey, bs) );

        // And the end key for scanning.
        final String endKey = joinKeyPrefix + END_RANGE_SUFFIX;
        values.add( new KeyValue<>(endKey, RANGE_MARKER_VALUE) );

        // Write the pairs to the store.
        log.debug("\nStoring the following values: {}\n", values);
        store.putAll( values );
    }

    @Override
    public CloseableIterator<VisibilityBindingSet> getJoinedValues(final BinaryResult result) {
        requireNonNull(result);

        // Get an iterator over the values that start with the join variables for the other side.
        final Side otherSide = result.getSide() == Side.LEFT ? Side.RIGHT : Side.LEFT;
        final VisibilityBindingSet bs = result.getResult();
        final String joinKeyPrefix = makeCommaDelimitedValues(otherSide, joinVars, bs, joinVars.size());

        final String startKey = joinKeyPrefix + START_RANGE_SUFFIX;
        final String endKey = joinKeyPrefix + END_RANGE_SUFFIX;

        final KeyValueIterator<String, VisibilityBindingSet> rangeIt = store.range(startKey, endKey);

        // Return a CloseableIterator over the range's value fields, skipping the start and end entry.
        return new CloseableIterator<VisibilityBindingSet>() {

            private Optional<VisibilityBindingSet> next = null;

            @Override
            public boolean hasNext() {
                // If the iterator has not been initialized yet, read a value in.
                if(next == null) {
                    next = readNext();
                }

                // Return true if there is a next value, otherwise false.
                return next.isPresent();
            }

            @Override
            public VisibilityBindingSet next() {
                // If the iterator has not been initialized yet, read a value in.
                if(next == null) {
                    next = readNext();
                }

                // It's illegal to call next() when there is no next value.
                if(!next.isPresent()) {
                    throw new IllegalStateException("May not invoke next() when there is nothing left in the Iterator.");
                }

                // Update and return the next value.
                final VisibilityBindingSet ret = next.get();
                log.debug("\nReturning: {}", ret);
                next = readNext();
                return ret;
            }

            private Optional<VisibilityBindingSet> readNext() {
                // Check to see if there's anything left in the iterator.
                if(!rangeIt.hasNext()) {
                    return Optional.empty();
                }

                // Read a candidate key/value pair from the iterator.
                KeyValue<String, VisibilityBindingSet> candidate = rangeIt.next();

                // If we are initializing, then the first thing we must read is a start of range marker.
                if(next == null) {
                    if(!candidate.key.endsWith(START_RANGE_SUFFIX)) {
                        throw new IllegalStateException("The first key encountered must be a start of range key.");
                    }
                    log.debug("Read the start of range markers.\n");

                    // Read a new candidate to skip this one.
                    if(!rangeIt.hasNext()) {
                        throw new IllegalStateException("There must be another entry after the start of range key.");
                    }
                    candidate = rangeIt.next();
                }

                // If that value is an end of range key, then we are finished. Otherwise, return it.
                else if(candidate.key.endsWith(END_RANGE_SUFFIX)) {
                    log.debug("Read the end of range marker.\n");

                    // If there are more messages, that's a problem.
                    if(rangeIt.hasNext()) {
                        throw new IllegalStateException("The end of range marker must be the last key in the iterator.");
                    }

                    return Optional.empty();
                }

                // Otherwise we found a new value.
                return Optional.of( candidate.value );
            }

            @Override
            public void close() throws Exception {
                rangeIt.close();
            }
        };
    }

    /**
     * A utility function that helps construct the keys used by {@link KeyValueJoinStateStore}.
     *
     * @param side - The side value for the key. (not null)
     * @param vars - Which variables within the binding set to use for the key's values. (not null)
     * @param bindingSet - The binding set the key is being constructed from. (not null)
     * @param joinVarSize - the number of join variables at the beginning of
     * {@code vars}.
     * @return A comma delimited list of the binding values, leading with the side.
     */
    private String makeCommaDelimitedValues(final Side side, final List<String> vars, final VisibilityBindingSet bindingSet, final int joinVarSize) {
        requireNonNull(side);
        requireNonNull(vars);
        requireNonNull(bindingSet);

        // Make a an ordered list of the binding set variables.
        final List<String> values = new ArrayList<>();
        values.add(id);
        values.add(side.toString());
        int count = 0;
        for(final String var : vars) {
            count++;
            String value = bindingSet.hasBinding(var) ? bindingSet.getBinding(var).getValue().toString() : "";
            if (count == joinVarSize) {
                // Place the marker at the end of the last joinVar String (and
                // before the remaining "allVars")
                // A marker is needed to indicate where the join vars end so
                // that a range search from "urn:Student9[0x00]" to "urn:Student9[0xFF]"
                // does not return "urn:Student95,[remainingBindingValues]".
                value += JOIN_VAR_END_MARKER;
            }
            values.add(value);
        }

        // Return a comma delimited list of those values.
        return Joiner.on(",").join(values);
    }

    private void printStateStoreRange(final String startKey, final String endKey) {
        final KeyValueIterator<String, VisibilityBindingSet> rangeIt = store.range(startKey, endKey);
        printStateStoreKeyValueIterator(rangeIt);
    }

    private void printStateStoreAll() {
        final KeyValueIterator<String, VisibilityBindingSet> rangeIt = store.all();
        printStateStoreKeyValueIterator(rangeIt);
    }

    private static void printStateStoreKeyValueIterator(final KeyValueIterator<String, VisibilityBindingSet> rangeIt) {
        log.info("----------------");
        while (rangeIt.hasNext()) {
            final KeyValue<String, VisibilityBindingSet> keyValue = rangeIt.next();
            log.info(keyValue.key + " :::: " + keyValue.value);
        }
        log.info("----------------\n\n");
        if (rangeIt != null) {
            rangeIt.close();
        }
    }
}