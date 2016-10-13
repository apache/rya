/*
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rya.jena.legacy.graph.query;

/**
 * A FixedValuator is a {@link Valuator} that delivers a constant value
 * (supplied when it is constructed).
 */
public class FixedValuator implements Valuator {
    private final Object value;

    /**
     * Initialize this {@link FixedValuator} with a specific value.
     * @param value the constant value {@link Object}.
     */
    public FixedValuator(final Object value) {
        this.value = value;
    }

    /**
     * @return this FixedValuator's value, which must be a Boolean
     * object, as a {@code boolean}. The index values are irrelevant.
     */
    @Override
    public boolean evalBool(final IndexValues iv) {
        return ((Boolean) evalObject(iv)).booleanValue();
    }

    /**
     * @return this FixedValuator's value, as supplied when it was constructed.
     */
    @Override
    public Object evalObject(final IndexValues iv) {
        return value;
    }
}