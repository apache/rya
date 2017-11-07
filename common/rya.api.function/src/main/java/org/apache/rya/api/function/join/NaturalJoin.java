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
package org.apache.rya.api.function.join;

import static java.util.Objects.requireNonNull;

import java.util.Iterator;

import org.apache.rya.api.function.join.LazyJoiningIterator.Side;
import org.apache.rya.api.model.VisibilityBindingSet;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Implements an {@link IterativeJoin} that uses the Natural Join algorithm
 * defined by Relational Algebra.
 * <p>
 * This is how you combine {@code BindnigSet}s that may have common Binding
 * names. When two Binding Sets are joined, any bindings that appear in both
 * binding sets are only included once.
 */
@DefaultAnnotation(NonNull.class)
public class NaturalJoin implements IterativeJoin {
    @Override
    public Iterator<VisibilityBindingSet> newLeftResult(final VisibilityBindingSet newLeftResult,
            final Iterator<VisibilityBindingSet> rightResults) {
        requireNonNull(newLeftResult);
        requireNonNull(rightResults);

        // Both sides are required, so if there are no right results, then do
        // not emit anything.
        return new LazyJoiningIterator(Side.LEFT, newLeftResult, rightResults);
    }

    @Override
    public Iterator<VisibilityBindingSet> newRightResult(final Iterator<VisibilityBindingSet> leftResults,
            final VisibilityBindingSet newRightResult) {
        requireNonNull(leftResults);
        requireNonNull(newRightResult);

        // Both sides are required, so if there are no left reuslts, then do not
        // emit anything.
        return new LazyJoiningIterator(Side.RIGHT, newRightResult, leftResults);
    }
}