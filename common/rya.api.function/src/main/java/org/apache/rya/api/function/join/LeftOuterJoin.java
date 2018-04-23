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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.rya.api.function.join.LazyJoiningIterator.Side;
import org.apache.rya.api.model.VisibilityBindingSet;
import org.eclipse.rdf4j.query.BindingSet;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Implements an {@link IterativeJoin} that uses the Left Outer Join
 * algorithm defined by Relational Algebra.
 * <p>
 * This is how you add optional information to a {@link BindingSet}. Left
 * binding sets are emitted even if they do not join with anything on the right.
 * However, right binding sets must be joined with a left binding set.
 */
@DefaultAnnotation(NonNull.class)
public final class LeftOuterJoin implements IterativeJoin {
    @Override
    public Iterator<VisibilityBindingSet> newLeftResult(final VisibilityBindingSet newLeftResult, final Iterator<VisibilityBindingSet> rightResults) {
        requireNonNull(newLeftResult);
        requireNonNull(rightResults);

        // If the required portion does not join with any optional portions,
        // then emit a BindingSet that matches the new left result.
        if(!rightResults.hasNext()) {
            final List<VisibilityBindingSet> leftResultList = new ArrayList<>();
            leftResultList.add(newLeftResult);
            return leftResultList.iterator();
        }

        // Otherwise, return an iterator that holds the new required result
        // joined with the right results.
        return new LazyJoiningIterator(Side.LEFT, newLeftResult, rightResults);
    }

    @Override
    public Iterator<VisibilityBindingSet> newRightResult(final Iterator<VisibilityBindingSet> leftResults, final VisibilityBindingSet newRightResult) {
        requireNonNull(leftResults);
        requireNonNull(newRightResult);

        // The right result is optional, so if it does not join with anything
        // on the left, then do not emit anything.
        return new LazyJoiningIterator(Side.RIGHT, newRightResult, leftResults);
    }
}