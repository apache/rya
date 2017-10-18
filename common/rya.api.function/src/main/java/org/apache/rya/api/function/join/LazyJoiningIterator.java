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

import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.api.model.visibility.VisibilitySimplifier;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.impl.MapBindingSet;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Joins a {@link BindingSet} (which is new to the left or right side of a join)
 * to all binding sets on the other side that join with it.
 * <p>
 * This is done lazily so that you don't have to load all of the BindingSets
 * into memory at once.
 */
@DefaultAnnotation(NonNull.class)
public final class LazyJoiningIterator implements Iterator<VisibilityBindingSet> {
    private final Side newResultSide;
    private final VisibilityBindingSet newResult;
    private final Iterator<VisibilityBindingSet> joinedResults;

    /**
     * Constructs an instance of {@link LazyJoiningIterator}.
     *
     * @param newResultSide - Indicates which side of the join the
     *        {@code newResult} arrived on. (not null)
     * @param newResult - A binding set that will be joined with some other
     *        binding sets. (not null)
     * @param joinedResults - The binding sets that will be joined with
     *        {@code newResult}. (not null)
     */
    public LazyJoiningIterator(final Side newResultSide, final VisibilityBindingSet newResult,
            final Iterator<VisibilityBindingSet> joinedResults) {
        this.newResultSide = requireNonNull(newResultSide);
        this.newResult = requireNonNull(newResult);
        this.joinedResults = requireNonNull(joinedResults);
    }

    @Override
    public boolean hasNext() {
        return joinedResults.hasNext();
    }

    @Override
    public VisibilityBindingSet next() {
        final MapBindingSet bs = new MapBindingSet();

        for (final Binding binding : newResult) {
            bs.addBinding(binding);
        }

        final VisibilityBindingSet joinResult = joinedResults.next();
        for (final Binding binding : joinResult) {
            bs.addBinding(binding);
        }

        // We want to make sure the visibilities are always written the same way,
        // so figure out which are on the left side and which are on the right side.
        final String leftVisi;
        final String rightVisi;
        if (newResultSide == Side.LEFT) {
            leftVisi = newResult.getVisibility();
            rightVisi = joinResult.getVisibility();
        } else {
            leftVisi = joinResult.getVisibility();
            rightVisi = newResult.getVisibility();
        }

        final String visibility = VisibilitySimplifier.unionAndSimplify(leftVisi, rightVisi);

        return new VisibilityBindingSet(bs, visibility);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove() is unsupported.");
    }

    /**
     * The different sides a new binding set may appear on.
     */
    public static enum Side {
        LEFT, RIGHT;
    }
}