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
 * An {@link Applyer} object will run the {@link StageElement} {@code next} over
 * all the extensions of the Domain {@code domain} which are derived from
 * applying the {@link Matcher} {@code matcher} to some internal supply of
 * triples.
 */
public abstract class Applyer {
    /**
     * Applies the next {@link StateElement} over all extensions of the the
     * {@link Domain} which are derived from applying the {@link Matcher} to its
     * internal supply of triples.
     * @param domain the {@link Domain}.
     * @param matcher the {@link Matcher}.
     * @param next the next {@link StageElement}.
     */
    public abstract void applyToTriples(Domain domain, Matcher matcher, StageElement next);

    /**
     * An {@link Applyer} that never calls its {@code next}
     * {@link StageElement}.
     */
    public static final Applyer EMPTY = new Applyer() {
        @Override
        public void applyToTriples(final Domain domain, final Matcher matcher, final StageElement next) {
        }
    };
}