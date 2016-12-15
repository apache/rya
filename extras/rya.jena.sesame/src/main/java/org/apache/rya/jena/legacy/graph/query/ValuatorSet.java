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

import java.util.Iterator;
import java.util.Set;

import org.apache.jena.util.CollectionFactory;

/**
 * ValuatorSet - a set of Valuators, which can be added to and evaluated [only].
 */
public class ValuatorSet {
    private final Set<Valuator> valuators = CollectionFactory.createHashedSet();

    /**
     * Creates a new instance of {@link ValuatorSet}.
     */
    public ValuatorSet() {
    }

    /**
     * @return {@code true} if evaluating this ValuatorSet runs some Valuators.
     */
    public boolean isNonTrivial() {
        return valuators.size() > 0;
    }

    /**
     * @param e the {@link Valuator}
     * @return this ValuatorSet after adding the Valuator {@code e} to it.
     */
    public ValuatorSet add(final Valuator e) {
        valuators.add(e);
        return this;
    }

    /**
     * @return {@code true} if no Valuator in this set evaluates to {@code false}. The
     * Valuators are evaluated in an unspecified order, and evaluation ceases as
     * soon as any Valuator has returned {@code false}.
     */
    public boolean evalBool(final IndexValues iv) {
        final Iterator<Valuator> it = valuators.iterator();
        while (it.hasNext()) {
            final Valuator valuator = it.next();
            if (!valuator.evalBool(iv)) {
                return false;
            }
        }
        return true;
    }
}