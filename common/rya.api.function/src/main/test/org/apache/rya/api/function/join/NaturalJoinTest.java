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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.api.function.join.IterativeJoin;
import org.apache.rya.api.function.join.NaturalJoin;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Tests the methods of {@link NaturalJoin}.
 */
public class NaturalJoinTest {

    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    @Test
    public void newLeftResult_noRightMatches() {
        final IterativeJoin naturalJoin = new NaturalJoin();

        // There is a new left result.
        final MapBindingSet newLeftResult = new MapBindingSet();
        newLeftResult.addBinding("name", VF.createLiteral("Bob"));

        // There are no right results that join with the left result.
        final Iterator<VisibilityBindingSet> rightResults= new ArrayList<VisibilityBindingSet>().iterator();

        // Therefore, the left result is a new join result.
        final Iterator<VisibilityBindingSet> newJoinResultsIt = naturalJoin.newLeftResult(new VisibilityBindingSet(newLeftResult), rightResults);
        assertFalse( newJoinResultsIt.hasNext() );
    }

    @Test
    public void newLeftResult_joinsWithRightResults() {
        final IterativeJoin naturalJoin = new NaturalJoin();

        // There is a new left result.
        final MapBindingSet newLeftResult = new MapBindingSet();
        newLeftResult.addBinding("name", VF.createLiteral("Bob"));
        newLeftResult.addBinding("height", VF.createLiteral("5'9\""));

        // There are a few right results that join with the left result.
        final MapBindingSet nameAge = new MapBindingSet();
        nameAge.addBinding("name", VF.createLiteral("Bob"));
        nameAge.addBinding("age", VF.createLiteral(BigInteger.valueOf(56)));

        final MapBindingSet nameHair = new MapBindingSet();
        nameHair.addBinding("name", VF.createLiteral("Bob"));
        nameHair.addBinding("hairColor", VF.createLiteral("Brown"));

        final Iterator<VisibilityBindingSet> rightResults = Lists.newArrayList(
                new VisibilityBindingSet(nameAge),
                new VisibilityBindingSet(nameHair)).iterator();

        // Therefore, there are a few new join results that mix the two together.
        final Iterator<VisibilityBindingSet> newJoinResultsIt = naturalJoin.newLeftResult(new VisibilityBindingSet(newLeftResult), rightResults);

        final Set<BindingSet> newJoinResults = new HashSet<>();
        while(newJoinResultsIt.hasNext()) {
            newJoinResults.add( newJoinResultsIt.next() );
        }

        final Set<BindingSet> expected = Sets.newHashSet();
        final MapBindingSet nameHeightAge = new MapBindingSet();
        nameHeightAge.addBinding("name", VF.createLiteral("Bob"));
        nameHeightAge.addBinding("height", VF.createLiteral("5'9\""));
        nameHeightAge.addBinding("age", VF.createLiteral(BigInteger.valueOf(56)));
        expected.add(new VisibilityBindingSet(nameHeightAge));

        final MapBindingSet nameHeightHair = new MapBindingSet();
        nameHeightHair.addBinding("name", VF.createLiteral("Bob"));
        nameHeightHair.addBinding("height", VF.createLiteral("5'9\""));
        nameHeightHair.addBinding("hairColor", VF.createLiteral("Brown"));
        expected.add(new VisibilityBindingSet(nameHeightHair));

        assertEquals(expected, newJoinResults);
    }

    @Test
    public void newRightResult_noLeftMatches() {
        final IterativeJoin naturalJoin = new NaturalJoin();

        // There are no left results.
        final Iterator<VisibilityBindingSet> leftResults= new ArrayList<VisibilityBindingSet>().iterator();

        // There is a new right result.
        final MapBindingSet newRightResult = new MapBindingSet();
        newRightResult.addBinding("name", VF.createLiteral("Bob"));

        // Therefore, there are no new join results.
        final Iterator<VisibilityBindingSet> newJoinResultsIt = naturalJoin.newRightResult(leftResults, new VisibilityBindingSet(newRightResult));
        assertFalse( newJoinResultsIt.hasNext() );
    }

    @Test
    public void newRightResult_joinsWithLeftResults() {
        final IterativeJoin naturalJoin = new NaturalJoin();

        // There are a few left results that join with the new right result.
        final MapBindingSet nameAge = new MapBindingSet();
        nameAge.addBinding("name", VF.createLiteral("Bob"));
        nameAge.addBinding("age", VF.createLiteral(BigInteger.valueOf(56)));

        final MapBindingSet nameHair = new MapBindingSet();
        nameHair.addBinding("name", VF.createLiteral("Bob"));
        nameHair.addBinding("hairColor", VF.createLiteral("Brown"));

        final Iterator<VisibilityBindingSet> leftResults = Lists.newArrayList(
                new VisibilityBindingSet(nameAge),
                new VisibilityBindingSet(nameHair)).iterator();

        // There is a new right result.
        final MapBindingSet newRightResult = new MapBindingSet();
        newRightResult.addBinding("name", VF.createLiteral("Bob"));
        newRightResult.addBinding("height", VF.createLiteral("5'9\""));

        // Therefore, there are a few new join results that mix the two together.
        final Iterator<VisibilityBindingSet> newJoinResultsIt = naturalJoin.newRightResult(leftResults, new VisibilityBindingSet(newRightResult));

        final Set<BindingSet> newJoinResults = new HashSet<>();
        while(newJoinResultsIt.hasNext()) {
            newJoinResults.add( newJoinResultsIt.next() );
        }

        final Set<BindingSet> expected = Sets.newHashSet();
        final MapBindingSet nameHeightAge = new MapBindingSet();
        nameHeightAge.addBinding("name", VF.createLiteral("Bob"));
        nameHeightAge.addBinding("height", VF.createLiteral("5'9\""));
        nameHeightAge.addBinding("age", VF.createLiteral(BigInteger.valueOf(56)));
        expected.add(new VisibilityBindingSet(nameHeightAge));

        final MapBindingSet nameHeightHair = new MapBindingSet();
        nameHeightHair.addBinding("name", VF.createLiteral("Bob"));
        nameHeightHair.addBinding("height", VF.createLiteral("5'9\""));
        nameHeightHair.addBinding("hairColor", VF.createLiteral("Brown"));
        expected.add(new VisibilityBindingSet(nameHeightHair));

        assertEquals(expected, newJoinResults);
    }
}