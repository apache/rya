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
package org.apache.rya.indexing.pcj.fluo.app;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.rya.indexing.pcj.fluo.app.JoinResultUpdater.IterativeJoin;
import org.apache.rya.indexing.pcj.fluo.app.JoinResultUpdater.LeftOuterJoin;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;
import org.junit.Test;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.impl.MapBindingSet;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Tests the methods of {@link LeftOuterJoin}.
 */
public class LeftOuterJoinTest {

    private final ValueFactory vf = new ValueFactoryImpl();

    @Test
    public void newLeftResult_noRightMatches() {
        final IterativeJoin leftOuterJoin = new LeftOuterJoin();

        // There is a new left result.
        final MapBindingSet mapLeftResult = new MapBindingSet();
        mapLeftResult.addBinding("name", vf.createLiteral("Bob"));
        final VisibilityBindingSet newLeftResult = new VisibilityBindingSet(mapLeftResult);

        // There are no right results that join with the left result.
        final Iterator<VisibilityBindingSet> rightResults= new ArrayList<VisibilityBindingSet>().iterator();

        // Therefore, the left result is a new join result.
        final Iterator<VisibilityBindingSet> newJoinResultsIt = leftOuterJoin.newLeftResult(newLeftResult, rightResults);

        final Set<BindingSet> newJoinResults = new HashSet<>();
        while(newJoinResultsIt.hasNext()) {
            newJoinResults.add( newJoinResultsIt.next() );
        }

        final Set<BindingSet> expected = Sets.<BindingSet>newHashSet( newLeftResult );

        assertEquals(expected, newJoinResults);
    }

    @Test
    public void newLeftResult_joinsWithRightResults() {
        final IterativeJoin leftOuterJoin = new LeftOuterJoin();

        // There is a new left result.
        final MapBindingSet mapLeftResult = new MapBindingSet();
        mapLeftResult.addBinding("name", vf.createLiteral("Bob"));
        mapLeftResult.addBinding("height", vf.createLiteral("5'9\""));
        final VisibilityBindingSet newLeftResult = new VisibilityBindingSet(mapLeftResult);

        // There are a few right results that join with the left result.
        final MapBindingSet nameAge = new MapBindingSet();
        nameAge.addBinding("name", vf.createLiteral("Bob"));
        nameAge.addBinding("age", vf.createLiteral(56));
        final VisibilityBindingSet visiAge = new VisibilityBindingSet(nameAge);

        final MapBindingSet nameHair = new MapBindingSet();
        nameHair.addBinding("name", vf.createLiteral("Bob"));
        nameHair.addBinding("hairColor", vf.createLiteral("Brown"));
        final VisibilityBindingSet visiHair = new VisibilityBindingSet(nameHair);

        final Iterator<VisibilityBindingSet> rightResults = Lists.<VisibilityBindingSet>newArrayList(visiAge, visiHair).iterator();

        // Therefore, there are a few new join results that mix the two together.
        final Iterator<VisibilityBindingSet> newJoinResultsIt = leftOuterJoin.newLeftResult(newLeftResult, rightResults);

        final Set<BindingSet> newJoinResults = new HashSet<>();
        while(newJoinResultsIt.hasNext()) {
            newJoinResults.add( newJoinResultsIt.next() );
        }

        final Set<BindingSet> expected = Sets.<BindingSet>newHashSet();
        final MapBindingSet nameHeightAge = new MapBindingSet();
        nameHeightAge.addBinding("name", vf.createLiteral("Bob"));
        nameHeightAge.addBinding("height", vf.createLiteral("5'9\""));
        nameHeightAge.addBinding("age", vf.createLiteral(56));
        expected.add(new VisibilityBindingSet(nameHeightAge));

        final MapBindingSet nameHeightHair = new MapBindingSet();
        nameHeightHair.addBinding("name", vf.createLiteral("Bob"));
        nameHeightHair.addBinding("height", vf.createLiteral("5'9\""));
        nameHeightHair.addBinding("hairColor", vf.createLiteral("Brown"));
        expected.add(new VisibilityBindingSet(nameHeightHair));

        assertEquals(expected, newJoinResults);
    }

    @Test
    public void newRightResult_noLeftMatches() {
        final IterativeJoin leftOuterJoin = new LeftOuterJoin();

        // There are no left results.
        final Iterator<VisibilityBindingSet> leftResults= new ArrayList<VisibilityBindingSet>().iterator();

        // There is a new right result.
        final MapBindingSet newRightResult = new MapBindingSet();
        newRightResult.addBinding("name", vf.createLiteral("Bob"));

        // Therefore, there are no new join results.
        final Iterator<VisibilityBindingSet> newJoinResultsIt = leftOuterJoin.newRightResult(leftResults, new VisibilityBindingSet(newRightResult));
        assertFalse( newJoinResultsIt.hasNext() );
    }

    @Test
    public void newRightResult_joinsWithLeftResults() {
        final IterativeJoin leftOuterJoin = new LeftOuterJoin();

        // There are a few left results that join with the new right result.
        final MapBindingSet nameAge = new MapBindingSet();
        nameAge.addBinding("name", vf.createLiteral("Bob"));
        nameAge.addBinding("age", vf.createLiteral(56));

        final MapBindingSet nameHair = new MapBindingSet();
        nameHair.addBinding("name", vf.createLiteral("Bob"));
        nameHair.addBinding("hairColor", vf.createLiteral("Brown"));

        final Iterator<VisibilityBindingSet> leftResults = Lists.<VisibilityBindingSet>newArrayList(
                new VisibilityBindingSet(nameAge),
                new VisibilityBindingSet(nameHair)).iterator();

        // There is a new right result.
        final MapBindingSet newRightResult = new MapBindingSet();
        newRightResult.addBinding("name", vf.createLiteral("Bob"));
        newRightResult.addBinding("height", vf.createLiteral("5'9\""));

        // Therefore, there are a few new join results that mix the two together.
        final Iterator<VisibilityBindingSet> newJoinResultsIt = leftOuterJoin.newRightResult(leftResults, new VisibilityBindingSet(newRightResult));

        final Set<BindingSet> newJoinResults = new HashSet<>();
        while(newJoinResultsIt.hasNext()) {
            newJoinResults.add( newJoinResultsIt.next() );
        }

        final Set<BindingSet> expected = Sets.<BindingSet>newHashSet();
        final MapBindingSet nameHeightAge = new MapBindingSet();
        nameHeightAge.addBinding("name", vf.createLiteral("Bob"));
        nameHeightAge.addBinding("height", vf.createLiteral("5'9\""));
        nameHeightAge.addBinding("age", vf.createLiteral(56));
        expected.add(new VisibilityBindingSet(nameHeightAge));

        final MapBindingSet nameHeightHair = new MapBindingSet();
        nameHeightHair.addBinding("name", vf.createLiteral("Bob"));
        nameHeightHair.addBinding("height", vf.createLiteral("5'9\""));
        nameHeightHair.addBinding("hairColor", vf.createLiteral("Brown"));
        expected.add(new VisibilityBindingSet(nameHeightHair));

        assertEquals(expected, newJoinResults);
    }
}