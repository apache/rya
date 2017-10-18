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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.api.function.join.IterativeJoin;
import org.apache.rya.api.function.join.LeftOuterJoin;
import org.apache.rya.api.function.join.NaturalJoin;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assert.assertEquals;

/**
 * Tests the methods of {@link IterativeJoin}.
 */
@RunWith(Parameterized.class)
public class IterativeJoinTest {

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            {new NaturalJoin()},
            {new LeftOuterJoin()}
           });
    }

    @Parameter
    public IterativeJoin join;

    /**
     * This test ensures the same binding sets are created as the result of a
     * {@link IterativeJoin} regardless of which side the notification is triggered on.
     */
    @Test
    public void naturalJoin_sideDoesNotMatter() {
        // Create the binding sets that will be joined.
        final ValueFactory vf = SimpleValueFactory.getInstance();

        final MapBindingSet bs1 = new MapBindingSet();
        bs1.addBinding("id", vf.createLiteral("some_uid"));
        bs1.addBinding("name", vf.createLiteral("Alice"));
        final VisibilityBindingSet vbs1 = new VisibilityBindingSet(bs1, "a");

        final MapBindingSet bs2 = new MapBindingSet();
        bs2.addBinding("id", vf.createLiteral("some_uid"));
        bs2.addBinding("hair", vf.createLiteral("brown"));
        final VisibilityBindingSet vbs2 = new VisibilityBindingSet(bs2, "b");

        // new vbs1 shows up on the left, matches vbs2 on the right
        final Iterator<VisibilityBindingSet> newLeftIt = join.newLeftResult(vbs1, Collections.singleton(vbs2).iterator());
        final VisibilityBindingSet newLeftResult = newLeftIt.next();

        // new vbs2 shows up on the right, matches vbs1 on the left
        final Iterator<VisibilityBindingSet> newRightIt = join.newRightResult(Collections.singleton(vbs1).iterator(), vbs2);
        final VisibilityBindingSet newRightResult = newRightIt.next();

        // Ensure those two results are the same.
        assertEquals(newLeftResult, newRightResult);
    }
}