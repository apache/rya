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
package org.apache.rya.api.function.temporal;

import static org.junit.Assert.assertEquals;

import java.time.ZonedDateTime;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.Test;

public class BeforeTemporalFunctionsTest {
    private static final ZonedDateTime TIME = ZonedDateTime.parse("2015-12-30T12:00:00Z");
    private static final ZonedDateTime TIME_10 = ZonedDateTime.parse("2015-12-30T12:00:10Z");
    private static final ZonedDateTime TIME_20 = ZonedDateTime.parse("2015-12-30T12:00:20Z");

    final ValueFactory VF = SimpleValueFactory.getInstance();

    @Test
    public void testBefore_same() throws Exception {
        final BeforeTemporalInstant function = new BeforeTemporalInstant();

        // 2 times equal
        final Value[] args = new Value[2];
        args[0] = VF.createLiteral(TIME.toString());
        args[1] = VF.createLiteral(TIME.toString());
        final Value rez = function.evaluate(VF, args);

        assertEquals(VF.createLiteral(false), rez);
    }

    @Test
    public void testBefore_before() throws Exception {
        final BeforeTemporalInstant function = new BeforeTemporalInstant();

        // 2 times equal
        final Value[] args = new Value[2];
        args[0] = VF.createLiteral(TIME.toString());
        args[1] = VF.createLiteral(TIME_10.toString());
        final Value rez = function.evaluate(VF, args);

        assertEquals(VF.createLiteral(true), rez);
    }

    @Test
    public void testBefore_after() throws Exception {
        final BeforeTemporalInstant function = new BeforeTemporalInstant();

        // 2 times equal
        final Value[] args = new Value[2];
        args[0] = VF.createLiteral(TIME_20.toString());
        args[1] = VF.createLiteral(TIME.toString());
        final Value rez = function.evaluate(VF, args);

        assertEquals(VF.createLiteral(false), rez);
    }
}
