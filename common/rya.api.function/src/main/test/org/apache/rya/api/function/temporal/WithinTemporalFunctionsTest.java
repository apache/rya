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
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.junit.Test;

public class WithinTemporalFunctionsTest {
    private static final ZonedDateTime TIME = ZonedDateTime.parse("2015-12-30T12:00:00Z");
    private static final ZonedDateTime TIME_10 = ZonedDateTime.parse("2015-12-30T12:00:10Z");
    private static final ZonedDateTime TIME_20 = ZonedDateTime.parse("2015-12-30T12:00:20Z");

    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    @Test(expected = ValueExprEvaluationException.class)
    public void within_NotInterval() throws Exception {
        // correct date formats are ensured through other tests
        final WithinTemporalInterval function = new WithinTemporalInterval();

        // 2 dates are provided
        final Value[] args = new Value[2];
        args[0] = VF.createLiteral(TIME.toString());
        args[1] = VF.createLiteral(TIME.toString());
        function.evaluate(VF, args);
    }

    @Test
    public void testWithin_beginning() throws Exception {
        final WithinTemporalInterval function = new WithinTemporalInterval();

        // 2 times equal
        final Value[] args = new Value[2];
        args[0] = VF.createLiteral(TIME.toString());
        args[1] = VF.createLiteral(TIME.toString() + "/" + TIME_20.toString());
        final Value rez = function.evaluate(VF, args);

        assertEquals(VF.createLiteral(false), rez);
    }

    @Test
    public void testWithin_within() throws Exception {
        final WithinTemporalInterval function = new WithinTemporalInterval();

        // first time is before
        final Value[] args = new Value[2];
        args[0] = VF.createLiteral(TIME_10.toString());
        args[1] = VF.createLiteral(TIME.toString() + "/" + TIME_20.toString());
        final Value rez = function.evaluate(VF, args);

        assertEquals(VF.createLiteral(true), rez);
    }

    @Test
    public void testWithin_end() throws Exception {
        final WithinTemporalInterval function = new WithinTemporalInterval();

        // first time is after
        final Value[] args = new Value[2];
        args[0] = VF.createLiteral(TIME_20.toString());
        args[1] = VF.createLiteral(TIME.toString() + "/" + TIME_20.toString());
        final Value rez = function.evaluate(VF, args);

        assertEquals(VF.createLiteral(false), rez);
    }
}
