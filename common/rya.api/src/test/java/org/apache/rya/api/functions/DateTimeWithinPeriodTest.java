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
package org.apache.rya.api.functions;

import static org.junit.Assert.assertEquals;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.junit.Test;

public class DateTimeWithinPeriodTest {

    private static final ValueFactory VF = SimpleValueFactory.getInstance();
    private static final Literal TRUE = VF.createLiteral(true);
    private static final Literal FALSE = VF.createLiteral(false);

    @Test
    public void testSeconds() throws DatatypeConfigurationException, ValueExprEvaluationException {
        DatatypeFactory dtf = DatatypeFactory.newInstance();

        ZonedDateTime zTime = ZonedDateTime.now();
        String time = zTime.format(DateTimeFormatter.ISO_INSTANT);

        ZonedDateTime zTime1 = zTime.minusSeconds(1);
        String time1 = zTime1.format(DateTimeFormatter.ISO_INSTANT);

        Literal now = VF.createLiteral(dtf.newXMLGregorianCalendar(time));
        Literal nowMinusOne = VF.createLiteral(dtf.newXMLGregorianCalendar(time1));

        DateTimeWithinPeriod func = new DateTimeWithinPeriod();

        assertEquals(TRUE, func.evaluate(VF, now, now, VF.createLiteral(1), OWLTime.SECONDS_URI));
        assertEquals(FALSE, func.evaluate(VF, now, nowMinusOne,VF.createLiteral(1), OWLTime.SECONDS_URI));
        assertEquals(TRUE, func.evaluate(VF, now, nowMinusOne,VF.createLiteral(2), OWLTime.SECONDS_URI));
    }

    @Test
    public void testMinutes() throws DatatypeConfigurationException, ValueExprEvaluationException {

        DatatypeFactory dtf = DatatypeFactory.newInstance();

        ZonedDateTime zTime = ZonedDateTime.now();
        String time = zTime.format(DateTimeFormatter.ISO_INSTANT);

        ZonedDateTime zTime1 = zTime.minusMinutes(1);
        String time1 = zTime1.format(DateTimeFormatter.ISO_INSTANT);

        Literal now = VF.createLiteral(dtf.newXMLGregorianCalendar(time));
        Literal nowMinusOne = VF.createLiteral(dtf.newXMLGregorianCalendar(time1));

        DateTimeWithinPeriod func = new DateTimeWithinPeriod();

        assertEquals(TRUE, func.evaluate(VF, now, now,VF.createLiteral(1),OWLTime.MINUTES_URI));
        assertEquals(FALSE, func.evaluate(VF, now, nowMinusOne,VF.createLiteral(1),OWLTime.MINUTES_URI));
        assertEquals(TRUE, func.evaluate(VF, now, nowMinusOne,VF.createLiteral(2),OWLTime.MINUTES_URI));
    }


    @Test
    public void testHours() throws DatatypeConfigurationException, ValueExprEvaluationException {
        DatatypeFactory dtf = DatatypeFactory.newInstance();

        ZonedDateTime zTime = ZonedDateTime.now();
        String time = zTime.format(DateTimeFormatter.ISO_INSTANT);

        ZonedDateTime zTime1 = zTime.minusHours(1);
        String time1 = zTime1.format(DateTimeFormatter.ISO_INSTANT);

        Literal now = VF.createLiteral(dtf.newXMLGregorianCalendar(time));
        Literal nowMinusOne = VF.createLiteral(dtf.newXMLGregorianCalendar(time1));

        DateTimeWithinPeriod func = new DateTimeWithinPeriod();

        assertEquals(TRUE, func.evaluate(VF, now, now,VF.createLiteral(1),OWLTime.HOURS_URI));
        assertEquals(FALSE, func.evaluate(VF, now, nowMinusOne,VF.createLiteral(1),OWLTime.HOURS_URI));
        assertEquals(TRUE, func.evaluate(VF, now, nowMinusOne,VF.createLiteral(2),OWLTime.HOURS_URI));
    }


    @Test
    public void testDays() throws DatatypeConfigurationException, ValueExprEvaluationException {
        DatatypeFactory dtf = DatatypeFactory.newInstance();

        ZonedDateTime zTime = ZonedDateTime.now();
        String time = zTime.format(DateTimeFormatter.ISO_INSTANT);

        ZonedDateTime zTime1 = zTime.minusDays(1);
        String time1 = zTime1.format(DateTimeFormatter.ISO_INSTANT);

        Literal now = VF.createLiteral(dtf.newXMLGregorianCalendar(time));
        Literal nowMinusOne = VF.createLiteral(dtf.newXMLGregorianCalendar(time1));

        DateTimeWithinPeriod func = new DateTimeWithinPeriod();

        assertEquals(TRUE, func.evaluate(VF, now, now, VF.createLiteral(1), OWLTime.DAYS_URI));
        assertEquals(FALSE, func.evaluate(VF, now, nowMinusOne, VF.createLiteral(1), OWLTime.DAYS_URI));
        assertEquals(TRUE, func.evaluate(VF, now, nowMinusOne, VF.createLiteral(2), OWLTime.DAYS_URI));
    }

    @Test
    public void testWeeks() throws DatatypeConfigurationException, ValueExprEvaluationException {
        DatatypeFactory dtf = DatatypeFactory.newInstance();

        ZonedDateTime zTime = ZonedDateTime.now();
        String time = zTime.format(DateTimeFormatter.ISO_INSTANT);

        ZonedDateTime zTime1 = zTime.minusWeeks(1);
        String time1 = zTime1.format(DateTimeFormatter.ISO_INSTANT);

        ZonedDateTime zTime2 = zTime.minusWeeks(7);
        String time2 = zTime2.format(DateTimeFormatter.ISO_INSTANT);

        Literal now = VF.createLiteral(dtf.newXMLGregorianCalendar(time));
        Literal nowMinusOne = VF.createLiteral(dtf.newXMLGregorianCalendar(time1));
        Literal nowMinusSeven = VF.createLiteral(dtf.newXMLGregorianCalendar(time2));

        DateTimeWithinPeriod func = new DateTimeWithinPeriod();

        assertEquals(TRUE, func.evaluate(VF, now, now, VF.createLiteral(1), OWLTime.WEEKS_URI));
        assertEquals(FALSE, func.evaluate(VF, now, nowMinusOne, VF.createLiteral(1), OWLTime.WEEKS_URI));
        assertEquals(TRUE, func.evaluate(VF, now, nowMinusOne, VF.createLiteral(2), OWLTime.WEEKS_URI));
        assertEquals(FALSE, func.evaluate(VF, now, nowMinusSeven, VF.createLiteral(7), OWLTime.WEEKS_URI));
    }

    @Test
    public void testTimeZone() throws DatatypeConfigurationException, ValueExprEvaluationException {
        DatatypeFactory dtf = DatatypeFactory.newInstance();

        ZonedDateTime now = ZonedDateTime.now();
        String time = now.format(DateTimeFormatter.ISO_INSTANT);

        ZonedDateTime zTime1 = now.withZoneSameInstant(ZoneId.of("Europe/London"));
        String time1 = zTime1.format(DateTimeFormatter.ISO_INSTANT);

        ZonedDateTime zTime2 = now.withZoneSameInstant(ZoneId.of("Australia/Sydney"));
        String time2 = zTime2.format(DateTimeFormatter.ISO_INSTANT);

        ZonedDateTime zTime3 = now.minusDays(1).withZoneSameInstant(ZoneId.of("Asia/Seoul"));
        String time3 = zTime3.format(DateTimeFormatter.ISO_INSTANT);

        Literal nowLocal = VF.createLiteral(dtf.newXMLGregorianCalendar(time));
        Literal nowEuropeTZ = VF.createLiteral(dtf.newXMLGregorianCalendar(time1));
        Literal nowAustraliaTZ = VF.createLiteral(dtf.newXMLGregorianCalendar(time2));
        Literal nowAsiaTZMinusOne = VF.createLiteral(dtf.newXMLGregorianCalendar(time3));

        DateTimeWithinPeriod func = new DateTimeWithinPeriod();

        assertEquals(TRUE, func.evaluate(VF, nowLocal, nowEuropeTZ, VF.createLiteral(1), OWLTime.SECONDS_URI));
        assertEquals(TRUE, func.evaluate(VF, nowLocal, nowAustraliaTZ, VF.createLiteral(1), OWLTime.SECONDS_URI));
        assertEquals(FALSE, func.evaluate(VF, nowLocal, nowAsiaTZMinusOne, VF.createLiteral(1), OWLTime.DAYS_URI));
        assertEquals(TRUE, func.evaluate(VF, nowLocal, nowAsiaTZMinusOne, VF.createLiteral(2), OWLTime.DAYS_URI));
    }


}
