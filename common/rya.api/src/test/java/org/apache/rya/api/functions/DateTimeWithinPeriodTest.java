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

import org.junit.Test;
import org.openrdf.model.Literal;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.algebra.evaluation.ValueExprEvaluationException;

public class DateTimeWithinPeriodTest {

    private static final ValueFactory vf = new ValueFactoryImpl();
    private static final Literal TRUE = vf.createLiteral(true);
    private static final Literal FALSE = vf.createLiteral(false);
    private static final ZonedDateTime testThisTimeDate = ZonedDateTime.parse("2018-02-03T14:15:16+07:00");

    @Test
    public void testSeconds() throws DatatypeConfigurationException, ValueExprEvaluationException {
        DatatypeFactory dtf = DatatypeFactory.newInstance();

        ZonedDateTime zTime = testThisTimeDate;
        String time = zTime.format(DateTimeFormatter.ISO_INSTANT);

        ZonedDateTime zTime1 = zTime.minusSeconds(1);
        String time1 = zTime1.format(DateTimeFormatter.ISO_INSTANT);

        Literal now = vf.createLiteral(dtf.newXMLGregorianCalendar(time));
        Literal nowMinusOne = vf.createLiteral(dtf.newXMLGregorianCalendar(time1));

        DateTimeWithinPeriod func = new DateTimeWithinPeriod();

        assertEquals(TRUE, func.evaluate(vf, now, now, vf.createLiteral(1), OWLTime.SECONDS_URI));
        assertEquals(FALSE, func.evaluate(vf, now, nowMinusOne,vf.createLiteral(1), OWLTime.SECONDS_URI));
        assertEquals(TRUE, func.evaluate(vf, now, nowMinusOne,vf.createLiteral(2), OWLTime.SECONDS_URI));
    }

    @Test
    public void testMinutes() throws DatatypeConfigurationException, ValueExprEvaluationException {

        DatatypeFactory dtf = DatatypeFactory.newInstance();

        ZonedDateTime zTime = testThisTimeDate;
        String time = zTime.format(DateTimeFormatter.ISO_INSTANT);

        ZonedDateTime zTime1 = zTime.minusMinutes(1);
        String time1 = zTime1.format(DateTimeFormatter.ISO_INSTANT);

        Literal now = vf.createLiteral(dtf.newXMLGregorianCalendar(time));
        Literal nowMinusOne = vf.createLiteral(dtf.newXMLGregorianCalendar(time1));

        DateTimeWithinPeriod func = new DateTimeWithinPeriod();

        assertEquals(TRUE, func.evaluate(vf, now, now,vf.createLiteral(1),OWLTime.MINUTES_URI));
        assertEquals(FALSE, func.evaluate(vf, now, nowMinusOne,vf.createLiteral(1),OWLTime.MINUTES_URI));
        assertEquals(TRUE, func.evaluate(vf, now, nowMinusOne,vf.createLiteral(2),OWLTime.MINUTES_URI));
    }


    @Test
    public void testHours() throws DatatypeConfigurationException, ValueExprEvaluationException {
        DatatypeFactory dtf = DatatypeFactory.newInstance();

        ZonedDateTime zTime = testThisTimeDate;
        String time = zTime.format(DateTimeFormatter.ISO_INSTANT);

        ZonedDateTime zTime1 = zTime.minusHours(1);
        String time1 = zTime1.format(DateTimeFormatter.ISO_INSTANT);

        Literal now = vf.createLiteral(dtf.newXMLGregorianCalendar(time));
        Literal nowMinusOne = vf.createLiteral(dtf.newXMLGregorianCalendar(time1));

        DateTimeWithinPeriod func = new DateTimeWithinPeriod();

        assertEquals(TRUE, func.evaluate(vf, now, now,vf.createLiteral(1),OWLTime.HOURS_URI));
        assertEquals(FALSE, func.evaluate(vf, now, nowMinusOne,vf.createLiteral(1),OWLTime.HOURS_URI));
        assertEquals(TRUE, func.evaluate(vf, now, nowMinusOne,vf.createLiteral(2),OWLTime.HOURS_URI));
    }


    @Test
    public void testDays() throws DatatypeConfigurationException, ValueExprEvaluationException {
        DatatypeFactory dtf = DatatypeFactory.newInstance();

        ZonedDateTime zTime = testThisTimeDate;
        String time = zTime.format(DateTimeFormatter.ISO_INSTANT);

        ZonedDateTime zTime1 = zTime.minusDays(1);
        String time1 = zTime1.format(DateTimeFormatter.ISO_INSTANT);

        Literal now = vf.createLiteral(dtf.newXMLGregorianCalendar(time));
        Literal nowMinusOne = vf.createLiteral(dtf.newXMLGregorianCalendar(time1));

        DateTimeWithinPeriod func = new DateTimeWithinPeriod();

        assertEquals(TRUE, func.evaluate(vf, now, now, vf.createLiteral(1), OWLTime.DAYS_URI));
        assertEquals(FALSE, func.evaluate(vf, now, nowMinusOne, vf.createLiteral(1), OWLTime.DAYS_URI));
        assertEquals(TRUE, func.evaluate(vf, now, nowMinusOne, vf.createLiteral(2), OWLTime.DAYS_URI));
    }

    // Note that this test fails if the week under test spans a DST when the USA springs forward.
    @Test
    public void testWeeks() throws DatatypeConfigurationException, ValueExprEvaluationException {
        DatatypeFactory dtf = DatatypeFactory.newInstance();

        ZonedDateTime zTime = testThisTimeDate;
        String time = zTime.format(DateTimeFormatter.ISO_INSTANT);

        ZonedDateTime zTime1 = zTime.minusWeeks(1);
        String time1 = zTime1.format(DateTimeFormatter.ISO_INSTANT);

        ZonedDateTime zTime2 = zTime.minusWeeks(7);
        String time2 = zTime2.format(DateTimeFormatter.ISO_INSTANT);

        Literal now = vf.createLiteral(dtf.newXMLGregorianCalendar(time));
        Literal nowMinusOne = vf.createLiteral(dtf.newXMLGregorianCalendar(time1));
        Literal nowMinusSeven = vf.createLiteral(dtf.newXMLGregorianCalendar(time2));

        DateTimeWithinPeriod func = new DateTimeWithinPeriod();

        assertEquals(TRUE, func.evaluate(vf, now, now, vf.createLiteral(1), OWLTime.WEEKS_URI));
        assertEquals(FALSE, func.evaluate(vf, now, nowMinusOne, vf.createLiteral(1), OWLTime.WEEKS_URI));
        assertEquals(TRUE, func.evaluate(vf, now, nowMinusOne, vf.createLiteral(2), OWLTime.WEEKS_URI));
        assertEquals(FALSE, func.evaluate(vf, now, nowMinusSeven, vf.createLiteral(7), OWLTime.WEEKS_URI));
    }

    @Test
    public void testTimeZone() throws DatatypeConfigurationException, ValueExprEvaluationException {
        DatatypeFactory dtf = DatatypeFactory.newInstance();

        ZonedDateTime now = testThisTimeDate;
        String time = now.format(DateTimeFormatter.ISO_INSTANT);

        ZonedDateTime zTime1 = now.withZoneSameInstant(ZoneId.of("Europe/London"));
        String time1 = zTime1.format(DateTimeFormatter.ISO_INSTANT);

        ZonedDateTime zTime2 = now.withZoneSameInstant(ZoneId.of("Australia/Sydney"));
        String time2 = zTime2.format(DateTimeFormatter.ISO_INSTANT);

        ZonedDateTime zTime3 = now.minusDays(1).withZoneSameInstant(ZoneId.of("Asia/Seoul"));
        String time3 = zTime3.format(DateTimeFormatter.ISO_INSTANT);

        Literal nowLocal = vf.createLiteral(dtf.newXMLGregorianCalendar(time));
        Literal nowEuropeTZ = vf.createLiteral(dtf.newXMLGregorianCalendar(time1));
        Literal nowAustraliaTZ = vf.createLiteral(dtf.newXMLGregorianCalendar(time2));
        Literal nowAsiaTZMinusOne = vf.createLiteral(dtf.newXMLGregorianCalendar(time3));

        DateTimeWithinPeriod func = new DateTimeWithinPeriod();

        assertEquals(TRUE, func.evaluate(vf, nowLocal, nowEuropeTZ, vf.createLiteral(1), OWLTime.SECONDS_URI));
        assertEquals(TRUE, func.evaluate(vf, nowLocal, nowAustraliaTZ, vf.createLiteral(1), OWLTime.SECONDS_URI));
        assertEquals(FALSE, func.evaluate(vf, nowLocal, nowAsiaTZMinusOne, vf.createLiteral(1), OWLTime.DAYS_URI));
        assertEquals(TRUE, func.evaluate(vf, nowLocal, nowAsiaTZMinusOne, vf.createLiteral(2), OWLTime.DAYS_URI));
    }

}
