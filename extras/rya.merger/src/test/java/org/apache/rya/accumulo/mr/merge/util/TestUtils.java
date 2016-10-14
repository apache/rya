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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.accumulo.mr.merge.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Date;

import info.aduna.iteration.CloseableIteration;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.persist.RyaDAOException;

/**
 * Utility methods for testing merging/copying.
 */
public final class TestUtils {
    /**
     * 'LAST_MONTH' is the testing timestamp for all data that was in the both the parent and child when the child was copied from the parent.
     * So, after the child was created it should contain data with this timestamp too before merging.
     */
    public static final Date LAST_MONTH = new Date(monthBefore(System.currentTimeMillis()));

    /**
     * 'YESTERDAY' is the testing start timestamp specified by the user to use for merging, all data with timestamps
     * after yesterday should be merged from the child to the parent.
     */
    public static final Date YESTERDAY = new Date(dayBefore(System.currentTimeMillis()));

    /**
     * 'TODAY' is when the merge process is actually happening.
     */
    public static final Date TODAY = new Date();

    /**
     * Indicates when something occurred: before or after.
     */
    public static enum Occurrence {
        BEFORE(-1),
        AFTER(1);

        private int sign;

        /**
         * Creates a new {@link Occurrence}.
         * @param sign the sign value: positive or negative.
         */
        private Occurrence(final int sign) {
            this.sign = sign;
        }

        /**
         * @return the sign value: positive or negative.
         */
        public int getSign() {
            return sign;
        }
    }

    /**
     * A {@code CalendarUnit} represents time durations at a given unit of
     * granularity and provides utility methods to convert to milliseconds. This
     * is similar to {@code java.util.concurrent.TimeUnit} but adds the week,
     * month, and year units and only converts to milliseconds.
     */
    public static enum CalendarUnit {
        MILLISECOND(1L),
        SECOND(1000L * MILLISECOND.getMilliseconds()),
        MINUTE(60L * SECOND.getMilliseconds()),
        HOUR(60L * MINUTE.getMilliseconds()),
        DAY(24L * HOUR.getMilliseconds()),
        WEEK(7L * DAY.getMilliseconds()),
        MONTH(31L * DAY.getMilliseconds()),
        YEAR(365L * DAY.getMilliseconds());

        private long milliseconds;

        /**
         * Creates a new {@link CalendarUnit}.
         * @param milliseconds the milliseconds value of this unit.
         */
        private CalendarUnit(final long milliseconds) {
            this.milliseconds = milliseconds;
        }

        /**
         * @return the milliseconds value of this unit.
         */
        public long getMilliseconds() {
            return milliseconds;
        }
    }

    private static final String NAMESPACE = "#:";//"urn:x:x#"; //"urn:test:litdups#";

    /**
     * Private constructor to prevent instantiation.
     */
    private TestUtils() {
    }

    /**
     * Finds the time value for one day before the specified time.
     * @param time the time to find the new time occurrence from. (in milliseconds)
     * @return the value of one day before the specified {@code time}. (in milliseconds)
     */
    public static long dayBefore(final long time) {
        return timeFrom(time, 1, CalendarUnit.DAY, Occurrence.BEFORE);
    }

    /**
     * Finds the date value for one day before the specified time.
     * @param date the {@link Date} to find the new time occurrence from.
     * @return the {@link Date} value of one day before the specified {@code date}.
     */
    public static Date dayBefore(final Date date) {
        return dateFrom(date, 1, CalendarUnit.DAY, Occurrence.BEFORE);
    }

    /**
     * Finds the time value for one month before the specified time.
     * @param time the time to find the new time occurrence from. (in milliseconds)
     * @return the value of one month before the specified {@code time}. (in milliseconds)
     */
    public static long monthBefore(final long time) {
        return timeFrom(time, 1, CalendarUnit.MONTH, Occurrence.BEFORE);
    }

    /**
     * Finds the date value for one month before the specified time.
     * @param date the {@link Date} to find the new time occurrence from.
     * @return the {@link Date} value of one month before the specified {@code date}.
     */
    public static Date monthBefore(final Date date) {
        return dateFrom(date, 1, CalendarUnit.MONTH, Occurrence.BEFORE);
    }

    /**
     * Determines the time from the specified duration before or after the provided time.
     * For example, this can be used to find the time value of something that happened 1 month before
     * the current time.
     * @param time the time to find the new time occurrence from. (in milliseconds)
     * @param duration the duration offset from the specified {@code time}.
     * @param unit the {@link CalendarUnit} of the duration
     * @param occurrence when the new time takes place, before or after.
     * @return the value of the new time. (in milliseconds)
     */
    public static long timeFrom(final long time, final long duration, final CalendarUnit unit, final Occurrence occurrence) {
        final long durationInMillis = occurrence.getSign() * duration * unit.getMilliseconds();
        final long newTime = time + durationInMillis;
        return newTime;
    }

    /**
     * Determines the date from the specified duration before or after the provided date.
     * For example, this can be used to find the time value of something that happened 1 month before
     * the current time.
     * @param date the {@link Date} to find the new time occurrence from.
     * @param duration the duration offset from the specified {@code time}.
     * @param unit the {@link CalendarUnit} of the duration
     * @param occurrence when the new time takes place, before or after.
     * @return the value of the new {@link Date}.
     */
    public static Date dateFrom(final Date date, final long duration, final CalendarUnit unit, final Occurrence occurrence) {
        final long time = timeFrom(date.getTime(), duration, unit, occurrence);
        final Date newDate = new Date(time);
        return newDate;
    }

    /**
     * Checks if a {@link RyaStatement} is in the specified instance's DAO.
     * @param description the description message for the statement.
     * @param verifyResultCount the expected number of matches.
     * @param matchStatement the {@link RyaStatement} to match.
     * @param dao the {@link AccumuloRyaDAO}.
     * @param config the {@link AccumuloRdfConfiguration} for the instance.
     * @throws RyaDAOException
     */
    public static void assertStatementInInstance(final String description, final int verifyResultCount, final RyaStatement matchStatement, final AccumuloRyaDAO dao, final AccumuloRdfConfiguration config) throws RyaDAOException {
        final CloseableIteration<RyaStatement, RyaDAOException> iter = dao.getQueryEngine().query(matchStatement, config);
        int count = 0;
        while (iter.hasNext()) {
            final RyaStatement statement = iter.next();
            assertTrue(description + " - match subject: " + matchStatement,      matchStatement.getSubject().equals(statement.getSubject()));
            assertTrue(description + " - match predicate: " + matchStatement,    matchStatement.getPredicate().equals(statement.getPredicate()));
            assertTrue(description + " - match object match: " + matchStatement, matchStatement.getObject().equals(statement.getObject()));
            count++;
        }
        iter.close();
        assertEquals(description+" - Match Counts.", verifyResultCount, count);
    }

    /**
     * Creates a {@link RyaURI} for the specified local name.
     * @param localName the URI's local name.
     * @return the {@link RyraURI}.
     */
    public static RyaURI createRyaUri(final String localName) {
        return AccumuloRyaUtils.createRyaUri(NAMESPACE, localName);
    }

    /**
     * Creates a {@link RyaStatement} from the specified subject, predicate, and object.
     * @param subject the subject.
     * @param predicate the predicate.
     * @param object the object.
     * @param date the {@link Date} to use for the key's timestamp.
     * @return the {@link RyaStatement}.
     */
    public static RyaStatement createRyaStatement(final String subject, final String predicate, final String object, final Date date) {
        final RyaURI subjectUri = createRyaUri(subject);
        final RyaURI predicateUri = createRyaUri(predicate);
        final RyaURI objectUri = createRyaUri(object);
        final RyaStatement ryaStatement = new RyaStatement(subjectUri, predicateUri, objectUri);
        if (date != null) {
            ryaStatement.setTimestamp(date.getTime());
        }
        return ryaStatement;
    }

    /**
     * Copies a {@link RyaStatement} into a new {@link RyaStatement}.
     * @param s the {@link RyaStatement} to copy.
     * @return the newly copied {@link RyaStatement}.
     */
    public static RyaStatement copyRyaStatement(final RyaStatement s) {
        return new RyaStatement(s.getSubject(), s.getPredicate(), s.getObject(), s.getContext(), s.getQualifer(), s.getColumnVisibility(), s.getValue(), s.getTimestamp());
    }
}