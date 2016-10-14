/**
 *
 */
package org.apache.rya.indexing;

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


import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.codec.binary.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

/**
 * Immutable date and time instance returning a human readable key.
 * Preserves the Time zone, but not stored in the key.
 * Converts fields (hours, etc) correctly for tz=Zulu when stored,
 * so the original timezone is not preserved when retrieved.
 *
 * Uses rfc 3339, which looks like: YYYY-MM-DDThh:mm:ssZ a subset
 * of ISO-8601 : https://www.ietf.org/rfc/rfc3339.txt
 *
 * Limits: All dates and times are assumed to be in the "current era", no BC,
 * somewhere between 0000AD and 9999AD.
 *
 * Resolution: to the second, or millisecond if the optional fraction is used.
 *
 * This is really a wrapper for Joda DateTime. if you need functionality from
 * that wonderful class, simply use t.getAsDateTime().
 *
 */
public class TemporalInstantRfc3339 implements TemporalInstant {

    private static final long serialVersionUID = -7790000399142290309L;

    private final DateTime dateTime;
    /**
     * Format key like this: YYYY-MM-DDThh:mm:ssZ
     */
    public final static DateTimeFormatter FORMATTER = ISODateTimeFormat.dateTimeNoMillis();

    public static final Pattern PATTERN = Pattern.compile("\\[(.*)\\,(.*)\\].*");

    /**
     * New date assumed UTC time zone.
     *
     * @param year
     * @param month
     * @param day
     * @param hour
     * @param minute
     * @param second
     */
    public TemporalInstantRfc3339(final int year, final int month, final int day, final int hour, final int minute, final int second) {
        dateTime = new DateTime(year, month, day, hour, minute, second, DateTimeZone.UTC);
    }

    /**
     * Construct with a Joda/java v8 DateTime;
     * TZ is preserved, but not in the key.
     *
     * @param dateTime
     *            initialize with this date time. Converted to zulu time zone for key generation.
     * @return
     */
    public TemporalInstantRfc3339(final DateTime datetime) {
        dateTime = datetime;
    }
    /**
     * Get an interval setting beginning and end with this implementation of {@link TemporalInstant}.
     * beginning must be less than end.
     *
     * @param dateTimeInterval  String in the form [dateTime1,dateTime2]
     */
    public static TemporalInterval parseInterval(final String dateTimeInterval) {

        final Matcher matcher = PATTERN.matcher(dateTimeInterval);
        if (matcher.find()) {
            // Got a date time pair, parse into an interval.
            return new TemporalInterval(
                new TemporalInstantRfc3339(new DateTime(matcher.group(1))),
                new TemporalInstantRfc3339(new DateTime(matcher.group(2))));
        }
        throw new IllegalArgumentException("Can't parse interval, expecting '[ISO8601dateTime1,ISO8601dateTime2]', actual: "+dateTimeInterval);
    }

    /**
     * if this is older returns -1, equal 0, else 1
     *
     */
    @Override
    public int compareTo(final TemporalInstant that) {
        return getAsKeyString().compareTo(that.getAsKeyString());
    }

    @Override
    public byte[] getAsKeyBytes() {
        return StringUtils.getBytesUtf8(getAsKeyString());
    }

    @Override
    public String getAsKeyString() {
        return dateTime.withZone(DateTimeZone.UTC).toString(FORMATTER);
    }

    /**
     * Readable string, formated local time at {@link DateTimeZone}.
     * If the timezone is UTC (Z), it was probably a key from the database.
     * If the server and client are in different Time zone, should probably use the client timezone.
     *
     * Time at specified time zone:
     * instant.getAsReadable(DateTimeZone.forID("-05:00")));
     * instant.getAsReadable(DateTimeZone.getDefault()));
     *
     * Use original time zone set in the constructor:
     * instant.getAsDateTime().toString(TemporalInstantRfc3339.FORMATTER));
     *
     */
    @Override
    public String getAsReadable(final DateTimeZone dateTimeZone) {
        return dateTime.withZone(dateTimeZone).toString(FORMATTER);
    }

    /**
     * Use original time zone set in the constructor, or UTC if from parsing the key.
     */
    @Override
    public String getAsReadable() {
        return dateTime.toString(FORMATTER);
    }

    /**
     * default toString, same as getAsReadable().
     */
    @Override
    public String toString() {
        return getAsReadable();
    }

    /**
     * Show readable time converted to the default timezone.
     */
    @Override
    public DateTime getAsDateTime() {
        return dateTime;
    }

    /**
     * Minimum Date, used for infinitely past.
     */
    private static final TemporalInstant MINIMUM = new TemporalInstantRfc3339(new DateTime(Long.MIN_VALUE));
    /**
     * maximum date/time is used for infinitely in the future.
     */
    private static final TemporalInstant MAXIMUM = new TemporalInstantRfc3339(new DateTime(Long.MAX_VALUE));

    /**
     * infinite past date.
     * @return an instant that will compare as NEWER than anything but itself.
     */
    public static TemporalInstant getMinimumInstance() {
        return MINIMUM;
    }
    /**
     * infinite future date.
     * @return an instant that will compare as OLDER than anything but itself
     */

    public static TemporalInstant getMaximumInstance() {
        return MAXIMUM;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        return getAsKeyString().hashCode();
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final TemporalInstantRfc3339 other = (TemporalInstantRfc3339) obj;
        return (getAsKeyString().equals(other.getAsKeyString()));
    }
}
