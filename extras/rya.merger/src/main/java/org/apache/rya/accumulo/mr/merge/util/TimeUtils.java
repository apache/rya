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

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.net.ntp.NTPUDPClient;
import org.apache.commons.net.ntp.TimeInfo;
import org.codehaus.plexus.util.StringUtils;
import org.mortbay.jetty.HttpMethods;

import com.google.common.net.HttpHeaders;

import twitter4j.Logger;

/**
 * Utility methods for time.
 */
public final class TimeUtils {
    private static final Logger log = Logger.getLogger(TimeUtils.class);

    /**
     * The default host name of the time server to use.
     * List of time servers: http://tf.nist.gov/tf-cgi/servers.cgi
     * Do not query time server more than once every 4 seconds.
     */
    public static final String DEFAULT_TIME_SERVER_HOST = "time.nist.gov";

    private static final int NTP_SERVER_TIMEOUT_MS = 15000;

    /**
     * Queries the default NTP Server for the time.
     * Do not query time server more than once every 4 seconds.
     * @return the NTP server {@link Date} or {@code null}.
     * @throws IOException
     */
    public static Date getDefaultNtpServerDate() throws IOException {
        return getNtpServerDate(DEFAULT_TIME_SERVER_HOST);
    }

    /**
     * Queries the specified NTP Server for the time.
     * Do not query time server more than once every 4 seconds.
     * @param timeServerHost the time server host name.
     * @return the NTP server {@link Date} or {@code null}.
     * @throws IOException
     */
    public static Date getNtpServerDate(final String timeServerHost) throws IOException {
        try {
            TimeInfo timeInfo = null;
            final NTPUDPClient timeClient = new NTPUDPClient();
            timeClient.setDefaultTimeout(NTP_SERVER_TIMEOUT_MS);
            final InetAddress inetAddress = InetAddress.getByName(timeServerHost);
            if (inetAddress != null) {
                timeInfo = timeClient.getTime(inetAddress);
                if (timeInfo != null) {
                    // TODO: which time to use?
                    final long serverTime = timeInfo.getMessage().getTransmitTimeStamp().getTime();
                    //long serverTime = timeInfo.getReturnTime();
                    final Date ntpDate = new Date(serverTime);
                    return ntpDate;
                }
            }
        } catch (final IOException e) {
            throw new IOException("Unable to get NTP server time.", e);
        }
        return null;
    }

    /**
     * Gets the remote machine's system time by checking the DATE field in the header
     * from a HTTP HEAD method response.
     * @param urlString the URL string of the remote machine's web server to connect to.
     * @return the remote machine's system {@link Date} or {@code null}.
     * @throws IOException
     * @throws ParseException
     */
    public static Date getRemoteMachineDate(final String urlString) throws IOException, ParseException {
        Date remoteDate = null;
        HttpURLConnection conn = null;
        try {
            final URL url = new URL(urlString);

            // Set up the initial connection
            conn = (HttpURLConnection)url.openConnection();
            // Use HEAD instead of GET so content isn't returned.
            conn.setRequestMethod(HttpMethods.HEAD);
            conn.setDoOutput(false);
            conn.setReadTimeout(10000);

            conn.connect();

            final Map<String, List<String>> header = conn.getHeaderFields();
            for (final String key : header.keySet()) {
                if (key != null && HttpHeaders.DATE.equals(key)) {
                    final List<String> data = header.get(key);
                    final String dateString = data.get(0);
                    final SimpleDateFormat sdf = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss Z");
                    remoteDate = sdf.parse(dateString);
                    break;
                }
            }
        } finally {
            // Close the connection
            conn.disconnect();
        }

        return remoteDate;
    }

    /**
     * Gets the time difference between the 2 specified times from the NTP server time and the machine system time.
     * @param ntpDate the {@link Date} from the NTP server host.
     * @param machineDate the {@link Date} from the machine (either local or remote).
     * @param isMachineLocal {@code true} if the {@code machineDate} from a local machine.  {@code false}
     * if it's from a remote machine.
     * @return the difference between the NTP server time and the machine's system time.  A positive value
     * indicates that the machine's system time is ahead of the time server.  A negative value indicates that
     * the machine's system time is behind of the time server.
     */
    public static Long getTimeDifference(final Date ntpDate, final Date machineDate, final boolean isMachineLocal) {
        Long diff = null;
        if (ntpDate != null && machineDate != null) {
            log.info("NTP Server Time: " + ntpDate);
            final String machineLabel = isMachineLocal ? "Local" : "Remote";
            log.info(machineLabel + " Machine Time: " + machineDate);
            diff = machineDate.getTime() - ntpDate.getTime();

            final boolean isAhead = diff > 0;
            final String durationBreakdown = TimeUtils.getDurationBreakdown(diff, false);
            log.info(machineLabel + " Machine time is " + (isAhead ? "ahead of" : "behind") + " NTP server time by " + durationBreakdown + ".");
        }

        return diff;
    }

    /**
     * Gets the time difference between the NTP server and the local machine system time.
     * @param timeServerHost the time server host name.
     * @return the difference between the NTP server time and the local machine's system time.  A positive value
     * indicates that the local machine's system time is ahead of the time server.  A negative value indicates that
     * the local machine's system time is behind of the time server.
     * @throws IOException
     */
    public static Long getNtpServerAndLocalMachineTimeDifference(final String timeServerHost) throws IOException {
        log.info("Getting NTP Server time from " + timeServerHost + "...");
        final Date ntpDate = getNtpServerDate(timeServerHost);
        Long diff = null;
        if (ntpDate != null) {
            log.info("Getting Local Machine time...");
            final Date machineDate = new Date();

            diff = getTimeDifference(ntpDate, machineDate, true);
        }

        return diff;
    }

    /**
     * Gets the time difference between the NTP server and the remote machine system time.
     * @param timeServerHost the time server host name.
     * @param remoteMachineUrlString the URL string of the remote machine's web server to connect to.
     * @return the difference between the NTP server time and the remote machine's system time.  A positive value
     * indicates that the remote machine's system time is ahead the time server.  A negative value indicates that
     * the remote machine's system time is behind the time server.
     * @throws ParseException
     * @throws IOException
     */
    public static Long getNtpServerAndRemoteMachineTimeDifference(final String timeServerHost, final String remoteMachineUrlString) throws IOException, ParseException {
        log.info("Getting NTP Server time from " + timeServerHost + "...");
        final Date ntpDate = getNtpServerDate(timeServerHost);
        Long diff = null;
        if (ntpDate != null) {
            log.info("Getting Remote Machine time from " + remoteMachineUrlString + "...");
            final Date machineDate = getRemoteMachineDate(remoteMachineUrlString);

            diff = getTimeDifference(ntpDate, machineDate, false);
        }

        return diff;
    }

    /**
     * Gets the time difference between the NTP server and the machine system time (either locally or remotely depending on the URL).
     * @param timeServerHost the time server host name.
     * @param machineUrlString the URL string of the machine's web server to connect to.  The machine might be
     * local or remote.
     * @return the difference between the NTP server time and the machine's system time.  A positive value
     * indicates that the machine's system time is ahead of the time server.  A negative value indicates that
     * the machine's system time is behind the time server.
     * @throws ParseException
     * @throws IOException
     */
    public static Long getNtpServerAndMachineTimeDifference(final String timeServerHost, final String machineUrlString) throws IOException, ParseException {
        final boolean isUrlLocalMachine = isUrlLocalMachine(machineUrlString);

        Long machineTimeOffset;
        if (isUrlLocalMachine) {
            machineTimeOffset = getNtpServerAndLocalMachineTimeDifference(timeServerHost);
        } else {
            machineTimeOffset = getNtpServerAndRemoteMachineTimeDifference(timeServerHost, machineUrlString);
        }

        return machineTimeOffset;
    }

    /**
     * Gets the machine system time (either locally or remotely depending on the URL).
     * @param urlString the URL string of the machine to check.
     * @return the machine's system time.
     * @throws IOException
     * @throws ParseException
     */
    public static Date getMachineDate(final String urlString) throws IOException, ParseException {
        final boolean isMachineLocal = isUrlLocalMachine(urlString);

        Date machineDate;
        if (isMachineLocal) {
            // Get local system machine time
            machineDate = new Date();
        } else {
            // Get remote machine time from HTTP HEAD response.  Check hosted server web page on machine for time.
            machineDate = getRemoteMachineDate(urlString);
        }

        return machineDate;
    }

    /**
     * Checks to see if the URL provided is hosted on the local machine or not.
     * @param urlString the URL string to check.
     * @return {@code true} if the URL is hosted on the local machine.  {@code false}
     * if it's on a remote machine.
     * @throws UnknownHostException
     * @throws MalformedURLException
     */
    public static boolean isUrlLocalMachine(final String urlString) throws UnknownHostException, MalformedURLException {
        final String localAddress = InetAddress.getLocalHost().getHostAddress();
        final String requestAddress = InetAddress.getByName(new URL(urlString).getHost()).getHostAddress();
        return localAddress != null && requestAddress != null && localAddress.equals(requestAddress);
    }

    /**
     * Convert a millisecond duration to a string format.
     * @param durationMs A duration to convert to a string form.
     * @return A string of the form "X Days Y Hours Z Minutes A Seconds B Milliseconds".
     */
    public static String getDurationBreakdown(final long durationMs) {
        return getDurationBreakdown(durationMs, true);
    }

    /**
     * Convert a millisecond duration to a string format.
     * @param durationMs A duration to convert to a string form.
     * @param showSign {@code true} to show if the duration is positive or negative. {@code false}
     * to not display the sign.
     * @return A string of the form "X Days Y Hours Z Minutes A Seconds B Milliseconds".
     */
    public static String getDurationBreakdown(final long durationMs, final boolean showSign) {
        long tempDurationMs = Math.abs(durationMs);

        final long days = TimeUnit.MILLISECONDS.toDays(tempDurationMs);
        tempDurationMs -= TimeUnit.DAYS.toMillis(days);
        final long hours = TimeUnit.MILLISECONDS.toHours(tempDurationMs);
        tempDurationMs -= TimeUnit.HOURS.toMillis(hours);
        final long minutes = TimeUnit.MILLISECONDS.toMinutes(tempDurationMs);
        tempDurationMs -= TimeUnit.MINUTES.toMillis(minutes);
        final long seconds = TimeUnit.MILLISECONDS.toSeconds(tempDurationMs);
        tempDurationMs -= TimeUnit.SECONDS.toMillis(seconds);
        final long milliseconds = TimeUnit.MILLISECONDS.toMillis(tempDurationMs);

        final StringBuilder sb = new StringBuilder();
        if (tempDurationMs != 0 && showSign) {
            sb.append(tempDurationMs > 0 ? "+" : "-");
        }
        if (days > 0) {
            sb.append(days);
            sb.append(days == 1 ? " Day " : " Days ");
        }
        if (hours > 0) {
            sb.append(hours);
            sb.append(hours == 1 ? " Hour " : " Hours ");
        }
        if (minutes > 0) {
            sb.append(minutes);
            sb.append(minutes == 1 ? " Minute " : " Minutes ");
        }
        if (seconds > 0) {
            sb.append(seconds);
            sb.append(seconds == 1 ? " Second " : " Seconds " );
        }
        if (milliseconds > 0 || (!showSign && sb.length() == 0) || (showSign && sb.length() == 1)) {
            // At least show the milliseconds if nothing else has been shown so far
            sb.append(milliseconds);
            sb.append(milliseconds == 1 ? " Millisecond" : " Milliseconds");
        }

        return StringUtils.trim(sb.toString());
    }

    /**
     * Checks if a date is before another date or if they are equal.
     * @param date1 the first {@link Date}.
     * @param date2 the second {@link Date}.
     * @return {@code true} if {@code date1} is before or equal to {@code date2}.  {@code false} otherwise.
     */
    public static boolean dateBeforeInclusive(final Date date1, final Date date2) {
        return !date1.after(date2);
    }

    /**
     * Checks if a date is after another date or if they are equal.
     * @param date1 the first {@link Date}.
     * @param date2 the second {@link Date}.
     * @return {@code true} if {@code date1} is after or equal to {@code date2}.  {@code false} otherwise.
     */
    public static boolean dateAfterInclusive(final Date date1, final Date date2) {
        return !date1.before(date2);
    }
}
