package org.apache.rya.accumulo.utils;

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



import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * Set the startTime and timeRange. The filter will only keyValues that
 * are within the range [startTime - timeRange, startTime].
 */
public class TimeRangeFilter extends Filter {
    private long timeRange;
    private long startTime;
    public static final String TIME_RANGE_PROP = "timeRange";
    public static final String START_TIME_PROP = "startTime";

    @Override
    public boolean accept(Key k, Value v) {
        long diff = startTime - k.getTimestamp();
        return !(diff > timeRange || diff < 0);
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        if (options == null) {
            throw new IllegalArgumentException("options must be set for TimeRangeFilter");
        }

        timeRange = -1;
        String timeRange_s = options.get(TIME_RANGE_PROP);
        if (timeRange_s == null)
            throw new IllegalArgumentException("timeRange must be set for TimeRangeFilter");

        timeRange = Long.parseLong(timeRange_s);

        String time = options.get(START_TIME_PROP);
        if (time != null)
            startTime = Long.parseLong(time);
        else
            startTime = System.currentTimeMillis();
    }

    @Override
    public OptionDescriber.IteratorOptions describeOptions() {
        Map<String, String> options = new TreeMap<String, String>();
        options.put(TIME_RANGE_PROP, "time range from the startTime (milliseconds)");
        options.put(START_TIME_PROP, "if set, use the given value as the absolute time in milliseconds as the start time in the time range.");
        return new OptionDescriber.IteratorOptions("timeRangeFilter", "TimeRangeFilter removes entries with timestamps outside of the given time range: " +
                "[startTime - timeRange, startTime]",
                options, null);
    }

    @Override
    public boolean validateOptions(Map<String, String> options) {
        Long.parseLong(options.get(TIME_RANGE_PROP));
        return true;
    }
}
