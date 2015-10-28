package mvm.rya.cloudbase.utils.filters;

import cloudbase.core.data.Key;
import cloudbase.core.data.Value;
import cloudbase.core.iterators.OptionDescriber;
import cloudbase.core.iterators.filter.Filter;

import java.util.Map;
import java.util.TreeMap;

/**
 * Set the startTime and timeRange. The filter will only keyValues that
 * are within the range [startTime - timeRange, startTime].
 *
 * @deprecated Use the LimitingAgeOffFilter
 */
public class TimeRangeFilter implements Filter, OptionDescriber {
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
    public void init(Map<String, String> options) {
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
    public IteratorOptions describeOptions() {
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