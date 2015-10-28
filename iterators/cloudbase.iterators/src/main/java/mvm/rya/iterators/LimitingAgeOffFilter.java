package mvm.rya.iterators;

import cloudbase.core.data.Key;
import cloudbase.core.data.Value;
import cloudbase.core.iterators.OptionDescriber;
import cloudbase.core.iterators.filter.Filter;

import java.util.Map;
import java.util.TreeMap;

/**
 * A small modification of the age off filter that ships with Accumulo which ages off key/value pairs based on the
 * Key's timestamp. It removes an entry if its timestamp is less than currentTime - threshold.
 * <p/>
 * The modification will now allow rows with timestamp > currentTime to pass through.
 * <p/>
 * This filter requires a "ttl" option, in milliseconds, to determine the age off threshold.
 */
public class LimitingAgeOffFilter implements Filter, OptionDescriber {

    public static final String TTL = "ttl";
    public static final String CURRENT_TIME = "currentTime";

    protected long threshold;

    /**
     * The use of private for this member in the original AgeOffFilter wouldn't allow me to extend it. Setting to protected.
     */
    protected long currentTime;

    @Override
    public boolean accept(Key k, Value v) {
        long diff = currentTime - k.getTimestamp();
        return !(diff > threshold || diff < 0);
    }

    @Override
    public void init(Map<String, String> options) {
        threshold = -1;
        if (options == null)
            throw new IllegalArgumentException(TTL + " must be set for LimitingAgeOffFilter");

        String ttl = options.get(TTL);
        if (ttl == null)
            throw new IllegalArgumentException(TTL + " must be set for LimitingAgeOffFilter");

        threshold = Long.parseLong(ttl);

        String time = options.get(CURRENT_TIME);
        if (time != null)
            currentTime = Long.parseLong(time);
        else
            currentTime = System.currentTimeMillis();

        // add sanity checks for threshold and currentTime?
    }

    @Override
    public IteratorOptions describeOptions() {
        Map<String, String> options = new TreeMap<String, String>();
        options.put(TTL, "time to live (milliseconds)");
        options.put(CURRENT_TIME, "if set, use the given value as the absolute time in milliseconds as the current time of day");
        return new OptionDescriber.IteratorOptions("limitingAgeOff", "LimitingAgeOffFilter removes entries with timestamps more than <ttl> milliseconds old & timestamps newer than currentTime",
                options, null);
    }

    @Override
    public boolean validateOptions(Map<String, String> options) {
        try {
            Long.parseLong(options.get(TTL));
        } catch (NumberFormatException e) {
            return false;
        }
        return true;
    }
}