package ss.cloudbase.core.iterators.ext;

import cloudbase.core.data.Key;
import cloudbase.core.data.Value;
import cloudbase.core.iterators.IteratorEnvironment;
import cloudbase.core.iterators.SortedKeyValueIterator;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.Text;
import ss.cloudbase.core.iterators.SortedRangeIterator;

import java.io.IOException;
import java.util.Map;

/**
 * Class EncodedSortedRangeIterator
 * Date: Aug 10, 2011
 * Time: 10:37:28 AM
 */
public class EncodedSortedRangeIterator extends SortedRangeIterator {

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        if (options.containsKey(OPTION_LOWER_BOUND)) {
            lower = new Text(decode(options.get(OPTION_LOWER_BOUND)));
        } else {
            lower = new Text("\u0000");
        }

        if (options.containsKey(OPTION_UPPER_BOUND)) {
            upper = new Text(decode(options.get(OPTION_UPPER_BOUND)));
        } else {
            upper = new Text("\u0000");
        }
    }

    public static String encode(String str) {
        return new String(Base64.encodeBase64(str.getBytes()));
    }

    public static String decode(String str) {
        return new String(Base64.decodeBase64(str.getBytes()));
    }
}
