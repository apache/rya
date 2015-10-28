package mvm.rya.cloudbase.query;

import cloudbase.core.data.Key;
import cloudbase.core.data.Range;
import org.openrdf.query.BindingSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

/**
 * Class RangeBindingSetCollection
 * Date: Feb 23, 2011
 * Time: 10:15:48 AM
 */
public class RangeBindingSetEntries {
    public Collection<Map.Entry<Range, BindingSet>> ranges;

    public RangeBindingSetEntries() {
        this(new ArrayList<Map.Entry<Range, BindingSet>>());
    }

    public RangeBindingSetEntries(Collection<Map.Entry<Range, BindingSet>> ranges) {
        this.ranges = ranges;
    }

    public Collection<BindingSet> containsKey(Key key) {
        //TODO: need to find a better way to sort these and pull
        //TODO: maybe fork/join here
        Collection<BindingSet> bss = new ArrayList<BindingSet>();
        for (Map.Entry<Range, BindingSet> entry : ranges) {
            if (entry.getKey().contains(key))
                bss.add(entry.getValue());
        }
        return bss;
    }
}
