package mvm.rya.cloudbase;

import cloudbase.core.data.Key;
import cloudbase.core.data.Value;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

/**
 * The intention of this iterator is the wrap the iterator that is returned by a
 * BatchScan in cloudbase in order to serve as a workaround for
 * ACCUMULO-226 (https://issues.apache.org/jira/browse/ACCUMULO-226).  The bug
 * involves subsequent calls to hasNext() on batch scan results after false has been
 * returned will return true
 * <p/>
 * A patch has been submitted and accepted in Accumulo but this wrapper can be used
 * for previous versions of Cloudbase/Accumulo that do not yet have the patch.
 */
public class BatchScannerIterator implements Iterator<Entry<Key, Value>> {

    private Iterator<Entry<Key, Value>> cloudbaseScanner = null;

    private Entry<Key, Value> nextKeyValue = null;

    public BatchScannerIterator(Iterator<Entry<Key, Value>> cloudbaseScanner) {
        this.cloudbaseScanner = cloudbaseScanner;
    }

    public boolean hasNext() {
        if (nextKeyValue == null) {
            if (cloudbaseScanner.hasNext()) {
                nextKeyValue = cloudbaseScanner.next();
            }
        }
        return !isTerminatingKeyValue(nextKeyValue);
    }

    private boolean isTerminatingKeyValue(Entry<Key, Value> nextEntry) {
        if (nextEntry == null) {
            return true;
        }
        return !(nextEntry.getKey() != null && nextEntry.getValue() != null); //Condition taken from cloudbase's TabletServerBatchReaderIterator
    }

    public Entry<Key, Value> next() {
        if (hasNext()) {
            Entry<Key, Value> entry = nextKeyValue;
            nextKeyValue = null;
            return entry;
        } else {
            throw new NoSuchElementException();
        }
    }

    public void remove() {
        cloudbaseScanner.remove();
    }
}
