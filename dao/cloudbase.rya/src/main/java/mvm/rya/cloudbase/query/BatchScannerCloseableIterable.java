package mvm.rya.cloudbase.query;

import cloudbase.core.client.BatchScanner;
import cloudbase.core.data.Key;
import cloudbase.core.data.Value;
import com.google.common.base.Preconditions;
import mango.collect.AbstractCloseableIterable;
import mvm.rya.cloudbase.BatchScannerIterator;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 */
public class BatchScannerCloseableIterable extends AbstractCloseableIterable<Map.Entry<Key, Value>> {

    private BatchScanner scanner;

    public BatchScannerCloseableIterable(BatchScanner scanner) {
        Preconditions.checkNotNull(scanner);
        this.scanner = scanner;
    }

    @Override
    protected void doClose() throws IOException {
        scanner.close();
    }

    @Override
    protected Iterator<Map.Entry<Key, Value>> retrieveIterator() {
        return new BatchScannerIterator(scanner.iterator());
    }
}
