package mvm.rya.cloudbase.query;

import cloudbase.core.client.Scanner;
import cloudbase.core.data.Key;
import cloudbase.core.data.Value;
import com.google.common.base.Preconditions;
import mango.collect.AbstractCloseableIterable;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Date: 1/30/13
 * Time: 2:15 PM
 */
public class ScannerCloseableIterable extends AbstractCloseableIterable<Map.Entry<Key, Value>> {

    protected Scanner scanner;

    public ScannerCloseableIterable(Scanner scanner) {
        Preconditions.checkNotNull(scanner);
        this.scanner = scanner;
    }

    @Override
    protected void doClose() throws IOException {
        //do nothing
    }

    @Override
    protected Iterator<Map.Entry<Key, Value>> retrieveIterator() {
        return scanner.iterator();
    }
}
