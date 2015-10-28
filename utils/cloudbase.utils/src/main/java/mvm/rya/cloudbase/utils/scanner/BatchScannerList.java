package mvm.rya.cloudbase.utils.scanner;

import cloudbase.core.client.*;
import cloudbase.core.data.Key;
import cloudbase.core.data.Range;
import cloudbase.core.data.Value;
import cloudbase.core.util.ArgumentChecker;
import com.google.common.collect.Iterators;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * Date: 4/18/12
 * Time: 11:06 AM
 * To change this template use File | Settings | File Templates.
 */
public class BatchScannerList implements BatchScanner{
    private List<BatchScanner> scanners = new ArrayList<BatchScanner>();

    public BatchScannerList(List<BatchScanner> scanners) {
        this.scanners = scanners;
    }

    //setRanges
    public void setRanges(Collection<Range> ranges) {
        ArgumentChecker.notNull(ranges);
        for(BatchScanner scanner : scanners) {
            scanner.setRanges(ranges);
        }
    }

    public Iterator<Map.Entry<Key, Value>> iterator() {
        List<Iterator<Map.Entry<Key,Value>>> iterators = new ArrayList<Iterator<Map.Entry<Key, Value>>>();
        for(BatchScanner scanner: scanners) {
            iterators.add(scanner.iterator());
        }
        return Iterators.concat(iterators.toArray(new Iterator[]{}));
    }

    public void close() {
        for(BatchScanner scanner: scanners) {
            scanner.close();
        }
    }

    public void setScanIterators(int i, String s, String s1) throws IOException {
        for(BatchScanner scanner: scanners) {
            scanner.setScanIterators(i, s, s1);
        }
    }

    public void setScanIteratorOption(String s, String s1, String s2) {
        for(BatchScanner scanner: scanners) {
            scanner.setScanIteratorOption(s, s1, s2);
        }
    }

    @Override
    public void setupRegex(String s, int i) throws IOException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setRowRegex(String s) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setColumnFamilyRegex(String s) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setColumnQualifierRegex(String s) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setValueRegex(String s) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void fetchColumnFamily(Text cf) {
        for(BatchScanner scanner: scanners) {
            scanner.fetchColumnFamily(cf);
        }
    }

    public void fetchColumn(Text cf, Text cq) {
        for(BatchScanner scanner: scanners) {
            scanner.fetchColumn(cf, cq);
        }
    }

    @Override
    public void clearColumns() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void clearScanIterators() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

}
