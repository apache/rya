package mvm.rya.cloudbase.utils.scanner;

import cloudbase.core.client.BatchScanner;
import cloudbase.core.client.Connector;
import cloudbase.core.client.Scanner;
import cloudbase.core.data.Key;
import cloudbase.core.data.Range;
import cloudbase.core.data.Value;
import cloudbase.core.security.Authorizations;
import com.google.common.collect.Iterators;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.*;

/**
 * This class will decorate a List of Scanners and treat it as one BatchScanner.
 * Each Scanner in the List corresponds to a separate Range.
 * The reason we are doing this and not just using BatchScanner is because,
 * the Scanner class will return information sorted and is more performant on
 * larger amounts of data.
 */
public class RangesScanner implements BatchScanner, Scanner {

    private List<Scanner> scanners = new ArrayList<Scanner>();
    private Connector connector;
    private String table;
    private Authorizations authorizations;

    public RangesScanner(Connector connector, String table, Authorizations authorizations) {
        this.connector = connector;
        this.table = table;
        this.authorizations = authorizations;
    }

    @Override
    public void setRanges(Collection<Range> ranges) {
        try {
            scanners.clear(); //no need to close them since Scanners do their own cleanup
            for (Range range : ranges) {
                Scanner scanner = connector.createScanner(table, authorizations);
                scanner.setRange(range);
            }
        } catch (Exception e) {
            throw new RuntimeException(e); //TODO: Better exception handling
        }
    }

    @Override
    public void setTimeOut(int i) {
        if(scanners.size() == 0) {
            throw new IllegalArgumentException("Set Ranges first to initalize underneath scanners"); //TODO: if we save this info we don't need this check
        }
        for(Scanner scanner: scanners) {
            scanner.setTimeOut(i);
        }
    }

    @Override
    public int getTimeOut() {
        return 0;
    }

    @Override
    public void setRange(Range range) {
        //TODO: How to set only one range
    }

    @Override
    public Range getRange() {
        return null; //TODO: How to get only one range
    }

    @Override
    public void setBatchSize(int i) {
        if(scanners.size() == 0) {
            throw new IllegalArgumentException("Set Ranges first to initalize underneath scanners"); //TODO: if we save this info we don't need this check
        }
        for(Scanner scanner: scanners) {
            scanner.setBatchSize(i);
        }
    }

    @Override
    public int getBatchSize() {
        return 0; //TODO: What does this mean with multiple scanners?
    }

    @Override
    public void enableIsolation() {
        if(scanners.size() == 0) {
            throw new IllegalArgumentException("Set Ranges first to initalize underneath scanners"); //TODO: if we save this info we don't need this check
        }
        for(Scanner scanner: scanners) {
            scanner.enableIsolation();
        }
    }

    @Override
    public void disableIsolation() {
        if(scanners.size() == 0) {
            throw new IllegalArgumentException("Set Ranges first to initalize underneath scanners"); //TODO: if we save this info we don't need this check
        }
        for(Scanner scanner: scanners) {
            scanner.disableIsolation();
        }
    }

    @Override
    public Iterator<Map.Entry<Key, Value>> iterator() {
        //TODO: Lazy load iterator to only open the next scanner iterator after the first one is done
        if(scanners.size() == 0) {
            throw new IllegalArgumentException("Set Ranges first to initalize underneath scanners"); //TODO: if we save this info we don't need this check
        }
        List<Iterator<Map.Entry<Key,Value>>> iterators = new ArrayList<Iterator<Map.Entry<Key, Value>>>();
        for(Scanner scanner: scanners) {
            iterators.add(scanner.iterator());
        }
        return Iterators.concat(iterators.toArray(new Iterator[]{}));
    }

    @Override
    public void close() {
        //scanners do not close
    }

    @Override
    public void setScanIterators(int i, String s, String s1) throws IOException {
        if(scanners.size() == 0) {
            throw new IllegalArgumentException("Set Ranges first to initalize underneath scanners"); //TODO: if we save this info we don't need this check
        }
        for(Scanner scanner: scanners) {
            scanner.setScanIterators(i, s, s1);
        }
    }

    @Override
    public void setScanIteratorOption(String s, String s1, String s2) {
        if(scanners.size() == 0) {
            throw new IllegalArgumentException("Set Ranges first to initalize underneath scanners"); //TODO: if we save this info we don't need this check
        }
        for(Scanner scanner: scanners) {
            scanner.setScanIteratorOption(s, s1, s2);
        }
    }

    @Override
    public void setupRegex(String s, int i) throws IOException {
        if(scanners.size() == 0) {
            throw new IllegalArgumentException("Set Ranges first to initalize underneath scanners"); //TODO: if we save this info we don't need this check
        }
        for(Scanner scanner: scanners) {
            scanner.setupRegex(s, i);
        }
    }

    @Override
    public void setRowRegex(String s) {
        if(scanners.size() == 0) {
            throw new IllegalArgumentException("Set Ranges first to initalize underneath scanners"); //TODO: if we save this info we don't need this check
        }
        for(Scanner scanner: scanners) {
            scanner.setRowRegex(s);
        }
    }

    @Override
    public void setColumnFamilyRegex(String s) {
        if(scanners.size() == 0) {
            throw new IllegalArgumentException("Set Ranges first to initalize underneath scanners"); //TODO: if we save this info we don't need this check
        }
        for(Scanner scanner: scanners) {
            scanner.setColumnFamilyRegex(s);
        }
    }

    @Override
    public void setColumnQualifierRegex(String s) {
        if(scanners.size() == 0) {
            throw new IllegalArgumentException("Set Ranges first to initalize underneath scanners"); //TODO: if we save this info we don't need this check
        }
        for(Scanner scanner: scanners) {
            scanner.setColumnQualifierRegex(s);
        }
    }

    @Override
    public void setValueRegex(String s) {
        if(scanners.size() == 0) {
            throw new IllegalArgumentException("Set Ranges first to initalize underneath scanners"); //TODO: if we save this info we don't need this check
        }
        for(Scanner scanner: scanners) {
            scanner.setValueRegex(s);
        }
    }

    @Override
    public void fetchColumnFamily(Text text) {
        if(scanners.size() == 0) {
            throw new IllegalArgumentException("Set Ranges first to initalize underneath scanners"); //TODO: if we save this info we don't need this check
        }
        for(Scanner scanner: scanners) {
            scanner.fetchColumnFamily(text);
        }
    }

    @Override
    public void fetchColumn(Text text, Text text1) {
        if(scanners.size() == 0) {
            throw new IllegalArgumentException("Set Ranges first to initalize underneath scanners"); //TODO: if we save this info we don't need this check
        }
        for(Scanner scanner: scanners) {
            scanner.fetchColumn(text, text1);
        }
    }

    @Override
    public void clearColumns() {
        if(scanners.size() == 0) {
            throw new IllegalArgumentException("Set Ranges first to initalize underneath scanners"); //TODO: if we save this info we don't need this check
        }
        for(Scanner scanner: scanners) {
            scanner.clearColumns();
        }
    }

    @Override
    public void clearScanIterators() {
        if(scanners.size() == 0) {
            throw new IllegalArgumentException("Set Ranges first to initalize underneath scanners"); //TODO: if we save this info we don't need this check
        }
        for(Scanner scanner: scanners) {
            scanner.clearScanIterators();
        }
    }
}
