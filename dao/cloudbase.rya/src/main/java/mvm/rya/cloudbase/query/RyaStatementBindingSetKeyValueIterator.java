package mvm.rya.cloudbase.query;

import cloudbase.core.client.BatchScanner;
import cloudbase.core.client.Scanner;
import cloudbase.core.client.ScannerBase;
import cloudbase.core.data.Key;
import cloudbase.core.data.Value;
import info.aduna.iteration.CloseableIteration;
import mvm.rya.api.RdfCloudTripleStoreUtils;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.persist.RyaDAOException;
import mvm.rya.api.resolver.RyaContext;
import mvm.rya.api.resolver.triple.TripleRow;
import mvm.rya.api.resolver.triple.TripleRowResolverException;
import mvm.rya.cloudbase.BatchScannerIterator;
import org.openrdf.query.BindingSet;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import static mvm.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;

/**
 * Date: 7/17/12
 * Time: 11:48 AM
 */
public class RyaStatementBindingSetKeyValueIterator implements CloseableIteration<Map.Entry<RyaStatement, BindingSet>, RyaDAOException> {
    private Iterator<Map.Entry<Key, Value>> dataIterator;
    private TABLE_LAYOUT tableLayout;
    private Long maxResults = -1L;
    private ScannerBase scanner;
    private boolean isBatchScanner;
    private RangeBindingSetEntries rangeMap;
    private Iterator<BindingSet> bsIter;
    private RyaStatement statement;

    public RyaStatementBindingSetKeyValueIterator(TABLE_LAYOUT tableLayout, ScannerBase scannerBase, RangeBindingSetEntries rangeMap) {
        this(tableLayout, ((scannerBase instanceof BatchScanner) ? new BatchScannerIterator(((BatchScanner) scannerBase).iterator()) : ((Scanner) scannerBase).iterator()), rangeMap);
        this.scanner = scannerBase;
        isBatchScanner = scanner instanceof BatchScanner;
    }

    public RyaStatementBindingSetKeyValueIterator(TABLE_LAYOUT tableLayout, Iterator<Map.Entry<Key, Value>> dataIterator, RangeBindingSetEntries rangeMap) {
        this.tableLayout = tableLayout;
        this.rangeMap = rangeMap;
        this.dataIterator = dataIterator;
    }

    @Override
    public void close() throws RyaDAOException {
        dataIterator = null;
        if (scanner != null && isBatchScanner) {
            ((BatchScanner) scanner).close();
        }
    }

    public boolean isClosed() throws RyaDAOException {
        return dataIterator == null;
    }

    @Override
    public boolean hasNext() throws RyaDAOException {
        if (isClosed()) {
            throw new RyaDAOException("Closed Iterator");
        }
        if (maxResults != 0) {
            if (bsIter != null && bsIter.hasNext()) {
                return true;
            }
            if (dataIterator.hasNext()) {
                return true;
            } else {
                maxResults = 0l;
                return false;
            }
        }
        return false;
    }

    @Override
    public Map.Entry<RyaStatement, BindingSet> next() throws RyaDAOException {
        if (!hasNext()) {
            return null;
        }

        try {
            while (true) {
                if (bsIter != null && bsIter.hasNext()) {
                    maxResults--;
                    return new RdfCloudTripleStoreUtils.CustomEntry<RyaStatement, BindingSet>(statement, bsIter.next());
                }

                if (dataIterator.hasNext()) {
                    Map.Entry<Key, Value> next = dataIterator.next();
                    Key key = next.getKey();
                    statement = RyaContext.getInstance().deserializeTriple(tableLayout,
                            new TripleRow(key.getRowData().toArray(), key.getColumnFamilyData().toArray(), key.getColumnQualifierData().toArray(),
                                    key.getTimestamp(), key.getColumnVisibilityData().toArray(), next.getValue().get()));
                    if (next.getValue() != null) {
                        statement.setValue(next.getValue().get());
                    }
                    Collection<BindingSet> bindingSets = rangeMap.containsKey(key);
                    if (!bindingSets.isEmpty()) {
                        bsIter = bindingSets.iterator();
                    }
                } else {
                    break;
                }
            }
            return null;
        } catch (TripleRowResolverException e) {
            throw new RyaDAOException(e);
        }
    }

    @Override
    public void remove() throws RyaDAOException {
        next();
    }

    public Long getMaxResults() {
        return maxResults;
    }

    public void setMaxResults(Long maxResults) {
        this.maxResults = maxResults;
    }
}
