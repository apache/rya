package mvm.rya.cloudbase.query;

import cloudbase.core.data.Key;
import cloudbase.core.data.Value;
import info.aduna.iteration.CloseableIteration;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.persist.RyaDAOException;
import mvm.rya.api.resolver.RyaContext;
import mvm.rya.api.resolver.triple.TripleRow;
import mvm.rya.api.resolver.triple.TripleRowResolverException;

import java.util.Iterator;
import java.util.Map;

import static mvm.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;

/**
 * Date: 7/17/12
 * Time: 11:48 AM
 */
public class RyaStatementKeyValueIterator implements CloseableIteration<RyaStatement, RyaDAOException> {
    private Iterator<Map.Entry<Key, Value>> dataIterator;
    private TABLE_LAYOUT tableLayout;
    private Long maxResults = -1L;

    public RyaStatementKeyValueIterator(TABLE_LAYOUT tableLayout, Iterator<Map.Entry<Key, Value>> dataIterator) {
        this.tableLayout = tableLayout;
        this.dataIterator = dataIterator;
    }

    @Override
    public void close() throws RyaDAOException {
        dataIterator = null;
    }

    public boolean isClosed() throws RyaDAOException {
        return dataIterator == null;
    }

    @Override
    public boolean hasNext() throws RyaDAOException {
        if (isClosed()) {
            throw new RyaDAOException("Closed Iterator");
        }
        return maxResults != 0 && dataIterator.hasNext();
    }

    @Override
    public RyaStatement next() throws RyaDAOException {
        if (!hasNext()) {
            return null;
        }

        try {
            Map.Entry<Key, Value> next = dataIterator.next();
            Key key = next.getKey();
            RyaStatement statement = RyaContext.getInstance().deserializeTriple(tableLayout,
                    new TripleRow(key.getRowData().toArray(), key.getColumnFamilyData().toArray(), key.getColumnQualifierData().toArray(),
                            key.getTimestamp(), key.getColumnVisibilityData().toArray(), next.getValue().get()));
            if (next.getValue() != null) {
                statement.setValue(next.getValue().get());
            }
            maxResults--;
            return statement;
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
