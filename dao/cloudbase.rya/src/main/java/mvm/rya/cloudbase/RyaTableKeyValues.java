package mvm.rya.cloudbase;

import cloudbase.core.data.Key;
import cloudbase.core.data.Value;
import cloudbase.core.security.ColumnVisibility;
import mvm.rya.api.RdfCloudTripleStoreConstants;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.domain.RyaType;
import mvm.rya.api.domain.RyaURI;
import mvm.rya.api.resolver.RyaContext;
import mvm.rya.api.resolver.triple.TripleRow;
import mvm.rya.api.resolver.triple.TripleRowResolverException;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import static java.util.AbstractMap.SimpleEntry;
import static mvm.rya.cloudbase.CloudbaseRdfConstants.EMPTY_CV;
import static mvm.rya.cloudbase.CloudbaseRdfConstants.EMPTY_VALUE;

public class RyaTableKeyValues {
    public static final ColumnVisibility EMPTY_CV = new ColumnVisibility();
    public static final Text EMPTY_CV_TEXT = new Text(EMPTY_CV.getExpression());

    RyaContext instance = RyaContext.getInstance();

    private RyaStatement stmt;
    private Collection<Map.Entry<Key, Value>> spo = new ArrayList<Map.Entry<Key, Value>>();
    private Collection<Map.Entry<Key, Value>> po = new ArrayList<Map.Entry<Key, Value>>();
    private Collection<Map.Entry<Key, Value>> osp = new ArrayList<Map.Entry<Key, Value>>();

    public RyaTableKeyValues(RyaStatement stmt) {
        this.stmt = stmt;
    }

    public Collection<Map.Entry<Key, Value>> getSpo() {
        return spo;
    }

    public Collection<Map.Entry<Key, Value>> getPo() {
        return po;
    }

    public Collection<Map.Entry<Key, Value>> getOsp() {
        return osp;
    }

    public RyaTableKeyValues invoke() throws IOException {
        /**
         * TODO: If there are contexts, do we still replicate the information into the default graph as well
         * as the named graphs?
         */try {
            Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, mvm.rya.api.resolver.triple.TripleRow> rowMap = instance.serializeTriple(stmt);
            TripleRow tripleRow = rowMap.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.SPO);
            byte[] columnVisibility = tripleRow.getColumnVisibility();
            Text cv = columnVisibility == null ? EMPTY_CV_TEXT : new Text(columnVisibility);
            Long timestamp = tripleRow.getTimestamp();
            timestamp = timestamp == null ? 0l : timestamp;
            byte[] value = tripleRow.getValue();
            Value v = value == null ? EMPTY_VALUE : new Value(value);
            spo.add(new SimpleEntry(new Key(new Text(tripleRow.getRow()),
                    new Text(tripleRow.getColumnFamily()),
                    new Text(tripleRow.getColumnQualifier()),
                    cv, timestamp), v));
            tripleRow = rowMap.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.PO);
            po.add(new SimpleEntry(new Key(new Text(tripleRow.getRow()),
                    new Text(tripleRow.getColumnFamily()),
                    new Text(tripleRow.getColumnQualifier()),
                    cv, timestamp), v));
            tripleRow = rowMap.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.OSP);
            osp.add(new SimpleEntry(new Key(new Text(tripleRow.getRow()),
                    new Text(tripleRow.getColumnFamily()),
                    new Text(tripleRow.getColumnQualifier()),
                    cv, timestamp), v));
        } catch (TripleRowResolverException e) {
            throw new IOException(e);
        }
        return this;
    }

    @Override
    public String toString() {
        return "RyaTableKeyValues{" +
                "statement=" + stmt +
                ", spo=" + spo +
                ", po=" + po +
                ", o=" + osp +
                '}';
    }
}