package mvm.rya.cloudbase;

import cloudbase.core.data.Mutation;
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
import java.util.HashMap;
import java.util.Map;

import static mvm.rya.api.RdfCloudTripleStoreConstants.EMPTY_TEXT;
import static mvm.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;
import static mvm.rya.cloudbase.CloudbaseRdfConstants.EMPTY_CV;
import static mvm.rya.cloudbase.CloudbaseRdfConstants.EMPTY_VALUE;

public class RyaTableMutationsFactory {

    RyaContext ryaContext = RyaContext.getInstance();

    public RyaTableMutationsFactory() {
    }

    //TODO: Does this still need to be collections
    public Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize(
            RyaStatement stmt) throws IOException {

        Collection<Mutation> spo_muts = new ArrayList<Mutation>();
        Collection<Mutation> po_muts = new ArrayList<Mutation>();
        Collection<Mutation> osp_muts = new ArrayList<Mutation>();
        /**
         * TODO: If there are contexts, do we still replicate the information into the default graph as well
         * as the named graphs?
         */
        try {
            Map<TABLE_LAYOUT, TripleRow> rowMap = ryaContext.serializeTriple(stmt);
            TripleRow tripleRow = rowMap.get(TABLE_LAYOUT.SPO);
            spo_muts.add(createMutation(tripleRow));
            tripleRow = rowMap.get(TABLE_LAYOUT.PO);
            po_muts.add(createMutation(tripleRow));
            tripleRow = rowMap.get(TABLE_LAYOUT.OSP);
            osp_muts.add(createMutation(tripleRow));
        } catch (TripleRowResolverException fe) {
            throw new IOException(fe);
        }

        Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> mutations =
                new HashMap<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>>();
        mutations.put(RdfCloudTripleStoreConstants.TABLE_LAYOUT.SPO, spo_muts);
        mutations.put(RdfCloudTripleStoreConstants.TABLE_LAYOUT.PO, po_muts);
        mutations.put(RdfCloudTripleStoreConstants.TABLE_LAYOUT.OSP, osp_muts);

        return mutations;
    }

    protected Mutation createMutation(TripleRow tripleRow) {
        Mutation mutation = new Mutation(new Text(tripleRow.getRow()));
        byte[] columnVisibility = tripleRow.getColumnVisibility();
        ColumnVisibility cv = columnVisibility == null ? EMPTY_CV : new ColumnVisibility(columnVisibility);
        Long timestamp = tripleRow.getTimestamp();
        timestamp = timestamp == null ? 0l : timestamp;
        byte[] value = tripleRow.getValue();
        Value v = value == null ? EMPTY_VALUE : new Value(value);
        byte[] columnQualifier = tripleRow.getColumnQualifier();
        Text cqText = columnQualifier == null ? EMPTY_TEXT : new Text(columnQualifier);
        byte[] columnFamily = tripleRow.getColumnFamily();
        Text cfText = columnFamily == null ? EMPTY_TEXT : new Text(columnFamily);

        mutation.put(cfText,cqText, cv, timestamp, v);
        return mutation;
    }
}