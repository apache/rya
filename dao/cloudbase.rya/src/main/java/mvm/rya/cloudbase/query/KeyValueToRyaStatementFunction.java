package mvm.rya.cloudbase.query;

import cloudbase.core.data.Key;
import cloudbase.core.data.Value;
import com.google.common.base.Function;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.resolver.RyaContext;
import mvm.rya.api.resolver.triple.TripleRow;
import mvm.rya.api.resolver.triple.TripleRowResolverException;

import java.util.Map;

import static mvm.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;

/**
 * Date: 1/30/13
 * Time: 2:09 PM
 */
public class KeyValueToRyaStatementFunction implements Function<Map.Entry<Key, Value>, RyaStatement> {

    private TABLE_LAYOUT tableLayout;

    public KeyValueToRyaStatementFunction(TABLE_LAYOUT tableLayout) {
        this.tableLayout = tableLayout;
    }

    @Override
    public RyaStatement apply(Map.Entry<Key, Value> input) {
        Key key = input.getKey();
        Value value = input.getValue();
        RyaStatement statement = null;
        try {
            statement = RyaContext.getInstance().deserializeTriple(tableLayout,
                    new TripleRow(key.getRowData().toArray(),
                            key.getColumnFamilyData().toArray(),
                            key.getColumnQualifierData().toArray(),
                            key.getTimestamp(),
                            key.getColumnVisibilityData().toArray(),
                            (value != null) ? value.get() : null
                    ));
        } catch (TripleRowResolverException e) {
            throw new RuntimeException(e);
        }

        return statement;
    }
}
