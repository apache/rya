package mvm.rya.cloudbase;

import cloudbase.core.client.CBException;
import cloudbase.core.client.CBSecurityException;
import cloudbase.core.client.TableExistsException;
import cloudbase.core.client.admin.TableOperations;
import cloudbase.core.data.Key;
import cloudbase.core.data.Value;
import mvm.rya.api.resolver.triple.TripleRow;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import static mvm.rya.api.RdfCloudTripleStoreConstants.EMPTY_BYTES;

/**
 * Class CloudbaseRdfUtils
 * Date: Mar 1, 2012
 * Time: 7:15:54 PM
 */
public class CloudbaseRdfUtils {
    private static final Log logger = LogFactory.getLog(CloudbaseRyaDAO.class);

    public static void createTableIfNotExist(TableOperations tableOperations, String tableName) throws TableExistsException, CBSecurityException, CBException {
        boolean tableExists = tableOperations.exists(tableName);
        if (!tableExists) {
            logger.info("Creating cloudbase table: " + tableName);
            tableOperations.create(tableName);
        }
    }

    public static Key from(TripleRow tripleRow) {
        return new Key(defaultTo(tripleRow.getRow(), EMPTY_BYTES),
                defaultTo(tripleRow.getColumnFamily(), EMPTY_BYTES),
                defaultTo(tripleRow.getColumnQualifier(), EMPTY_BYTES),
                defaultTo(tripleRow.getColumnVisibility(), EMPTY_BYTES),
                defaultTo(tripleRow.getTimestamp(), Long.MAX_VALUE));
    }

    public static Value extractValue(TripleRow tripleRow) {
        return new Value(defaultTo(tripleRow.getValue(), EMPTY_BYTES));
    }

    private static byte[] defaultTo(byte[] bytes, byte[] def) {
        return bytes != null ? bytes : def;
    }

    private static Long defaultTo(Long l, Long def) {
        return l != null ? l : def;
    }
}
