package mvm.rya.cloudbase.utils.shard;

import cloudbase.core.CBConstants;
import cloudbase.core.client.BatchScanner;
import cloudbase.core.client.Connector;
import cloudbase.core.client.admin.TableOperations;
import cloudbase.core.client.mock.MockInstance;
import cloudbase.core.data.Key;
import cloudbase.core.data.Mutation;
import cloudbase.core.data.Range;
import cloudbase.core.data.Value;
import com.google.common.collect.Iterators;
import junit.framework.TestCase;
import org.apache.hadoop.io.Text;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * Date: 4/18/12
 * Time: 11:59 AM
 * To change this template use File | Settings | File Templates.
 */
public class ShardedConnectorTest extends TestCase {
    public static final Text CF = new Text("cf");
    public static final Text CQ = new Text("cq");
    public static final Value EMPTY_VALUE = new Value(new byte[0]);
    private ShardedConnector shardedConnector;
    private Connector connector;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        connector = new MockInstance("shardConnector").getConnector("", "".getBytes());
        shardedConnector = new ShardedConnector(connector, 10, "tst_", null);
        shardedConnector.init();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        shardedConnector.close();
    }

    public void testTablesCreated() throws Exception {
        TableOperations tableOperations = connector.tableOperations();
        SortedSet<String> list = tableOperations.list();
        assertTrue(list.containsAll(Arrays.asList("tst_0","tst_1","tst_2","tst_3","tst_4","tst_5","tst_6","tst_7","tst_8","tst_9")));
    }
    
    public void testAddMutationByKey() throws Exception {
        Mutation mutation = new Mutation(new Text("a"));
        mutation.put(CF, CQ, EMPTY_VALUE);
        ShardedBatchWriter batchWriter = shardedConnector.createBatchWriter();
        batchWriter.addMutation(mutation, "1");
        batchWriter.flush();

        BatchScanner batchScanner = shardedConnector.createBatchScanner("1", CBConstants.NO_AUTHS, 1);
        batchScanner.setRanges(Collections.singleton(new Range()));
        Iterator<Map.Entry<Key,Value>> iterator = batchScanner.iterator();
        assertEquals(1, Iterators.size(iterator));
        batchScanner.close();

        batchScanner = shardedConnector.createBatchScanner(null, CBConstants.NO_AUTHS, 1);
        batchScanner.setRanges(Collections.singleton(new Range()));
        iterator = batchScanner.iterator();
        assertEquals(1, Iterators.size(iterator));
        batchScanner.close();

        batchScanner = shardedConnector.createBatchScanner("2", CBConstants.NO_AUTHS, 1);
        batchScanner.setRanges(Collections.singleton(new Range()));
        iterator = batchScanner.iterator();
        assertEquals(0, Iterators.size(iterator));
        batchScanner.close();
    }
}
