package mvm.mmrts.rdf.partition.mr.fileinput;

import cloudbase.core.client.Connector;
import cloudbase.core.client.ZooKeeperInstance;
import cloudbase.core.data.ColumnUpdate;
import cloudbase.core.data.Mutation;
import junit.framework.TestCase;
import mvm.mmrts.rdf.partition.utils.RdfIO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.zookeeper.ZooKeeper;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.ValueFactoryImpl;

import java.util.Collection;
import java.util.List;

/**
 * Class RdfFileInputToolTest
 * Date: Aug 8, 2011
 * Time: 3:22:25 PM
 */
public class RdfFileInputToolTest extends TestCase {

    ValueFactory vf = ValueFactoryImpl.getInstance();

    /**
     * MRUnit for latest mapreduce (0.21 api)
     * <p/>
     * 1. Test to see if the bytes overwrite will affect
     */

    private Mapper<LongWritable, BytesWritable, Text, BytesWritable> mapper = new RdfFileInputToCloudbaseTool.OutSubjStmtMapper();
    private Reducer<Text, BytesWritable, Text, Mutation> reducer = new RdfFileInputToCloudbaseTool.StatementToMutationReducer();
    private MapReduceDriver<LongWritable, BytesWritable, Text, BytesWritable, Text, Mutation> driver;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        driver = new MapReduceDriver(mapper, reducer);
        Configuration conf = new Configuration();
        conf.set(RdfFileInputToCloudbaseTool.CB_TABLE_PROP, "table");
        driver.setConfiguration(conf);
    }

    public void testNormalRun() throws Exception {
        StatementImpl stmt1 = new StatementImpl(vf.createURI("urn:namespace#subject"), vf.createURI("urn:namespace#pred"), vf.createLiteral("object"));
        StatementImpl stmt2 = new StatementImpl(vf.createURI("urn:namespace#subject"), vf.createURI("urn:namespace#pred"), vf.createLiteral("obje"));
        StatementImpl stmt3 = new StatementImpl(vf.createURI("urn:namespace#subj2"), vf.createURI("urn:namespace#pred"), vf.createLiteral("ob"));
        List<Pair<Text, Mutation>> pairs = driver.
                withInput(new LongWritable(1), new BytesWritable(RdfIO.writeStatement(stmt1, true))).
                withInput(new LongWritable(1), new BytesWritable(RdfIO.writeStatement(stmt2, true))).
                withInput(new LongWritable(1), new BytesWritable(RdfIO.writeStatement(stmt3, true))).
                run();

        assertEquals(4, pairs.size());

        ColumnUpdate update = pairs.get(0).getSecond().getUpdates().get(0);
        assertEquals("event", new String(update.getColumnFamily()));
        assertEquals("\07urn:namespace#subj2\0\07urn:namespace#pred\0\u0009ob", new String(update.getColumnQualifier()));
    }

    public static void main(String[] args) {
        try {
            Connector connector = new ZooKeeperInstance("stratus", "stratus13:2181").getConnector("root", "password".getBytes());
            Collection<Text> splits = connector.tableOperations().getSplits("partitionRdf", Integer.MAX_VALUE);
            System.out.println(splits.size());
            System.out.println(splits);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
