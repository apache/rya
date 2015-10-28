package mvm.rya.cloudbase.pig;

import cloudbase.core.CBConstants;
import cloudbase.core.client.BatchWriter;
import cloudbase.core.client.Connector;
import cloudbase.core.client.admin.SecurityOperations;
import cloudbase.core.client.mock.MockInstance;
import cloudbase.core.data.Mutation;
import cloudbase.core.data.Value;
import cloudbase.core.security.Authorizations;
import cloudbase.core.security.ColumnVisibility;
import cloudbase.core.security.TablePermission;
import junit.framework.TestCase;
import mvm.rya.cloudbase.pig.CloudbaseStorage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.pig.data.Tuple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * Date: 4/20/12
 * Time: 10:17 AM
 * To change this template use File | Settings | File Templates.
 */
public class CloudbaseStorageTest extends TestCase {

    private String user = "user";
    private String pwd = "pwd";
    private String instance = "myinstance";
    private String table = "testTable";
    private Authorizations auths = CBConstants.NO_AUTHS;
    private Connector connector;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        connector = new MockInstance(instance).getConnector(user, pwd.getBytes());
        connector.tableOperations().create(table);
        SecurityOperations secOps = connector.securityOperations();
        secOps.createUser(user, pwd.getBytes(), auths);
        secOps.grantTablePermission(user, table, TablePermission.READ);
        secOps.grantTablePermission(user, table, TablePermission.WRITE);
    }

    public void testSimpleOutput() throws Exception {
        BatchWriter batchWriter = connector.createBatchWriter(table, 10l, 10l, 2);
        Mutation row = new Mutation("row");
        row.put("cf", "cq", new Value(new byte[0]));
        batchWriter.addMutation(row);
        batchWriter.flush();
        batchWriter.close();

        String location = "cloudbase://" + table + "?instance=" + instance + "&user=" + user + "&password=" + pwd + "&range=a|z&mock=true";
        CloudbaseStorage storage = createCloudbaseStorage(location);
        int count = 0;
        while (true) {
            Tuple next = storage.getNext();
            if (next == null)
                break;
            assertEquals(6, next.size());
            count++;
        }
        assertEquals(1, count);
    }

    public void testRange() throws Exception {
        BatchWriter batchWriter = connector.createBatchWriter(table, 10l, 10l, 2);
        Mutation row = new Mutation("a");
        row.put("cf", "cq", new Value(new byte[0]));
        batchWriter.addMutation(row);
        row = new Mutation("b");
        row.put("cf", "cq", new Value(new byte[0]));
        batchWriter.addMutation(row);
        row = new Mutation("d");
        row.put("cf", "cq", new Value(new byte[0]));
        batchWriter.addMutation(row);
        batchWriter.flush();
        batchWriter.close();

        String location = "cloudbase://" + table + "?instance=" + instance + "&user=" + user + "&password=" + pwd + "&range=a|c&mock=true";
        CloudbaseStorage storage = createCloudbaseStorage(location);
        int count = 0;
        while (true) {
            Tuple next = storage.getNext();
            if (next == null)
                break;
            assertEquals(6, next.size());
            count++;
        }
        assertEquals(2, count);
    }

    public void testMultipleRanges() throws Exception {
        BatchWriter batchWriter = connector.createBatchWriter(table, 10l, 10l, 2);
        Mutation row = new Mutation("a");
        row.put("cf", "cq", new Value(new byte[0]));
        batchWriter.addMutation(row);
        row = new Mutation("b");
        row.put("cf", "cq", new Value(new byte[0]));
        batchWriter.addMutation(row);
        row = new Mutation("d");
        row.put("cf", "cq", new Value(new byte[0]));
        batchWriter.addMutation(row);
        batchWriter.flush();
        batchWriter.close();

        String location = "cloudbase://" + table + "?instance=" + instance + "&user=" + user + "&password=" + pwd + "&range=a|c&range=d|e&mock=true";
        List<CloudbaseStorage> storages = createCloudbaseStorages(location);
        assertEquals(2, storages.size());
        CloudbaseStorage storage = storages.get(0);
        int count = 0;
        while (true) {
            Tuple next = storage.getNext();
            if (next == null)
                break;
            assertEquals(6, next.size());
            count++;
        }
        assertEquals(2, count);
        storage = storages.get(1);
        count = 0;
        while (true) {
            Tuple next = storage.getNext();
            if (next == null)
                break;
            assertEquals(6, next.size());
            count++;
        }
        assertEquals(1, count);
    }

    public void testColumns() throws Exception {
        BatchWriter batchWriter = connector.createBatchWriter(table, 10l, 10l, 2);
        Mutation row = new Mutation("a");
        row.put("cf1", "cq", new Value(new byte[0]));
        row.put("cf2", "cq", new Value(new byte[0]));
        row.put("cf3", "cq1", new Value(new byte[0]));
        row.put("cf3", "cq2", new Value(new byte[0]));
        batchWriter.addMutation(row);
        batchWriter.flush();
        batchWriter.close();

        String location = "cloudbase://" + table + "?instance=" + instance + "&user=" + user + "&password=" + pwd + "&range=a|c&columns=cf1,cf3|cq1&mock=true";
        CloudbaseStorage storage = createCloudbaseStorage(location);
        int count = 0;
        while (true) {
            Tuple next = storage.getNext();
            if (next == null)
                break;
            assertEquals(6, next.size());
            count++;
        }
        assertEquals(2, count);
    }

    public void testWholeRowRange() throws Exception {
        BatchWriter batchWriter = connector.createBatchWriter(table, 10l, 10l, 2);
        Mutation row = new Mutation("a");
        row.put("cf1", "cq", new Value(new byte[0]));
        row.put("cf2", "cq", new Value(new byte[0]));
        row.put("cf3", "cq1", new Value(new byte[0]));
        row.put("cf3", "cq2", new Value(new byte[0]));
        batchWriter.addMutation(row);
        batchWriter.flush();
        batchWriter.close();

        String location = "cloudbase://" + table + "?instance=" + instance + "&user=" + user + "&password=" + pwd + "&range=a&mock=true";
        CloudbaseStorage storage = createCloudbaseStorage(location);
        int count = 0;
        while (true) {
            Tuple next = storage.getNext();
            if (next == null)
                break;
            assertEquals(6, next.size());
            count++;
        }
        assertEquals(4, count);
    }

    public void testAuths() throws Exception {
        BatchWriter batchWriter = connector.createBatchWriter(table, 10l, 10l, 2);
        Mutation row = new Mutation("a");
        row.put("cf1", "cq1", new ColumnVisibility("A"), new Value(new byte[0]));
        row.put("cf2", "cq2", new Value(new byte[0]));
        batchWriter.addMutation(row);
        batchWriter.flush();
        batchWriter.close();

        String location = "cloudbase://" + table + "?instance=" + instance + "&user=" + user + "&password=" + pwd + "&range=a|c&mock=true";
        CloudbaseStorage storage = createCloudbaseStorage(location);
        int count = 0;
        while (true) {
            Tuple next = storage.getNext();
            if (next == null)
                break;
            assertEquals(6, next.size());
            count++;
        }
        assertEquals(1, count);

        location = "cloudbase://" + table + "?instance=" + instance + "&user=" + user + "&password=" + pwd + "&range=a|c&auths=A&mock=true";
        storage = createCloudbaseStorage(location);
        count = 0;
        while (true) {
            Tuple next = storage.getNext();
            if (next == null)
                break;
            assertEquals(6, next.size());
            count++;
        }
        assertEquals(2, count);
    }

    protected CloudbaseStorage createCloudbaseStorage(String location) throws IOException, InterruptedException {
        List<CloudbaseStorage> cloudbaseStorages = createCloudbaseStorages(location);
        if (cloudbaseStorages.size() > 0) {
            return cloudbaseStorages.get(0);
        }
        return null;
    }

    protected List<CloudbaseStorage> createCloudbaseStorages(String location) throws IOException, InterruptedException {
        List<CloudbaseStorage> cloudbaseStorages = new ArrayList<CloudbaseStorage>();
        CloudbaseStorage storage = new CloudbaseStorage();
        InputFormat inputFormat = storage.getInputFormat();
        Job job = new Job(new Configuration());
        storage.setLocation(location, job);
        List<InputSplit> splits = inputFormat.getSplits(job);
        assertNotNull(splits);

        for (InputSplit inputSplit : splits) {
            storage = new CloudbaseStorage();
            job = new Job(new Configuration());
            storage.setLocation(location, job);
            TaskAttemptContext taskAttemptContext = new TaskAttemptContext(job.getConfiguration(),
                    new TaskAttemptID("jtid", 0, false, 0, 0));
            RecordReader recordReader = inputFormat.createRecordReader(inputSplit,
                    taskAttemptContext);
            recordReader.initialize(inputSplit, taskAttemptContext);

            storage.prepareToRead(recordReader, null);
            cloudbaseStorages.add(storage);
        }
        return cloudbaseStorages;
    }
}
