package mvm.rya.cloudbase.pig;

import cloudbase.core.CBConstants;
import cloudbase.core.client.Connector;
import cloudbase.core.client.admin.SecurityOperations;
import cloudbase.core.client.mock.MockInstance;
import cloudbase.core.security.Authorizations;
import cloudbase.core.security.TablePermission;
import junit.framework.TestCase;
import mvm.rya.api.RdfCloudTripleStoreConstants;
import mvm.rya.api.resolver.RdfToRyaConversions;
import mvm.rya.cloudbase.CloudbaseRdfConfiguration;
import mvm.rya.cloudbase.CloudbaseRyaDAO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.pig.data.Tuple;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ContextStatementImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.ValueFactoryImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * Date: 4/20/12
 * Time: 5:14 PM
 * To change this template use File | Settings | File Templates.
 */
public class StatementPatternStorageTest extends TestCase {

    private String user = "user";
    private String pwd = "pwd";
    private String instance = "myinstance";
    private String tablePrefix = "t_";
    private Authorizations auths = CBConstants.NO_AUTHS;
    private Connector connector;
    private CloudbaseRyaDAO ryaDAO;
    private ValueFactory vf = new ValueFactoryImpl();
    private String namespace = "urn:test#";
    private CloudbaseRdfConfiguration conf;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        connector = new MockInstance(instance).getConnector(user, pwd.getBytes());
        connector.tableOperations().create(tablePrefix + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX);
        connector.tableOperations().create(tablePrefix + RdfCloudTripleStoreConstants.TBL_PO_SUFFIX);
        connector.tableOperations().create(tablePrefix + RdfCloudTripleStoreConstants.TBL_OSP_SUFFIX);
        connector.tableOperations().create(tablePrefix + RdfCloudTripleStoreConstants.TBL_NS_SUFFIX);
        SecurityOperations secOps = connector.securityOperations();
        secOps.createUser(user, pwd.getBytes(), auths);
        secOps.grantTablePermission(user, tablePrefix + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX, TablePermission.READ);
        secOps.grantTablePermission(user, tablePrefix + RdfCloudTripleStoreConstants.TBL_PO_SUFFIX, TablePermission.READ);
        secOps.grantTablePermission(user, tablePrefix + RdfCloudTripleStoreConstants.TBL_OSP_SUFFIX, TablePermission.READ);
        secOps.grantTablePermission(user, tablePrefix + RdfCloudTripleStoreConstants.TBL_NS_SUFFIX, TablePermission.READ);

        conf = new CloudbaseRdfConfiguration();
        ryaDAO = new CloudbaseRyaDAO();
        ryaDAO.setConnector(connector);
        conf.setTablePrefix(tablePrefix);
        ryaDAO.setConf(conf);
        ryaDAO.init();
    }

    public void testSimplePredicateRange() throws Exception {
        ryaDAO.add(RdfToRyaConversions.convertStatement(new StatementImpl(vf.createURI(namespace, "a"), vf.createURI(namespace, "p"), vf.createLiteral("l"))));
        ryaDAO.add(RdfToRyaConversions.convertStatement(new StatementImpl(vf.createURI(namespace, "b"), vf.createURI(namespace, "p"), vf.createLiteral("l"))));
        ryaDAO.add(RdfToRyaConversions.convertStatement(new StatementImpl(vf.createURI(namespace, "c"), vf.createURI(namespace, "n"), vf.createLiteral("l"))));
        

        int count = 0;
        List<StatementPatternStorage> storages = createStorages("cloudbase://" + tablePrefix + "?instance=" + instance + "&user=" + user + "&password=" + pwd + "&predicate=<" + namespace + "p>&mock=true");
        for (StatementPatternStorage storage : storages) {
            while (true) {
                Tuple next = storage.getNext();
                if (next == null) {
                    break;
                }
                count++;
            }
        }
        assertEquals(2, count);
        ryaDAO.destroy();
    }

    public void testContext() throws Exception {
        ryaDAO.add(RdfToRyaConversions.convertStatement(new StatementImpl(vf.createURI(namespace, "a"), vf.createURI(namespace, "p"), vf.createLiteral("l1"))));
        ryaDAO.add(RdfToRyaConversions.convertStatement(new ContextStatementImpl(vf.createURI(namespace, "a"), vf.createURI(namespace, "p"), vf.createLiteral("l2"), vf.createURI(namespace, "g1"))));
        

        int count = 0;
        List<StatementPatternStorage> storages = createStorages("cloudbase://" + tablePrefix + "?instance=" + instance + "&user=" + user + "&password=" + pwd + "&predicate=<" + namespace + "p>&mock=true");
        for (StatementPatternStorage storage : storages) {
            while (true) {
                Tuple next = storage.getNext();
                if (next == null) {
                    break;
                }
                count++;
            }
        }
        assertEquals(2, count);

        count = 0;
        storages = createStorages("cloudbase://" + tablePrefix + "?instance=" + instance + "&user=" + user + "&password=" + pwd + "&predicate=<" + namespace + "p>&context=<"+namespace+"g1>&mock=true");
        for (StatementPatternStorage storage : storages) {
            while (true) {
                Tuple next = storage.getNext();
                if (next == null) {
                    break;
                }
                count++;
            }
        }
        assertEquals(1, count);

        ryaDAO.destroy();
    }

    protected List<StatementPatternStorage> createStorages(String location) throws IOException, InterruptedException {
        List<StatementPatternStorage> storages = new ArrayList<StatementPatternStorage>();
        StatementPatternStorage storage = new StatementPatternStorage();
        InputFormat inputFormat = storage.getInputFormat();
        Job job = new Job(new Configuration());
        storage.setLocation(location, job);
        List<InputSplit> splits = inputFormat.getSplits(job);
        assertNotNull(splits);

        for (InputSplit inputSplit : splits) {
            storage = new StatementPatternStorage();
            job = new Job(new Configuration());
            storage.setLocation(location, job);
            TaskAttemptContext taskAttemptContext = new TaskAttemptContext(job.getConfiguration(),
                    new TaskAttemptID("jtid", 0, false, 0, 0));
            RecordReader recordReader = inputFormat.createRecordReader(inputSplit,
                    taskAttemptContext);
            recordReader.initialize(inputSplit, taskAttemptContext);

            storage.prepareToRead(recordReader, null);
            storages.add(storage);
        }
        return storages;
    }

}
