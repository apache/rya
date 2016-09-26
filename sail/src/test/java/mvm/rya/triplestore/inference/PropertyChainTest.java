package mvm.rya.triplestore.inference;

import java.util.List;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.junit.Assert;
import org.junit.Test;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.Update;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;

import junit.framework.TestCase;
import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.accumulo.AccumuloRyaDAO;
import mvm.rya.api.RdfCloudTripleStoreConstants;
import mvm.rya.rdftriplestore.RdfCloudTripleStore;
import mvm.rya.rdftriplestore.inference.InferenceEngine;
import mvm.rya.rdftriplestore.inference.InverseURI;

public class PropertyChainTest extends TestCase {
    private String user = "user";
    private String pwd = "pwd";
    private String instance = "myinstance";
    private String tablePrefix = "t_";
    private Authorizations auths = Constants.NO_AUTHS;
    private Connector connector;
    private AccumuloRyaDAO ryaDAO;
    private ValueFactory vf = new ValueFactoryImpl();
    private String namespace = "urn:test#";
    private AccumuloRdfConfiguration conf;

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

        conf = new AccumuloRdfConfiguration();
        ryaDAO = new AccumuloRyaDAO();
        ryaDAO.setConnector(connector);
        conf.setTablePrefix(tablePrefix);
        ryaDAO.setConf(conf);
        ryaDAO.init();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        connector.tableOperations().delete(tablePrefix + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX);
        connector.tableOperations().delete(tablePrefix + RdfCloudTripleStoreConstants.TBL_PO_SUFFIX);
        connector.tableOperations().delete(tablePrefix + RdfCloudTripleStoreConstants.TBL_OSP_SUFFIX);
        connector.tableOperations().delete(tablePrefix + RdfCloudTripleStoreConstants.TBL_NS_SUFFIX);
    }

    @Test
    public void testGraphConfiguration() throws Exception {
        // build a connection
        RdfCloudTripleStore store = new RdfCloudTripleStore();
        store.setConf(conf);
        store.setRyaDAO(ryaDAO);
        InferenceEngine inferenceEngine = new InferenceEngine();
        inferenceEngine.setRyaDAO(ryaDAO);
        store.setInferenceEngine(inferenceEngine);
        inferenceEngine.refreshGraph();
        store.initialize();
        SailRepository repository = new SailRepository(store);
        SailRepositoryConnection conn = repository.getConnection();
        

        
    	String query = "INSERT DATA\n"//
    			+ "{ GRAPH <http://updated/test> {\n"//
    			+ "  <urn:greatMother> owl:propertyChainAxiom <urn:12342>  . " + 
    			" <urn:12342> <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> _:node1atjakcvbx15023 . " + 
    			" _:node1atjakcvbx15023 <http://www.w3.org/2002/07/owl#inverseOf> <urn:isChildOf> . " + 
    			" <urn:12342> <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> _:node1atjakcvbx15123 . " + 
       			" _:node1atjakcvbx15123 <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil> . " + 
    			" _:node1atjakcvbx15123 <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> <urn:MotherOf> .  }}";
    	Update update = conn.prepareUpdate(QueryLanguage.SPARQL, query);
    	update.execute();
        inferenceEngine.refreshGraph();
       List<URI> chain = inferenceEngine.getPropertyChain(vf.createURI("urn:greatMother"));
       Assert.assertEquals(chain.size(), 2);
       Assert.assertEquals(chain.get(0), new InverseURI(vf.createURI("urn:isChildOf")));
       Assert.assertEquals(chain.get(1), vf.createURI("urn:MotherOf"));
 
    }
}
