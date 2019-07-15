package org.apache.rya.indexing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.rdftriplestore.inference.InferenceEngineException;
import org.apache.rya.sail.config.RyaSailFactory;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.Sail;
import org.junit.Test;

public class IndexingFunctionRegistryFreeTextTest {

  private final static String STRING_LENGTH_QUERY = "select (strlen(?o) AS ?o_len) WHERE { ?s <http://worksAt> ?o } LIMIT 1";

  private Sail accumuloSail;
  private Repository accumuloRepo;
  private RepositoryConnection accumuloConn;
  private AccumuloRyaDAO accumuloDao;
  
  @Test
  public void accumuloConnTest1() throws Exception {
    this.init(getAccumuloConf(true));
    this.sparqlConnTest(this.accumuloConn, STRING_LENGTH_QUERY);
    this.close();
  }

  @Test
  public void accumuloConnTest2() throws Exception {
    this.init(getAccumuloConf(false));
    this.sparqlConnTest(this.accumuloConn, STRING_LENGTH_QUERY);
    this.close();
  }

  
  private void init(RdfCloudTripleStoreConfiguration conf) throws AccumuloSecurityException, InferenceEngineException, AccumuloException, RyaDAOException {

    this.accumuloSail = RyaSailFactory.getInstance(conf);
    this.accumuloRepo = new SailRepository(accumuloSail);
    this.accumuloConn = this.accumuloRepo.getConnection();

    Connector conn = ConfigUtils.getConnector(conf);
    this.accumuloDao = new AccumuloRyaDAO();
    this.accumuloDao.setConnector(conn);
    this.accumuloDao.init();
  }

  private void close() throws RyaDAOException {
    this.accumuloConn.close();
    this.accumuloRepo.shutDown();
    this.accumuloSail.shutDown();
    this.accumuloDao.destroy();
  }

  private RdfCloudTripleStoreConfiguration getAccumuloConf(boolean useFreeText) {
    RdfCloudTripleStoreConfiguration conf;
    conf = new AccumuloRdfConfiguration();
    conf.setBoolean(ConfigUtils.USE_MOCK_INSTANCE, true);
    conf.set(RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX, "rya_");
    conf.set(AccumuloRdfConfiguration.CLOUDBASE_USER, "root");
    conf.set(AccumuloRdfConfiguration.CLOUDBASE_PASSWORD, "");
    conf.set(AccumuloRdfConfiguration.CLOUDBASE_INSTANCE, "instance");
    conf.set(RdfCloudTripleStoreConfiguration.CONF_QUERY_AUTH, "");
    conf.setBoolean(ConfigUtils.DISPLAY_QUERY_PLAN, true);
    
    conf.setBoolean(ConfigUtils.USE_FREETEXT, useFreeText);
    
    if (useFreeText) {
      conf.setStrings(ConfigUtils.FREETEXT_PREDICATES_LIST, RDFS.LABEL.stringValue());
    }
    
    return conf;
  }
  
  private void sparqlConnTest(RepositoryConnection conn, String query) throws Exception {
    ValueFactory vf = SimpleValueFactory.getInstance();

    Statement s = vf.createStatement(vf.createIRI("http://Joe"), vf.createIRI("http://worksAt"), vf.createLiteral("CoffeeShop"));
    conn.add(s);

    TupleQueryResult result = null;
    Exception e = null;
    List<BindingSet> results = new ArrayList<>();
    String o_len = "";

    try {
      result = conn.prepareTupleQuery(QueryLanguage.SPARQL, query).evaluate();
      
      while (result.hasNext()) {
        BindingSet bSet = result.next();
        o_len = bSet.getValue("o_len").stringValue();
        results.add(bSet);
      }

    } catch (Exception ex) {
      e = ex;
    }
    
    assertNull(e);
    assertNotNull(result);
    assertNotEquals(0, results.size());
    assertEquals("10", o_len);

    conn.remove(s);
  }
}
