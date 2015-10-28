package mvm.rya.indexing.external;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import junit.framework.Assert;
import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.api.persist.RyaDAOException;
import mvm.rya.indexing.RyaSailFactory;
import mvm.rya.indexing.accumulo.ConfigUtils;
import mvm.rya.indexing.external.tupleSet.AccumuloIndexSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.URI;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.QueryResultHandlerException;
import org.openrdf.query.TupleQueryResultHandler;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailException;
import org.openrdf.sail.memory.MemoryStore;

public class PrecompJoinOptimizerIntegrationTest {

    private SailRepositoryConnection conn;
    private SailRepository repo;
    private Connector accCon;
    String tablePrefix = "table_";
    AccumuloRdfConfiguration conf;
    URI sub, sub2, obj,obj2,subclass, subclass2, talksTo;
   
    
    
    
    @Before
    public void init() throws RepositoryException, TupleQueryResultHandlerException, QueryEvaluationException, MalformedQueryException, 
    AccumuloException, AccumuloSecurityException, TableExistsException, RyaDAOException {

        conf = new AccumuloRdfConfiguration();
        conf.set(ConfigUtils.USE_PCJ, "true");
        conf.set(ConfigUtils.USE_MOCK_INSTANCE,"true");
        conf.set(ConfigUtils.CLOUDBASE_INSTANCE, "instance");
        conf.setTablePrefix(tablePrefix);
        
        Sail sail = RyaSailFactory.getInstance(conf);
        repo = new SailRepository(sail);
        repo.initialize();
        conn = repo.getConnection();

        sub = new URIImpl("uri:entity");
        subclass = new URIImpl("uri:class");
        obj = new URIImpl("uri:obj");
        talksTo = new URIImpl("uri:talksTo");

        conn.add(sub, RDF.TYPE, subclass);
        conn.add(sub, RDFS.LABEL, new LiteralImpl("label"));
        conn.add(sub, talksTo, obj);

        sub2 = new URIImpl("uri:entity2");
        subclass2 = new URIImpl("uri:class2");
        obj2 = new URIImpl("uri:obj2");

        conn.add(sub2, RDF.TYPE, subclass2);
        conn.add(sub2, RDFS.LABEL, new LiteralImpl("label2"));
        conn.add(sub2, talksTo, obj2);

        accCon = new MockInstance("instance").getConnector("root",new PasswordToken("".getBytes()));

    }
    
    
   @After
   public void close() throws RepositoryException, AccumuloException, AccumuloSecurityException, TableNotFoundException {
       
       conf = null;
       conn.close();
       accCon.tableOperations().delete(tablePrefix + "spo");
       accCon.tableOperations().delete(tablePrefix + "po");
       accCon.tableOperations().delete(tablePrefix + "osp");
   }
    
    
    
    @Test
    public void testEvaluateSingeIndex() throws TupleQueryResultHandlerException, QueryEvaluationException,
    MalformedQueryException, RepositoryException, AccumuloException, 
    AccumuloSecurityException, TableExistsException, RyaDAOException, SailException, TableNotFoundException {

        if (accCon.tableOperations().exists(tablePrefix + "INDEX1")) {
            accCon.tableOperations().delete(tablePrefix + "INDEX1");
        }
        accCon.tableOperations().create(tablePrefix + "INDEX1");
        
        String indexSparqlString = ""//
                + "SELECT ?e ?l ?c " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//
      
        AccumuloIndexSet ais = new AccumuloIndexSet(indexSparqlString, conn, accCon, tablePrefix + "INDEX1");
         
       
        String queryString = ""//
                + "SELECT ?e ?c ?l ?o " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
                + "  ?e <uri:talksTo> ?o . "//
                + "}";//

        CountingResultHandler crh = new CountingResultHandler();       
        conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh);
        
//        Scanner scan = accCon.createScanner(tablePrefix + "spo", new Authorizations("U"));
//        
//        for(Entry<Key,Value> e: scan) {
//            System.out.println(e.getKey().getRow());
//        }
        
        Assert.assertEquals(2, crh.getCount());
        
         
    }
    
    
    
    
    
    
    @Test
    public void testEvaluateTwoIndexTwoVarOrder1() throws AccumuloException, AccumuloSecurityException, 
    TableExistsException, RepositoryException, MalformedQueryException, SailException, QueryEvaluationException, 
    TableNotFoundException, TupleQueryResultHandlerException, RyaDAOException {
        
        if (accCon.tableOperations().exists(tablePrefix + "INDEX1")) {
            accCon.tableOperations().delete(tablePrefix + "INDEX1");
        }

        if (accCon.tableOperations().exists(tablePrefix + "INDEX2")) {
            accCon.tableOperations().delete(tablePrefix + "INDEX2");
        }

        accCon.tableOperations().create(tablePrefix + "INDEX1");
        accCon.tableOperations().create(tablePrefix + "INDEX2");
        
        
        
        conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
        conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));

 
        String indexSparqlString = ""//
                + "SELECT ?e ?l ?c " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//

        String indexSparqlString2 = ""//
                + "SELECT ?e ?o ?l " //
                + "{" //
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//

        String queryString = ""//
                + "SELECT ?e ?c ?l ?o " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//

        AccumuloIndexSet ais1 = new AccumuloIndexSet(indexSparqlString, conn, accCon, tablePrefix + "INDEX1");
        AccumuloIndexSet ais2 = new AccumuloIndexSet(indexSparqlString2, conn, accCon, tablePrefix + "INDEX2");

        CountingResultHandler crh = new CountingResultHandler();
        conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh);

        Assert.assertEquals(2, crh.getCount());
     

        
        
    }
    
    
    @Test
    public void testEvaluateSingeFilterIndex() throws TupleQueryResultHandlerException, QueryEvaluationException,
    MalformedQueryException, RepositoryException, AccumuloException, 
    AccumuloSecurityException, TableExistsException, RyaDAOException, SailException, TableNotFoundException {

        if (accCon.tableOperations().exists(tablePrefix + "INDEX1")) {
            accCon.tableOperations().delete(tablePrefix + "INDEX1");
        }
        accCon.tableOperations().create(tablePrefix + "INDEX1");
        
        String indexSparqlString = ""//
                + "SELECT ?e ?l ?c " //
                + "{" //
                + "  Filter(?e = <uri:entity>) " //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//
      
        AccumuloIndexSet ais = new AccumuloIndexSet(indexSparqlString, conn, accCon, tablePrefix + "INDEX1");
         
       
        String queryString = ""//
                + "SELECT ?e ?c ?l ?o " //
                + "{" //
                + "   Filter(?e = <uri:entity>) " //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
                + "  ?e <uri:talksTo> ?o . "//
                + "}";//

        CountingResultHandler crh = new CountingResultHandler();       
        conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh);
        
        Assert.assertEquals(1, crh.getCount());
        
         
    }
    
    
    
    
    @Test
    public void testEvaluateSingeFilterWithUnion() throws TupleQueryResultHandlerException, QueryEvaluationException,
    MalformedQueryException, RepositoryException, AccumuloException, 
    AccumuloSecurityException, TableExistsException, RyaDAOException, SailException, TableNotFoundException {

        if (accCon.tableOperations().exists(tablePrefix + "INDEX2")) {
            accCon.tableOperations().delete(tablePrefix + "INDEX2");
        }
        accCon.tableOperations().create(tablePrefix + "INDEX2");
        
        String indexSparqlString2 = ""//
                + "SELECT ?e ?l ?c " //
                + "{" //
                + "  Filter(?l = \"label2\") " //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//
      
        AccumuloIndexSet ais2 = new AccumuloIndexSet(indexSparqlString2, conn, accCon, tablePrefix + "INDEX2");
         
       
        String queryString = ""//
                + "SELECT ?e ?c ?o ?m ?l" //
                + "{" //
                + "   Filter(?l = \"label2\") " //
                + "  ?e <uri:talksTo> ?o . "//
                + " { ?e a ?c .  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?m  }"//
                + " UNION { ?e a ?c .  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l  }"//
                + "}";//

        CountingResultHandler crh = new CountingResultHandler();       
        conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh);
        
        Assert.assertEquals(1, crh.getCount());
        
         
    }
    
    
    
    @Test
    public void testEvaluateSingeFilterWithLeftJoin() throws TupleQueryResultHandlerException, QueryEvaluationException,
    MalformedQueryException, RepositoryException, AccumuloException, 
    AccumuloSecurityException, TableExistsException, RyaDAOException, SailException, TableNotFoundException {

        if (accCon.tableOperations().exists(tablePrefix + "INDEX1")) {
            accCon.tableOperations().delete(tablePrefix + "INDEX1");
        }
        accCon.tableOperations().create(tablePrefix + "INDEX1");
        
        String indexSparqlString1 = ""//
                + "SELECT ?e ?l ?c " //
                + "{" //
                + "  Filter(?l = \"label3\") " //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//
        
   
        URI sub3 = new URIImpl("uri:entity3");
        URI subclass3 = new URIImpl("uri:class3");
        conn.add(sub3, RDF.TYPE, subclass3);
        conn.add(sub3,RDFS.LABEL, new LiteralImpl("label3"));
        AccumuloIndexSet ais1 = new AccumuloIndexSet(indexSparqlString1, conn, accCon, tablePrefix + "INDEX1");
        
        String queryString = ""//
                + "SELECT ?e ?c ?o ?m ?l" //
                + "{" //
                + "  Filter(?l = \"label3\") " //
                + "  ?e a ?c . " //  
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . " //
                + "  OPTIONAL { ?e <uri:talksTo> ?o . ?e <http://www.w3.org/2000/01/rdf-schema#label> ?m }"//
                + "}";//

        CountingResultHandler crh = new CountingResultHandler();       
        conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh);
        
        Assert.assertEquals(1, crh.getCount());
        
         
    }
    
    
    
    
    
    
    
    @Test
    public void testEvaluateTwoIndexUnionFilter() throws AccumuloException, AccumuloSecurityException, 
    TableExistsException, RepositoryException, MalformedQueryException, SailException, QueryEvaluationException, 
    TableNotFoundException, TupleQueryResultHandlerException, RyaDAOException {
        
        if (accCon.tableOperations().exists(tablePrefix + "INDEX1")) {
            accCon.tableOperations().delete(tablePrefix + "INDEX1");
        }

        if (accCon.tableOperations().exists(tablePrefix + "INDEX2")) {
            accCon.tableOperations().delete(tablePrefix + "INDEX2");
        }

        accCon.tableOperations().create(tablePrefix + "INDEX1");
        accCon.tableOperations().create(tablePrefix + "INDEX2");
           
        conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
        conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));
        conn.add(sub, RDF.TYPE, obj);
        conn.add(sub2, RDF.TYPE, obj2);
     
     
        String indexSparqlString = ""//
                + "SELECT ?e ?l ?o " //
                + "{" //
                + "   Filter(?l = \"label2\") " //
                + "  ?e a ?o . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//

        String indexSparqlString2 = ""//
                + "SELECT ?e ?l ?o " //
                + "{" //
                + "   Filter(?l = \"label2\") " //
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//

        String queryString = ""//
                + "SELECT ?c ?e ?l ?o " //
                + "{" //
                + "   Filter(?l = \"label2\") " //
                + "  ?e a ?c . "//
                + " { ?e a ?o .  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l  }"//
                + " UNION { ?e <uri:talksTo> ?o .  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l  }"//
                + "}";//

        AccumuloIndexSet ais1 = new AccumuloIndexSet(indexSparqlString, conn, accCon, tablePrefix + "INDEX1");
        AccumuloIndexSet ais2 = new AccumuloIndexSet(indexSparqlString2, conn, accCon, tablePrefix + "INDEX2");

        CountingResultHandler crh = new CountingResultHandler();
        conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh);
          
        Assert.assertEquals(6, crh.getCount());
     

     
    }
    
    
    
    
    
    @Test
    public void testEvaluateTwoIndexLeftJoinUnionFilter() throws AccumuloException, AccumuloSecurityException, 
    TableExistsException, RepositoryException, MalformedQueryException, SailException, QueryEvaluationException, 
    TableNotFoundException, TupleQueryResultHandlerException, RyaDAOException {
        
        if (accCon.tableOperations().exists(tablePrefix + "INDEX1")) {
            accCon.tableOperations().delete(tablePrefix + "INDEX1");
        }

        if (accCon.tableOperations().exists(tablePrefix + "INDEX2")) {
            accCon.tableOperations().delete(tablePrefix + "INDEX2");
        }

        accCon.tableOperations().create(tablePrefix + "INDEX1");
        accCon.tableOperations().create(tablePrefix + "INDEX2");
           
        conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
        conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));
        conn.add(sub, RDF.TYPE, obj);
        conn.add(sub2, RDF.TYPE, obj2);
        
        URI livesIn = new URIImpl("uri:livesIn");
        URI city = new URIImpl("uri:city");
        URI city2 = new URIImpl("uri:city2");
        URI city3 = new URIImpl("uri:city3");
        conn.add(sub,livesIn,city);
        conn.add(sub2,livesIn,city2);
        conn.add(sub2,livesIn,city3);
        conn.add(sub,livesIn,city3);
       
     
        String indexSparqlString = ""//
                + "SELECT ?e ?l ?o " //
                + "{" //
                + "  ?e a ?o . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//

        String indexSparqlString2 = ""//
                + "SELECT ?e ?l ?o " //
                + "{" //
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//

        String queryString = ""//
                + "SELECT ?c ?e ?l ?o " //
                + "{" //
                + " Filter(?c = <uri:city3>) " //
                + " ?e <uri:livesIn> ?c . "//
                + " OPTIONAL{{ ?e a ?o .  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l  }"//
                + " UNION { ?e <uri:talksTo> ?o .  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l  }}"//
                + "}";//

        AccumuloIndexSet ais1 = new AccumuloIndexSet(indexSparqlString, conn, accCon, tablePrefix + "INDEX1");
        AccumuloIndexSet ais2 = new AccumuloIndexSet(indexSparqlString2, conn, accCon, tablePrefix + "INDEX2");

        CountingResultHandler crh = new CountingResultHandler();
        conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh);

//        Scanner scan = accCon.createScanner(tablePrefix + "spo", new Authorizations("U"));
//        
//        for(Entry<Key,Value> e: scan) {
//            System.out.println(e.getKey().getRow());
//        }
        
        Assert.assertEquals(6, crh.getCount());
     

     
    }
    
    
    
    
    public static class CountingResultHandler implements TupleQueryResultHandler {
        private int count = 0;

        public int getCount() {
            return count;
        }

        public void resetCount() {
            this.count = 0;
        }

        @Override
        public void startQueryResult(List<String> arg0) throws TupleQueryResultHandlerException {
        }


        @Override
        public void handleSolution(BindingSet arg0) throws TupleQueryResultHandlerException {
            System.out.println(arg0);
            count++;
            System.out.println("Count is " + count);
        }

        @Override
        public void endQueryResult() throws TupleQueryResultHandlerException {
        }

        @Override
        public void handleBoolean(boolean arg0) throws QueryResultHandlerException {
            // TODO Auto-generated method stub

        }

        @Override
        public void handleLinks(List<String> arg0) throws QueryResultHandlerException {
            // TODO Auto-generated method stub

        }
    }
    
    
    
    
    
}
    
    
    
    