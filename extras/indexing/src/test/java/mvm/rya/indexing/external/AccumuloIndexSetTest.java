package mvm.rya.indexing.external;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */



import java.util.List;
import java.util.Set;

import junit.framework.Assert;
import mvm.rya.indexing.IndexPlanValidator.IndexPlanValidator;
import mvm.rya.indexing.external.tupleSet.AccumuloIndexSet;
import mvm.rya.indexing.external.tupleSet.ExternalProcessorTest.ExternalTupleVstor;
import mvm.rya.indexing.external.tupleSet.ExternalTupleSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
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
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailException;
import org.openrdf.sail.memory.MemoryStore;

import com.beust.jcommander.internal.Sets;
import com.google.common.collect.Lists;

public class AccumuloIndexSetTest {

    private SailRepositoryConnection conn;
    private Connector accCon;
    String tablename = "table";
    Sail s;
    URI obj,obj2,subclass, subclass2, talksTo;
   
    
    
    
    @Before
    public void init() throws RepositoryException, TupleQueryResultHandlerException, QueryEvaluationException, MalformedQueryException, AccumuloException, AccumuloSecurityException, TableExistsException {

        s = new MemoryStore();
        SailRepository repo = new SailRepository(s);
        repo.initialize();
        conn = repo.getConnection();

        URI sub = new URIImpl("uri:entity");
        subclass = new URIImpl("uri:class");
        obj = new URIImpl("uri:obj");
        talksTo = new URIImpl("uri:talksTo");

        conn.add(sub, RDF.TYPE, subclass);
        conn.add(sub, RDFS.LABEL, new LiteralImpl("label"));
        conn.add(sub, talksTo, obj);

        URI sub2 = new URIImpl("uri:entity2");
        subclass2 = new URIImpl("uri:class2");
        obj2 = new URIImpl("uri:obj2");

        conn.add(sub2, RDF.TYPE, subclass2);
        conn.add(sub2, RDFS.LABEL, new LiteralImpl("label2"));
        conn.add(sub2, talksTo, obj2);

        accCon = new MockInstance().getConnector("root", "".getBytes());
        accCon.tableOperations().create(tablename);
        

      

    }
    
    
    
    
    
    @Test
    public void testEvaluateSingeIndex() {
        
        
        // TODO Auto-generated method stub
        String indexSparqlString = ""//
                + "SELECT ?e ?l ?c " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//

        
        CountingResultHandler crh = new CountingResultHandler();
        try {
            conn.prepareTupleQuery(QueryLanguage.SPARQL, indexSparqlString).evaluate(crh);
        } catch (TupleQueryResultHandlerException e2) {
            
            e2.printStackTrace();
        } catch (QueryEvaluationException e2) {
            
            e2.printStackTrace();
        } catch (MalformedQueryException e2) {
            
            e2.printStackTrace();
        } catch (RepositoryException e2) {
            
            e2.printStackTrace();
        }

       

        List<ExternalTupleSet> index = Lists.newArrayList();
        AccumuloIndexSet ais = null; 
        
        try {
            ais = new AccumuloIndexSet(indexSparqlString, conn, accCon, tablename);
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        } catch (SailException e) {
            
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            
            e.printStackTrace();
        } catch (MutationsRejectedException e) {
            
            e.printStackTrace();
        } catch (TableNotFoundException e) {
            
            e.printStackTrace();
        }
        
//        Scanner scan = null;
//        try {
//            scan = accCon.createScanner(tablename, new Authorizations("auths"));
//        } catch (TableNotFoundException e) {
//            
//            e.printStackTrace();
//        }
        
//        scan.setRange(new Range());
//        
//        for (Map.Entry<Key, Value> entry : scan) {
//            System.out.println("Key row string is " + entry.getKey().getRow().toString());
//            System.out.println("Key is " + entry.getKey());
//            System.out.println("Value is " + (new String(entry.getKey().getColumnQualifier().toString())));
//
//        }
        
        
        
        
        index.add(ais);
        
        Assert.assertEquals((double)crh.getCount(), ais.cardinality());
        
        
        
        String queryString = ""//
                + "SELECT ?e ?c ?l ?o " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
                + "  ?e <uri:talksTo> ?o . "//
                + "}";//

        
             
        
        CountingResultHandler crh1 = new CountingResultHandler();
        
        try {
            conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh1);
        } catch (TupleQueryResultHandlerException e1) {
            
            e1.printStackTrace();
        } catch (QueryEvaluationException e1) {
            
            e1.printStackTrace();
        } catch (MalformedQueryException e1) {
            
            e1.printStackTrace();
        } catch (RepositoryException e1) {
            
            e1.printStackTrace();
        }

        
      
        
        ParsedQuery pq = null;
        SPARQLParser sp = new SPARQLParser();
        try {
            pq = sp.parseQuery(queryString, null);
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        }
       
        ExternalProcessor processor = new ExternalProcessor(index);
       

        
        Sail processingSail = new ExternalSail(s, processor);
        SailRepository smartSailRepo = new SailRepository(processingSail);
        try {
            smartSailRepo.initialize();
        } catch (RepositoryException e) {
            
            e.printStackTrace();
        }
        
        CountingResultHandler crh2 = new CountingResultHandler();

        try {
            smartSailRepo.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh2);
        } catch (TupleQueryResultHandlerException e) {
            
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            
            e.printStackTrace();
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        } catch (RepositoryException e) {
            
            e.printStackTrace();
        }
        
        Assert.assertEquals(crh1.getCount(), crh2.getCount());
        
        
    }
    
    
    
    
    
    
    @Test
    public void testEvaluateTwoIndexTwoVarOrder1() {
        
        
        try {
            accCon.tableOperations().create("table2");
        } catch (AccumuloException e4) {
            
            e4.printStackTrace();
        } catch (AccumuloSecurityException e4) {
            
            e4.printStackTrace();
        } catch (TableExistsException e4) {
            
            e4.printStackTrace();
        }
        
        
        try {
            conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
            conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));
        } catch (RepositoryException e3) {
            
            e3.printStackTrace();
        }
       
        
        
        // TODO Auto-generated method stub
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

        
        
        
        

        
        CountingResultHandler crh1 = new CountingResultHandler();
        CountingResultHandler crh2 = new CountingResultHandler();
        try {
            conn.prepareTupleQuery(QueryLanguage.SPARQL, indexSparqlString).evaluate(crh1);
            conn.prepareTupleQuery(QueryLanguage.SPARQL, indexSparqlString).evaluate(crh2);
        } catch (TupleQueryResultHandlerException e2) {
            
            e2.printStackTrace();
        } catch (QueryEvaluationException e2) {
            
            e2.printStackTrace();
        } catch (MalformedQueryException e2) {
            
            e2.printStackTrace();
        } catch (RepositoryException e2) {
            
            e2.printStackTrace();
        }

       

        List<ExternalTupleSet> index = Lists.newArrayList();
        AccumuloIndexSet ais1 = null;
        AccumuloIndexSet ais2 = null;
                
        try {
            ais1 = new AccumuloIndexSet(indexSparqlString, conn, accCon, tablename);
            ais2 = new AccumuloIndexSet(indexSparqlString2, conn, accCon, "table2");
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        } catch (SailException e) {
            
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            
            e.printStackTrace();
        } catch (MutationsRejectedException e) {
            
            e.printStackTrace();
        } catch (TableNotFoundException e) {
            
            e.printStackTrace();
        }
        
//        Scanner scan = null;
//        try {
//            scan = accCon.createScanner(tablename, new Authorizations("auths"));
//        } catch (TableNotFoundException e) {
//            
//            e.printStackTrace();
//        }
        
//        scan.setRange(new Range());
//        
//        for (Map.Entry<Key, Value> entry : scan) {
//            System.out.println("Key row string is " + entry.getKey().getRow().toString());
//            System.out.println("Key is " + entry.getKey());
//            System.out.println("Value is " + (new String(entry.getKey().getColumnQualifier().toString())));
//
//        }
        
        
        
        
        index.add(ais1);
        index.add(ais2);
        
        Assert.assertEquals((double)crh1.getCount(), ais1.cardinality());
        Assert.assertEquals((double)crh2.getCount(), ais2.cardinality());
          
  
        crh1 = new CountingResultHandler();
        crh2 = new CountingResultHandler();
        
        
        try {
            conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh1);
        } catch (TupleQueryResultHandlerException e1) {
            
            e1.printStackTrace();
        } catch (QueryEvaluationException e1) {
            
            e1.printStackTrace();
        } catch (MalformedQueryException e1) {
            
            e1.printStackTrace();
        } catch (RepositoryException e1) {
            
            e1.printStackTrace();
        }

        
  
        
        ParsedQuery pq = null;
        SPARQLParser sp = new SPARQLParser();
        try {
            pq = sp.parseQuery(queryString, null);
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        }
       
        ExternalProcessor processor = new ExternalProcessor(index);
      

        
        Sail processingSail = new ExternalSail(s, processor);
        SailRepository smartSailRepo = new SailRepository(processingSail);
        try {
            smartSailRepo.initialize();
        } catch (RepositoryException e) {
            
            e.printStackTrace();
        }
        
     

        try {
            smartSailRepo.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh2);
        } catch (TupleQueryResultHandlerException e) {
            
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            
            e.printStackTrace();
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        } catch (RepositoryException e) {
            
            e.printStackTrace();
        }
        
        Assert.assertEquals(2, crh1.getCount());
        Assert.assertEquals(2, crh2.getCount());
        
        
        
        
        
    }
    
    
    
    
    
    
    
    
   @Test
    public void testEvaluateTwoIndexTwoVarOrder2() {
        
        
        try {
            accCon.tableOperations().create("table2");
        } catch (AccumuloException e4) {
            
            e4.printStackTrace();
        } catch (AccumuloSecurityException e4) {
            
            e4.printStackTrace();
        } catch (TableExistsException e4) {
            
            e4.printStackTrace();
        }
        
        
        try {
            conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
            conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));
        } catch (RepositoryException e3) {
            
            e3.printStackTrace();
        }
       
        
        
        // TODO Auto-generated method stub
        String indexSparqlString = ""//
                + "SELECT ?c ?e ?l  " //
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

        
        
        
        

        
        CountingResultHandler crh1 = new CountingResultHandler();
        CountingResultHandler crh2 = new CountingResultHandler();
        try {
            conn.prepareTupleQuery(QueryLanguage.SPARQL, indexSparqlString).evaluate(crh1);
            conn.prepareTupleQuery(QueryLanguage.SPARQL, indexSparqlString).evaluate(crh2);
        } catch (TupleQueryResultHandlerException e2) {
            
            e2.printStackTrace();
        } catch (QueryEvaluationException e2) {
            
            e2.printStackTrace();
        } catch (MalformedQueryException e2) {
            
            e2.printStackTrace();
        } catch (RepositoryException e2) {
            
            e2.printStackTrace();
        }

       

        List<ExternalTupleSet> index = Lists.newArrayList();
        AccumuloIndexSet ais1 = null;
        AccumuloIndexSet ais2 = null;
                
        try {
            ais1 = new AccumuloIndexSet(indexSparqlString, conn, accCon, tablename);
            ais2 = new AccumuloIndexSet(indexSparqlString2, conn, accCon, "table2");
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        } catch (SailException e) {
            
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            
            e.printStackTrace();
        } catch (MutationsRejectedException e) {
            
            e.printStackTrace();
        } catch (TableNotFoundException e) {
            
            e.printStackTrace();
        }
      
        
        
        index.add(ais1);
        index.add(ais2);
        
        Assert.assertEquals((double)crh1.getCount(), ais1.cardinality());
        Assert.assertEquals((double)crh2.getCount(), ais2.cardinality());
          
  
        crh1 = new CountingResultHandler();
        crh2 = new CountingResultHandler();
        
        
        try {
            conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh1);
        } catch (TupleQueryResultHandlerException e1) {
            
            e1.printStackTrace();
        } catch (QueryEvaluationException e1) {
            
            e1.printStackTrace();
        } catch (MalformedQueryException e1) {
            
            e1.printStackTrace();
        } catch (RepositoryException e1) {
            
            e1.printStackTrace();
        }

        
     
        
        ParsedQuery pq = null;
        SPARQLParser sp = new SPARQLParser();
        try {
            pq = sp.parseQuery(queryString, null);
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        }
       
        ExternalProcessor processor = new ExternalProcessor(index);
        

        
        Sail processingSail = new ExternalSail(s, processor);
        SailRepository smartSailRepo = new SailRepository(processingSail);
        try {
            smartSailRepo.initialize();
        } catch (RepositoryException e) {
            
            e.printStackTrace();
        }
        
     

        try {
            smartSailRepo.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh2);
        } catch (TupleQueryResultHandlerException e) {
            
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            
            e.printStackTrace();
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        } catch (RepositoryException e) {
            
            e.printStackTrace();
        }
        
        Assert.assertEquals(2, crh1.getCount());
        Assert.assertEquals(2, crh2.getCount());
         
        
        
    }
    
    
    
    
    
    @Test
    public void testEvaluateTwoIndexTwoVarInvalidOrder() {
        
        
        try {
            accCon.tableOperations().create("table2");
        } catch (AccumuloException e4) {
            
            e4.printStackTrace();
        } catch (AccumuloSecurityException e4) {
            
            e4.printStackTrace();
        } catch (TableExistsException e4) {
            
            e4.printStackTrace();
        }
        
        
        try {
            conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
            conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));
        } catch (RepositoryException e3) {
            
            e3.printStackTrace();
        }
       
        
        
        // TODO Auto-generated method stub
        String indexSparqlString = ""//
                + "SELECT ?e ?c ?l  " //
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

        
        
        
        

        
        CountingResultHandler crh1 = new CountingResultHandler();
        CountingResultHandler crh2 = new CountingResultHandler();
        try {
            conn.prepareTupleQuery(QueryLanguage.SPARQL, indexSparqlString).evaluate(crh1);
            conn.prepareTupleQuery(QueryLanguage.SPARQL, indexSparqlString2).evaluate(crh2);
        } catch (TupleQueryResultHandlerException e2) {
            
            e2.printStackTrace();
        } catch (QueryEvaluationException e2) {
            
            e2.printStackTrace();
        } catch (MalformedQueryException e2) {
            
            e2.printStackTrace();
        } catch (RepositoryException e2) {
            
            e2.printStackTrace();
        }

       

        List<ExternalTupleSet> index = Lists.newArrayList();
        AccumuloIndexSet ais1 = null;
        AccumuloIndexSet ais2 = null;
                
        try {
            ais1 = new AccumuloIndexSet(indexSparqlString, conn, accCon, tablename);
            ais2 = new AccumuloIndexSet(indexSparqlString2, conn, accCon, "table2");
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        } catch (SailException e) {
            
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            
            e.printStackTrace();
        } catch (MutationsRejectedException e) {
            
            e.printStackTrace();
        } catch (TableNotFoundException e) {
            
            e.printStackTrace();
        }
        

        
        
        index.add(ais1);
        index.add(ais2);
        
        Assert.assertEquals((double)crh1.getCount(), ais1.cardinality());
        Assert.assertEquals((double)crh2.getCount(), ais2.cardinality());
          
  
        crh1 = new CountingResultHandler();
        crh2 = new CountingResultHandler();
        
        
        try {
            conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh1);
        } catch (TupleQueryResultHandlerException e1) {
            
            e1.printStackTrace();
        } catch (QueryEvaluationException e1) {
            
            e1.printStackTrace();
        } catch (MalformedQueryException e1) {
            
            e1.printStackTrace();
        } catch (RepositoryException e1) {
            
            e1.printStackTrace();
        }

        
       
        
        ParsedQuery pq = null;
        SPARQLParser sp = new SPARQLParser();
        try {
            pq = sp.parseQuery(queryString, null);
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        }
       
        ExternalProcessor processor = new ExternalProcessor(index);
       

        
        Sail processingSail = new ExternalSail(s, processor);
        SailRepository smartSailRepo = new SailRepository(processingSail);
        try {
            smartSailRepo.initialize();
        } catch (RepositoryException e) {
            
            e.printStackTrace();
        }
        

        try {
            smartSailRepo.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh2);
        } catch (TupleQueryResultHandlerException e) {
            
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            
            e.printStackTrace();
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        } catch (RepositoryException e) {
            
            e.printStackTrace();
        } 
        
        Assert.assertEquals(crh1.getCount(), crh2.getCount());
         
        
        
    }
    
    
    
    @Test
    public void testEvaluateTwoIndexThreeVarOrder1() {
        
        
        
        URI superclass = new URIImpl("uri:superclass");
        URI superclass2 = new URIImpl("uri:superclass2");
       

        try {
            conn.add(subclass, RDF.TYPE, superclass);
            conn.add(subclass2, RDF.TYPE, superclass2);
            conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
            conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));
        } catch (RepositoryException e5) {
            
            e5.printStackTrace();
        }
        
        
        try {
            if(accCon.tableOperations().exists("table2")){
                accCon.tableOperations().delete("table2");
            }
            accCon.tableOperations().create("table2");
        } catch (AccumuloException e4) {
            
            e4.printStackTrace();
        } catch (AccumuloSecurityException e4) {
            
            e4.printStackTrace();
        } catch (TableExistsException e4) {
            
            e4.printStackTrace();
        } catch (TableNotFoundException e) {
            
            e.printStackTrace();
        }
        
        
        try {
            conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
            conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));
        } catch (RepositoryException e3) {
            
            e3.printStackTrace();
        }
       
        
        
        // TODO Auto-generated method stub
        String indexSparqlString = ""//
                + "SELECT ?c ?e ?l  " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//
        
        
        String indexSparqlString2 = ""//
                + "SELECT ?e ?c ?l ?f ?o" //
                + "{" //
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?c a ?f . " //
                + "}";//
        
        
        String queryString = ""//
                + "SELECT ?e ?c ?l ?f ?o " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?c a ?f . " //
                + "}";//

        
        
        
        

        
        CountingResultHandler crh1 = new CountingResultHandler();
        CountingResultHandler crh2 = new CountingResultHandler();
        try {
            conn.prepareTupleQuery(QueryLanguage.SPARQL, indexSparqlString).evaluate(crh1);
            conn.prepareTupleQuery(QueryLanguage.SPARQL, indexSparqlString2).evaluate(crh2);
        } catch (TupleQueryResultHandlerException e2) {
            
            e2.printStackTrace();
        } catch (QueryEvaluationException e2) {
            
            e2.printStackTrace();
        } catch (MalformedQueryException e2) {
            
            e2.printStackTrace();
        } catch (RepositoryException e2) {
            
            e2.printStackTrace();
        }

       

        List<ExternalTupleSet> index = Lists.newArrayList();
        AccumuloIndexSet ais1 = null;
        AccumuloIndexSet ais2 = null;
                
        try {
            ais1 = new AccumuloIndexSet(indexSparqlString, conn, accCon, tablename);
            ais2 = new AccumuloIndexSet(indexSparqlString2, conn, accCon, "table2");
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        } catch (SailException e) {
            
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            
            e.printStackTrace();
        } catch (MutationsRejectedException e) {
            
            e.printStackTrace();
        } catch (TableNotFoundException e) {
            
            e.printStackTrace();
        }
      
        
        
        index.add(ais1);
        index.add(ais2);
        
        Assert.assertEquals((double)crh1.getCount(), ais1.cardinality());
        Assert.assertEquals((double)crh2.getCount(), ais2.cardinality());
          
  
        crh1 = new CountingResultHandler();
        crh2 = new CountingResultHandler();
        
        
        try {
            conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh1);
        } catch (TupleQueryResultHandlerException e1) {
            
            e1.printStackTrace();
        } catch (QueryEvaluationException e1) {
            
            e1.printStackTrace();
        } catch (MalformedQueryException e1) {
            
            e1.printStackTrace();
        } catch (RepositoryException e1) {
            
            e1.printStackTrace();
        }

        
    
        
        ParsedQuery pq = null;
        SPARQLParser sp = new SPARQLParser();
        try {
            pq = sp.parseQuery(queryString, null);
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        }
       
        ExternalProcessor processor = new ExternalProcessor(index);
  

        
        Sail processingSail = new ExternalSail(s, processor);
        SailRepository smartSailRepo = new SailRepository(processingSail);
        try {
            smartSailRepo.initialize();
        } catch (RepositoryException e) {
            
            e.printStackTrace();
        }
        
     

        try {
            smartSailRepo.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh2);
        } catch (TupleQueryResultHandlerException e) {
            
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            
            e.printStackTrace();
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        } catch (RepositoryException e) {
            
            e.printStackTrace();
        }
        
        Assert.assertEquals(2, crh1.getCount());
        Assert.assertEquals(2, crh2.getCount());
         
        
        
    }
    
    
    
    
    
    
    
    //@Test
    public void testEvaluateTwoIndexThreeVarsDiffLabel() {
        
        
        URI superclass = new URIImpl("uri:superclass");
        URI superclass2 = new URIImpl("uri:superclass2");
       

        try {
            conn.add(subclass, RDF.TYPE, superclass);
            conn.add(subclass2, RDF.TYPE, superclass2);
            conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
            conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));
        } catch (RepositoryException e5) {
            
            e5.printStackTrace();
        }
        
        
        try {
            if(accCon.tableOperations().exists("table2")){
                accCon.tableOperations().delete("table2");
            }
            accCon.tableOperations().create("table2");
        } catch (AccumuloException e4) {
            
            e4.printStackTrace();
        } catch (AccumuloSecurityException e4) {
            
            e4.printStackTrace();
        } catch (TableExistsException e4) {
            
            e4.printStackTrace();
        } catch (TableNotFoundException e) {
            
            e.printStackTrace();
        }
        
        
        try {
            conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
            conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));
        } catch (RepositoryException e3) {
            
            e3.printStackTrace();
        }
       
        
        
        // TODO Auto-generated method stub
        String indexSparqlString = ""//
                + "SELECT ?dog ?pig ?owl  " //
                + "{" //
                + "  ?pig a ?dog . "//
                + "  ?pig <http://www.w3.org/2000/01/rdf-schema#label> ?owl "//
                + "}";//
        
        
        String indexSparqlString2 = ""//
                + "SELECT ?e ?c ?l ?f ?o" //
                + "{" //
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?c a ?f . " //
                + "}";//
        
        
        String queryString = ""//
                + "SELECT ?e ?c ?l ?f ?o " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?c a ?f . " //
                + "}";//

        
        
        
        

        
        CountingResultHandler crh1 = new CountingResultHandler();
        CountingResultHandler crh2 = new CountingResultHandler();
//        try {
//            conn.prepareTupleQuery(QueryLanguage.SPARQL, indexSparqlString).evaluate(crh1);
//            conn.prepareTupleQuery(QueryLanguage.SPARQL, indexSparqlString2).evaluate(crh2);
//        } catch (TupleQueryResultHandlerException e2) {
//            
//            e2.printStackTrace();
//        } catch (QueryEvaluationException e2) {
//            
//            e2.printStackTrace();
//        } catch (MalformedQueryException e2) {
//            
//            e2.printStackTrace();
//        } catch (RepositoryException e2) {
//            
//            e2.printStackTrace();
//        }

       

        List<ExternalTupleSet> index = Lists.newArrayList();
        AccumuloIndexSet ais1 = null;
        AccumuloIndexSet ais2 = null;
                
        try {
            ais1 = new AccumuloIndexSet(indexSparqlString, conn, accCon, tablename);
            ais2 = new AccumuloIndexSet(indexSparqlString2, conn, accCon, "table2");
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        } catch (SailException e) {
            
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            
            e.printStackTrace();
        } catch (MutationsRejectedException e) {
            
            e.printStackTrace();
        } catch (TableNotFoundException e) {
            
            e.printStackTrace();
        }
        
//        
//      Scanner scan = null;
//      try {
//          scan = accCon.createScanner(tablename, new Authorizations("auths"));
//      } catch (TableNotFoundException e) {
//          
//          e.printStackTrace();
//      }
//      
//      scan.setRange(new Range());
//      
//      for (Map.Entry<Key, Value> entry : scan) {
//          System.out.println("Key row string is " + entry.getKey().getRow().toString());
//          System.out.println("Key is " + entry.getKey());
//          System.out.println("Value is " + (new String(entry.getKey().getColumnQualifier().toString())));
//
//      }
        
      
        
        index.add(ais2);
        index.add(ais1);
        
        
//        Assert.assertEquals((double)crh1.getCount(), ais1.cardinality());
//        Assert.assertEquals((double)crh2.getCount(), ais2.cardinality());
//          
//  
//        crh1 = new CountingResultHandler();
//        crh2 = new CountingResultHandler();
//        
//        
//        try {
//            conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh1);
//        } catch (TupleQueryResultHandlerException e1) {
//            
//            e1.printStackTrace();
//        } catch (QueryEvaluationException e1) {
//            
//            e1.printStackTrace();
//        } catch (MalformedQueryException e1) {
//            
//            e1.printStackTrace();
//        } catch (RepositoryException e1) {
//            
//            e1.printStackTrace();
//        }

        
        
        ParsedQuery pq = null;
        SPARQLParser sp = new SPARQLParser();
        try {
            pq = sp.parseQuery(queryString, null);
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        }
       
        ExternalProcessor processor = new ExternalProcessor(index);
        processor.process(pq.getTupleExpr());

        
        Sail processingSail = new ExternalSail(s, processor);
        SailRepository smartSailRepo = new SailRepository(processingSail);
        try {
            smartSailRepo.initialize();
        } catch (RepositoryException e) {
            
            e.printStackTrace();
        }
        
     

        try {
            smartSailRepo.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh2);
        } catch (TupleQueryResultHandlerException e) {
            
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            
            e.printStackTrace();
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        } catch (RepositoryException e) {
            
            e.printStackTrace();
        }
        
      //  Assert.assertEquals(2, crh1.getCount());
        Assert.assertEquals(2, crh2.getCount());
         
        
        
    }
    
    
    
    
    
    
    
    
    
    
    
    @Test
    public void testEvaluateTwoIndexThreeVarOrder2() {
        
        
        
        URI superclass = new URIImpl("uri:superclass");
        URI superclass2 = new URIImpl("uri:superclass2");
       

        try {
            conn.add(subclass, RDF.TYPE, superclass);
            conn.add(subclass2, RDF.TYPE, superclass2);
            conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
            conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));
        } catch (RepositoryException e5) {
            
            e5.printStackTrace();
        }
        
        
        try {
            if(accCon.tableOperations().exists("table2")){
                accCon.tableOperations().delete("table2");
            }
            accCon.tableOperations().create("table2");
        } catch (AccumuloException e4) {
            
            e4.printStackTrace();
        } catch (AccumuloSecurityException e4) {
            
            e4.printStackTrace();
        } catch (TableExistsException e4) {
            
            e4.printStackTrace();
        } catch (TableNotFoundException e) {
            
            e.printStackTrace();
        }
        
        
        try {
            conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
            conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));
        } catch (RepositoryException e3) {
            
            e3.printStackTrace();
        }
       
        
        
        // TODO Auto-generated method stub
        String indexSparqlString = ""//
                + "SELECT ?c ?e ?l  " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//
        
        
        String indexSparqlString2 = ""//
                + "SELECT ?o ?f ?e ?c ?l  " //
                + "{" //
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?c a ?f . " //
                + "}";//
        
        
        String queryString = ""//
                + "SELECT ?e ?c ?l ?f ?o " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?c a ?f . " //
                + "}";//

        
        
        
        

        
        CountingResultHandler crh1 = new CountingResultHandler();
        CountingResultHandler crh2 = new CountingResultHandler();
        try {
            conn.prepareTupleQuery(QueryLanguage.SPARQL, indexSparqlString).evaluate(crh1);
            conn.prepareTupleQuery(QueryLanguage.SPARQL, indexSparqlString2).evaluate(crh2);
        } catch (TupleQueryResultHandlerException e2) {
            
            e2.printStackTrace();
        } catch (QueryEvaluationException e2) {
            
            e2.printStackTrace();
        } catch (MalformedQueryException e2) {
            
            e2.printStackTrace();
        } catch (RepositoryException e2) {
            
            e2.printStackTrace();
        }

       

        List<ExternalTupleSet> index = Lists.newArrayList();
        AccumuloIndexSet ais1 = null;
        AccumuloIndexSet ais2 = null;
                
        try {
            ais1 = new AccumuloIndexSet(indexSparqlString, conn, accCon, tablename);
            ais2 = new AccumuloIndexSet(indexSparqlString2, conn, accCon, "table2");
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        } catch (SailException e) {
            
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            
            e.printStackTrace();
        } catch (MutationsRejectedException e) {
            
            e.printStackTrace();
        } catch (TableNotFoundException e) {
            
            e.printStackTrace();
        }
      
        
        index.add(ais2);
        index.add(ais1);
        
        
        Assert.assertEquals((double)crh1.getCount(), ais1.cardinality());
        Assert.assertEquals((double)crh2.getCount(), ais2.cardinality());
          
  
        crh1 = new CountingResultHandler();
        crh2 = new CountingResultHandler();
        
//        
//        try {
//            conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh1);
//        } catch (TupleQueryResultHandlerException e1) {
//            
//            e1.printStackTrace();
//        } catch (QueryEvaluationException e1) {
//            
//            e1.printStackTrace();
//        } catch (MalformedQueryException e1) {
//            
//            e1.printStackTrace();
//        } catch (RepositoryException e1) {
//            
//            e1.printStackTrace();
//        }

        
      
        
        ParsedQuery pq = null;
        SPARQLParser sp = new SPARQLParser();
        try {
            pq = sp.parseQuery(queryString, null);
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        }
       
        ExternalProcessor processor = new ExternalProcessor(index);
       

        
        Sail processingSail = new ExternalSail(s, processor);
        SailRepository smartSailRepo = new SailRepository(processingSail);
        try {
            smartSailRepo.initialize();
        } catch (RepositoryException e) {
            
            e.printStackTrace();
        }
        
     

        try {
            smartSailRepo.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh2);
        } catch (TupleQueryResultHandlerException e) {
            
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            
            e.printStackTrace();
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        } catch (RepositoryException e) {
            
            e.printStackTrace();
        }
        
 //       Assert.assertEquals(2, crh1.getCount());
        Assert.assertEquals(2, crh2.getCount());
         
        
        
    }
    
    
    
    
    
    @Test
    public void testEvaluateTwoIndexThreeVarOrder3ThreeBindingSet() {
        
        
        URI sub3 = new URIImpl("uri:entity3");
        URI subclass3 = new URIImpl("uri:class3");
        URI obj3 = new URIImpl("uri:obj3");
                
        URI superclass = new URIImpl("uri:superclass");
        URI superclass2 = new URIImpl("uri:superclass2");
        URI superclass3 = new URIImpl("uri:superclass3");
       

        try {
            
            conn.add(sub3, RDF.TYPE, subclass3);
            conn.add(sub3, RDFS.LABEL, new LiteralImpl("label3"));
            conn.add(sub3, talksTo, obj3);
            
            conn.add(subclass, RDF.TYPE, superclass);
            conn.add(subclass2, RDF.TYPE, superclass2);
            conn.add(subclass3, RDF.TYPE, superclass3);
            
            conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
            conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));
            conn.add(obj3, RDFS.LABEL, new LiteralImpl("label3"));
            
        } catch (RepositoryException e5) {
            
            e5.printStackTrace();
        }
        
        
        try {
            if(accCon.tableOperations().exists("table2")){
                accCon.tableOperations().delete("table2");
            }
            accCon.tableOperations().create("table2");
        } catch (AccumuloException e4) {
            
            e4.printStackTrace();
        } catch (AccumuloSecurityException e4) {
            
            e4.printStackTrace();
        } catch (TableExistsException e4) {
            
            e4.printStackTrace();
        } catch (TableNotFoundException e) {
            
            e.printStackTrace();
        }
        
        
        try {
            conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
            conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));
        } catch (RepositoryException e3) {
            
            e3.printStackTrace();
        }
        
          
        
        
        // TODO Auto-generated method stub
        String indexSparqlString = ""//
                + "SELECT ?c ?e ?l  " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//
        
        
        String indexSparqlString2 = ""//
                + "SELECT ?o ?f ?l ?e ?c  " //
                + "{" //
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?c a ?f . " //
                + "}";//
        
        
        String queryString = ""//
                + "SELECT ?e ?c ?l ?f ?o " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?c a ?f . " //
                + "}";//

        
        
        
        

        
        CountingResultHandler crh1 = new CountingResultHandler();
        CountingResultHandler crh2 = new CountingResultHandler();
        try {
            conn.prepareTupleQuery(QueryLanguage.SPARQL, indexSparqlString).evaluate(crh1);
            conn.prepareTupleQuery(QueryLanguage.SPARQL, indexSparqlString2).evaluate(crh2);
        } catch (TupleQueryResultHandlerException e2) {
            
            e2.printStackTrace();
        } catch (QueryEvaluationException e2) {
            
            e2.printStackTrace();
        } catch (MalformedQueryException e2) {
            
            e2.printStackTrace();
        } catch (RepositoryException e2) {
            
            e2.printStackTrace();
        }

       

        List<ExternalTupleSet> index = Lists.newArrayList();
        AccumuloIndexSet ais1 = null;
        AccumuloIndexSet ais2 = null;
                
        try {
            ais1 = new AccumuloIndexSet(indexSparqlString, conn, accCon, tablename);
            ais2 = new AccumuloIndexSet(indexSparqlString2, conn, accCon, "table2");
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        } catch (SailException e) {
            
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            
            e.printStackTrace();
        } catch (MutationsRejectedException e) {
            
            e.printStackTrace();
        } catch (TableNotFoundException e) {
            
            e.printStackTrace();
        }
        
        
        
//      Scanner scan1 = null;
//      Scanner scan2 = null;
//      try {
//          scan1 = accCon.createScanner(tablename, new Authorizations("auths"));
//          scan2 = accCon.createScanner("table2", new Authorizations("auths"));
//      } catch (TableNotFoundException e) {
//          
//          e.printStackTrace();
//      }
//      
//      scan1.setRange(new Range());
//      
//      for (Map.Entry<Key, Value> entry : scan1) {
//          System.out.println("Key row string is " + entry.getKey().getRow().toString());
//          System.out.println("Key is " + entry.getKey());
//          System.out.println("Value is " + (new String(entry.getKey().getColumnQualifier().toString())));
//
//      }
//      
//      
//      scan2.setRange(new Range());
//      
//      for (Map.Entry<Key, Value> entry : scan2) {
//          System.out.println("Key row string is " + entry.getKey().getRow().toString());
//          System.out.println("Key is " + entry.getKey());
//          System.out.println("Value is " + (new String(entry.getKey().getColumnQualifier().toString())));
//
//      }
        
      
        
        index.add(ais2);
        index.add(ais1);
        
        
        Assert.assertEquals((double)crh1.getCount(), ais1.cardinality());
        Assert.assertEquals((double)crh2.getCount(), ais2.cardinality());
          
  
        crh1 = new CountingResultHandler();
        crh2 = new CountingResultHandler();
        
        
        try {
            conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh1);
        } catch (TupleQueryResultHandlerException e1) {
            
            e1.printStackTrace();
        } catch (QueryEvaluationException e1) {
            
            e1.printStackTrace();
        } catch (MalformedQueryException e1) {
            
            e1.printStackTrace();
        } catch (RepositoryException e1) {
            
            e1.printStackTrace();
        }

        
     
        
        ParsedQuery pq = null;
        SPARQLParser sp = new SPARQLParser();
        try {
            pq = sp.parseQuery(queryString, null);
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        }
       
        ExternalProcessor processor = new ExternalProcessor(index);
        

        
        Sail processingSail = new ExternalSail(s, processor);
        SailRepository smartSailRepo = new SailRepository(processingSail);
        try {
            smartSailRepo.initialize();
        } catch (RepositoryException e) {
            
            e.printStackTrace();
        }
        
        // new SPARQLResultsXMLWriter(System.out)

        try {
            smartSailRepo.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh2);
        } catch (TupleQueryResultHandlerException e) {
            
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            
            e.printStackTrace();
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        } catch (RepositoryException e) {
            
            e.printStackTrace();
        }
        
        Assert.assertEquals(3, crh1.getCount());
        Assert.assertEquals(3, crh2.getCount());
         
        
        
    }
    
    
    
    
    
    
    
    @Test
    public void testEvaluateTwoIndexThreeVarOrder5ThreeBindingSet() {
        
        
        URI sub3 = new URIImpl("uri:entity3");
        URI subclass3 = new URIImpl("uri:class3");
        URI obj3 = new URIImpl("uri:obj3");
                
        URI superclass = new URIImpl("uri:superclass");
        URI superclass2 = new URIImpl("uri:superclass2");
        URI superclass3 = new URIImpl("uri:superclass3");
       

        try {
            
            conn.add(sub3, RDF.TYPE, subclass3);
            conn.add(sub3, RDFS.LABEL, new LiteralImpl("label3"));
            conn.add(sub3, talksTo, obj3);
            
            conn.add(subclass, RDF.TYPE, superclass);
            conn.add(subclass2, RDF.TYPE, superclass2);
            conn.add(subclass3, RDF.TYPE, superclass3);
            
            conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
            conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));
            conn.add(obj3, RDFS.LABEL, new LiteralImpl("label3"));
            
        } catch (RepositoryException e5) {
            
            e5.printStackTrace();
        }
        
        
        try {
            if(accCon.tableOperations().exists("table2")){
                accCon.tableOperations().delete("table2");
            }
            accCon.tableOperations().create("table2");
        } catch (AccumuloException e4) {
            
            e4.printStackTrace();
        } catch (AccumuloSecurityException e4) {
            
            e4.printStackTrace();
        } catch (TableExistsException e4) {
            
            e4.printStackTrace();
        } catch (TableNotFoundException e) {
            
            e.printStackTrace();
        }
        
        
        try {
            conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
            conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));
        } catch (RepositoryException e3) {
            
            e3.printStackTrace();
        }
        
          
        
        
        // TODO Auto-generated method stub
        String indexSparqlString = ""//
                + "SELECT ?c ?e ?l  " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//
        
        
        String indexSparqlString2 = ""//
                + "SELECT ?o ?f ?e ?l ?c  " //
                + "{" //
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?c a ?f . " //
                + "}";//
        
        
        String queryString = ""//
                + "SELECT ?e ?c ?l ?f ?o " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?c a ?f . " //
                + "}";//

        
        
        
        

        
        CountingResultHandler crh1 = new CountingResultHandler();
        CountingResultHandler crh2 = new CountingResultHandler();
        try {
            conn.prepareTupleQuery(QueryLanguage.SPARQL, indexSparqlString).evaluate(crh1);
            conn.prepareTupleQuery(QueryLanguage.SPARQL, indexSparqlString2).evaluate(crh2);
        } catch (TupleQueryResultHandlerException e2) {
            
            e2.printStackTrace();
        } catch (QueryEvaluationException e2) {
            
            e2.printStackTrace();
        } catch (MalformedQueryException e2) {
            
            e2.printStackTrace();
        } catch (RepositoryException e2) {
            
            e2.printStackTrace();
        }

       

        List<ExternalTupleSet> index = Lists.newArrayList();
        AccumuloIndexSet ais1 = null;
        AccumuloIndexSet ais2 = null;
                
        try {
            ais1 = new AccumuloIndexSet(indexSparqlString, conn, accCon, tablename);
            ais2 = new AccumuloIndexSet(indexSparqlString2, conn, accCon, "table2");
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        } catch (SailException e) {
            
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            
            e.printStackTrace();
        } catch (MutationsRejectedException e) {
            
            e.printStackTrace();
        } catch (TableNotFoundException e) {
            
            e.printStackTrace();
        }
         
        
        index.add(ais2);
        index.add(ais1);
        
        
        Assert.assertEquals((double)crh1.getCount(), ais1.cardinality());
        Assert.assertEquals((double)crh2.getCount(), ais2.cardinality());
          
  
        crh1 = new CountingResultHandler();
        crh2 = new CountingResultHandler();
        
        
        try {
            conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh1);
        } catch (TupleQueryResultHandlerException e1) {
            
            e1.printStackTrace();
        } catch (QueryEvaluationException e1) {
            
            e1.printStackTrace();
        } catch (MalformedQueryException e1) {
            
            e1.printStackTrace();
        } catch (RepositoryException e1) {
            
            e1.printStackTrace();
        }

        
        
        
        ParsedQuery pq = null;
        SPARQLParser sp = new SPARQLParser();
        try {
            pq = sp.parseQuery(queryString, null);
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        }
       
        ExternalProcessor processor = new ExternalProcessor(index);
       

        
        Sail processingSail = new ExternalSail(s, processor);
        SailRepository smartSailRepo = new SailRepository(processingSail);
        try {
            smartSailRepo.initialize();
        } catch (RepositoryException e) {
            
            e.printStackTrace();
        }
        
        // new SPARQLResultsXMLWriter(System.out)

        try {
            smartSailRepo.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh2);
        } catch (TupleQueryResultHandlerException e) {
            
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            
            e.printStackTrace();
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        } catch (RepositoryException e) {
            
            e.printStackTrace();
        }
        
        Assert.assertEquals(3, crh1.getCount());
        Assert.assertEquals(3, crh2.getCount());
         
        
        
    }
    
    
    
    
    
    
    
    
    
    
    
    
    
    @Test
    public void testEvaluateTwoIndexThreeVarOrder4ThreeBindingSet() {
        
        
        URI sub3 = new URIImpl("uri:entity3");
        URI subclass3 = new URIImpl("uri:class3");
        URI obj3 = new URIImpl("uri:obj3");
                
        URI superclass = new URIImpl("uri:superclass");
        URI superclass2 = new URIImpl("uri:superclass2");
        URI superclass3 = new URIImpl("uri:superclass3");
       

        try {
            
            conn.add(sub3, RDF.TYPE, subclass3);
            conn.add(sub3, RDFS.LABEL, new LiteralImpl("label3"));
            conn.add(sub3, talksTo, obj3);
            
            conn.add(subclass, RDF.TYPE, superclass);
            conn.add(subclass2, RDF.TYPE, superclass2);
            conn.add(subclass3, RDF.TYPE, superclass3);
            
            conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
            conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));
            conn.add(obj3, RDFS.LABEL, new LiteralImpl("label3"));
            
        } catch (RepositoryException e5) {
            
            e5.printStackTrace();
        }
        
        
        try {
            if(accCon.tableOperations().exists("table2")){
                accCon.tableOperations().delete("table2");
            }
            accCon.tableOperations().create("table2");
        } catch (AccumuloException e4) {
            
            e4.printStackTrace();
        } catch (AccumuloSecurityException e4) {
            
            e4.printStackTrace();
        } catch (TableExistsException e4) {
            
            e4.printStackTrace();
        } catch (TableNotFoundException e) {
            
            e.printStackTrace();
        }
        
        
        try {
            conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
            conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));
        } catch (RepositoryException e3) {
            
            e3.printStackTrace();
        }
        
          
        
        
        // TODO Auto-generated method stub
        String indexSparqlString = ""//
                + "SELECT ?c ?e ?l  " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//
        
        
        String indexSparqlString2 = ""//
                + "SELECT ?o ?f ?c ?e ?l  " //
                + "{" //
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?c a ?f . " //
                + "}";//
        
        
        String queryString = ""//
                + "SELECT ?e ?c ?l ?f ?o " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?c a ?f . " //
                + "}";//

        
        
        
        

        
        CountingResultHandler crh1 = new CountingResultHandler();
        CountingResultHandler crh2 = new CountingResultHandler();
        try {
            conn.prepareTupleQuery(QueryLanguage.SPARQL, indexSparqlString).evaluate(crh1);
            conn.prepareTupleQuery(QueryLanguage.SPARQL, indexSparqlString2).evaluate(crh2);
        } catch (TupleQueryResultHandlerException e2) {
            
            e2.printStackTrace();
        } catch (QueryEvaluationException e2) {
            
            e2.printStackTrace();
        } catch (MalformedQueryException e2) {
            
            e2.printStackTrace();
        } catch (RepositoryException e2) {
            
            e2.printStackTrace();
        }

       

        List<ExternalTupleSet> index = Lists.newArrayList();
        AccumuloIndexSet ais1 = null;
        AccumuloIndexSet ais2 = null;
                
        try {
            ais1 = new AccumuloIndexSet(indexSparqlString, conn, accCon, tablename);
            ais2 = new AccumuloIndexSet(indexSparqlString2, conn, accCon, "table2");
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        } catch (SailException e) {
            
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            
            e.printStackTrace();
        } catch (MutationsRejectedException e) {
            
            e.printStackTrace();
        } catch (TableNotFoundException e) {
            
            e.printStackTrace();
        }
        
        
        
//      Scanner scan1 = null;
//      Scanner scan2 = null;
//      try {
//          scan1 = accCon.createScanner(tablename, new Authorizations("auths"));
//          scan2 = accCon.createScanner("table2", new Authorizations("auths"));
//      } catch (TableNotFoundException e) {
//          
//          e.printStackTrace();
//      }
//      
//      scan1.setRange(new Range());
//      
//      for (Map.Entry<Key, Value> entry : scan1) {
//          System.out.println("Key row string is " + entry.getKey().getRow().toString());
//          System.out.println("Key is " + entry.getKey());
//          System.out.println("Value is " + (new String(entry.getKey().getColumnQualifier().toString())));
//
//      }
//      
//      
//      scan2.setRange(new Range());
//      
//      for (Map.Entry<Key, Value> entry : scan2) {
//          System.out.println("Key row string is " + entry.getKey().getRow().toString());
//          System.out.println("Key is " + entry.getKey());
//          System.out.println("Value is " + (new String(entry.getKey().getColumnQualifier().toString())));
//
//      }
        
      
        
        index.add(ais2);
        index.add(ais1);
        
        
        Assert.assertEquals((double)crh1.getCount(), ais1.cardinality());
        Assert.assertEquals((double)crh2.getCount(), ais2.cardinality());
          
  
        crh1 = new CountingResultHandler();
        crh2 = new CountingResultHandler();
        
        
        try {
            conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh1);
        } catch (TupleQueryResultHandlerException e1) {
            
            e1.printStackTrace();
        } catch (QueryEvaluationException e1) {
            
            e1.printStackTrace();
        } catch (MalformedQueryException e1) {
            
            e1.printStackTrace();
        } catch (RepositoryException e1) {
            
            e1.printStackTrace();
        }

        
        
        
        ParsedQuery pq = null;
        SPARQLParser sp = new SPARQLParser();
        try {
            pq = sp.parseQuery(queryString, null);
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        }
       
        ExternalProcessor processor = new ExternalProcessor(index);
        

        
        Sail processingSail = new ExternalSail(s, processor);
        SailRepository smartSailRepo = new SailRepository(processingSail);
        try {
            smartSailRepo.initialize();
        } catch (RepositoryException e) {
            
            e.printStackTrace();
        }
        
        //new SPARQLResultsXMLWriter(System.out)

        try {
            smartSailRepo.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh2);
        } catch (TupleQueryResultHandlerException e) {
            
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            
            e.printStackTrace();
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        } catch (RepositoryException e) {
            
            e.printStackTrace();
        }
        
        Assert.assertEquals(3, crh1.getCount());
        Assert.assertEquals(3, crh2.getCount());
         
        
        
    }
    
    
    
    
    
    
    
    
    @Test
    public void testEvaluateTwoIndexThreeVarOrder6ThreeBindingSet() {
        
        
        URI sub3 = new URIImpl("uri:entity3");
        URI subclass3 = new URIImpl("uri:class3");
        URI obj3 = new URIImpl("uri:obj3");
                
        URI superclass = new URIImpl("uri:superclass");
        URI superclass2 = new URIImpl("uri:superclass2");
        URI superclass3 = new URIImpl("uri:superclass3");
       

        try {
            
            conn.add(sub3, RDF.TYPE, subclass3);
            conn.add(sub3, RDFS.LABEL, new LiteralImpl("label3"));
            conn.add(sub3, talksTo, obj3);
            
            conn.add(subclass, RDF.TYPE, superclass);
            conn.add(subclass2, RDF.TYPE, superclass2);
            conn.add(subclass3, RDF.TYPE, superclass3);
            
            conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
            conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));
            conn.add(obj3, RDFS.LABEL, new LiteralImpl("label3"));
            
        } catch (RepositoryException e5) {
            
            e5.printStackTrace();
        }
        
        
        try {
            if(accCon.tableOperations().exists("table2")){
                accCon.tableOperations().delete("table2");
            }
            accCon.tableOperations().create("table2");
        } catch (AccumuloException e4) {
            
            e4.printStackTrace();
        } catch (AccumuloSecurityException e4) {
            
            e4.printStackTrace();
        } catch (TableExistsException e4) {
            
            e4.printStackTrace();
        } catch (TableNotFoundException e) {
            
            e.printStackTrace();
        }
        
        
        try {
            conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
            conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));
        } catch (RepositoryException e3) {
            
            e3.printStackTrace();
        }
        
          
        
        
        // TODO Auto-generated method stub
        String indexSparqlString = ""//
                + "SELECT ?c ?e ?l  " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//
        
        
        String indexSparqlString2 = ""//
                + "SELECT ?c ?l ?e ?o ?f " //
                + "{" //
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?c a ?f . " //
                + "}";//
        
        
        String queryString = ""//
                + "SELECT ?e ?c ?l ?f ?o " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?c a ?f . " //
                + "}";//

        
        
        
        

        
        CountingResultHandler crh1 = new CountingResultHandler();
        CountingResultHandler crh2 = new CountingResultHandler();
        try {
            conn.prepareTupleQuery(QueryLanguage.SPARQL, indexSparqlString).evaluate(crh1);
            conn.prepareTupleQuery(QueryLanguage.SPARQL, indexSparqlString2).evaluate(crh2);
        } catch (TupleQueryResultHandlerException e2) {
            
            e2.printStackTrace();
        } catch (QueryEvaluationException e2) {
            
            e2.printStackTrace();
        } catch (MalformedQueryException e2) {
            
            e2.printStackTrace();
        } catch (RepositoryException e2) {
            
            e2.printStackTrace();
        }

       

        List<ExternalTupleSet> index = Lists.newArrayList();
        AccumuloIndexSet ais1 = null;
        AccumuloIndexSet ais2 = null;
                
        try {
            ais1 = new AccumuloIndexSet(indexSparqlString, conn, accCon, tablename);
            ais2 = new AccumuloIndexSet(indexSparqlString2, conn, accCon, "table2");
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        } catch (SailException e) {
            
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            
            e.printStackTrace();
        } catch (MutationsRejectedException e) {
            
            e.printStackTrace();
        } catch (TableNotFoundException e) {
            
            e.printStackTrace();
        }
        
        
      
        
        index.add(ais2);
        index.add(ais1);
        
        
        Assert.assertEquals((double)crh1.getCount(), ais1.cardinality());
        Assert.assertEquals((double)crh2.getCount(), ais2.cardinality());
          
  
        crh1 = new CountingResultHandler();
        crh2 = new CountingResultHandler();
        
        
        try {
            conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh1);
        } catch (TupleQueryResultHandlerException e1) {
            
            e1.printStackTrace();
        } catch (QueryEvaluationException e1) {
            
            e1.printStackTrace();
        } catch (MalformedQueryException e1) {
            
            e1.printStackTrace();
        } catch (RepositoryException e1) {
            
            e1.printStackTrace();
        }

        
       
        
        ParsedQuery pq = null;
        SPARQLParser sp = new SPARQLParser();
        try {
            pq = sp.parseQuery(queryString, null);
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        }
       
        ExternalProcessor processor = new ExternalProcessor(index);
      

        
        Sail processingSail = new ExternalSail(s, processor);
        SailRepository smartSailRepo = new SailRepository(processingSail);
        try {
            smartSailRepo.initialize();
        } catch (RepositoryException e) {
            
            e.printStackTrace();
        }
        
        //new SPARQLResultsXMLWriter(System.out)

        try {
            smartSailRepo.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh2);
        } catch (TupleQueryResultHandlerException e) {
            
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            
            e.printStackTrace();
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        } catch (RepositoryException e) {
            
            e.printStackTrace();
        }
        
        Assert.assertEquals(3, crh1.getCount());
        Assert.assertEquals(3, crh2.getCount());
         
        
        
    }
    
    
    
    
    
    
    
    
    
    @Test
    public void testEvaluateTwoIndexThreeVarOrder7ThreeBindingSet() {
        
        
        URI sub3 = new URIImpl("uri:entity3");
        URI subclass3 = new URIImpl("uri:class3");
        URI obj3 = new URIImpl("uri:obj3");
                
        URI superclass = new URIImpl("uri:superclass");
        URI superclass2 = new URIImpl("uri:superclass2");
        URI superclass3 = new URIImpl("uri:superclass3");
       

        try {
            
            conn.add(sub3, RDF.TYPE, subclass3);
            conn.add(sub3, RDFS.LABEL, new LiteralImpl("label3"));
            conn.add(sub3, talksTo, obj3);
            
            conn.add(subclass, RDF.TYPE, superclass);
            conn.add(subclass2, RDF.TYPE, superclass2);
            conn.add(subclass3, RDF.TYPE, superclass3);
            
            conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
            conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));
            conn.add(obj3, RDFS.LABEL, new LiteralImpl("label3"));
            
        } catch (RepositoryException e5) {
            
            e5.printStackTrace();
        }
        
        
        try {
            if(accCon.tableOperations().exists("table2")){
                accCon.tableOperations().delete("table2");
            }
            accCon.tableOperations().create("table2");
        } catch (AccumuloException e4) {
            
            e4.printStackTrace();
        } catch (AccumuloSecurityException e4) {
            
            e4.printStackTrace();
        } catch (TableExistsException e4) {
            
            e4.printStackTrace();
        } catch (TableNotFoundException e) {
            
            e.printStackTrace();
        }
        
        
        try {
            conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
            conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));
        } catch (RepositoryException e3) {
            
            e3.printStackTrace();
        }
        
          
        
        
        // TODO Auto-generated method stub
        String indexSparqlString = ""//
                + "SELECT ?c ?e ?l  " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//
        
        
        String indexSparqlString2 = ""//
                + "SELECT ?o ?l ?c ?e ?f " //
                + "{" //
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?c a ?f . " //
                + "}";//
        
        
        String queryString = ""//
                + "SELECT ?e ?c ?l ?f ?o " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?c a ?f . " //
                + "}";//

        
        
        
        

        
        CountingResultHandler crh1 = new CountingResultHandler();
        CountingResultHandler crh2 = new CountingResultHandler();
        try {
            conn.prepareTupleQuery(QueryLanguage.SPARQL, indexSparqlString).evaluate(crh1);
            conn.prepareTupleQuery(QueryLanguage.SPARQL, indexSparqlString2).evaluate(crh2);
        } catch (TupleQueryResultHandlerException e2) {
            
            e2.printStackTrace();
        } catch (QueryEvaluationException e2) {
            
            e2.printStackTrace();
        } catch (MalformedQueryException e2) {
            
            e2.printStackTrace();
        } catch (RepositoryException e2) {
            
            e2.printStackTrace();
        }

       

        List<ExternalTupleSet> index = Lists.newArrayList();
        AccumuloIndexSet ais1 = null;
        AccumuloIndexSet ais2 = null;
                
        try {
            ais1 = new AccumuloIndexSet(indexSparqlString, conn, accCon, tablename);
            ais2 = new AccumuloIndexSet(indexSparqlString2, conn, accCon, "table2");
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        } catch (SailException e) {
            
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            
            e.printStackTrace();
        } catch (MutationsRejectedException e) {
            
            e.printStackTrace();
        } catch (TableNotFoundException e) {
            
            e.printStackTrace();
        }
        
        
      
        
        index.add(ais2);
        index.add(ais1);
        
        
        Assert.assertEquals((double)crh1.getCount(), ais1.cardinality());
        Assert.assertEquals((double)crh2.getCount(), ais2.cardinality());
          
  
        crh1 = new CountingResultHandler();
        crh2 = new CountingResultHandler();
        
        
        try {
            conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh1);
        } catch (TupleQueryResultHandlerException e1) {
            
            e1.printStackTrace();
        } catch (QueryEvaluationException e1) {
            
            e1.printStackTrace();
        } catch (MalformedQueryException e1) {
            
            e1.printStackTrace();
        } catch (RepositoryException e1) {
            
            e1.printStackTrace();
        }

        
        
        
        ParsedQuery pq = null;
        SPARQLParser sp = new SPARQLParser();
        try {
            pq = sp.parseQuery(queryString, null);
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        }
       
        ExternalProcessor processor = new ExternalProcessor(index);


        
        Sail processingSail = new ExternalSail(s, processor);
        SailRepository smartSailRepo = new SailRepository(processingSail);
        try {
            smartSailRepo.initialize();
        } catch (RepositoryException e) {
            
            e.printStackTrace();
        }
        
        //new SPARQLResultsXMLWriter(System.out)

        try {
            smartSailRepo.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh2);
        } catch (TupleQueryResultHandlerException e) {
            
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            
            e.printStackTrace();
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        } catch (RepositoryException e) {
            
            e.printStackTrace();
        }
        
        Assert.assertEquals(3, crh1.getCount());
        Assert.assertEquals(3, crh2.getCount());
         
        
        
    }
    
    
    
    
    
    
    
    
    
    
    
    
    @Test
    public void testEvaluateTwoIndexThreeVarInvalidOrder1() {
        
        
        
        URI superclass = new URIImpl("uri:superclass");
        URI superclass2 = new URIImpl("uri:superclass2");
       

        try {
            conn.add(subclass, RDF.TYPE, superclass);
            conn.add(subclass2, RDF.TYPE, superclass2);
            conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
            conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));
        } catch (RepositoryException e5) {
            
            e5.printStackTrace();
        }
        
        
        try {
            if(accCon.tableOperations().exists("table2")){
                accCon.tableOperations().delete("table2");
            }
            accCon.tableOperations().create("table2");
        } catch (AccumuloException e4) {
            
            e4.printStackTrace();
        } catch (AccumuloSecurityException e4) {
            
            e4.printStackTrace();
        } catch (TableExistsException e4) {
            
            e4.printStackTrace();
        } catch (TableNotFoundException e) {
            
            e.printStackTrace();
        }
        
        
        try {
            conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
            conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));
        } catch (RepositoryException e3) {
            
            e3.printStackTrace();
        }
       
        
        
        // TODO Auto-generated method stub
        String indexSparqlString = ""//
                + "SELECT ?c ?e ?l  " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//
        
        
        String indexSparqlString2 = ""//
                + "SELECT ?e ?o ?f ?c ?l  " //
                + "{" //
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?c a ?f . " //
                + "}";//
        
        
        String queryString = ""//
                + "SELECT ?e ?c ?l ?f ?o " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?c a ?f . " //
                + "}";//

        
        
        
        

        
        CountingResultHandler crh1 = new CountingResultHandler();
        CountingResultHandler crh2 = new CountingResultHandler();
        try {
            conn.prepareTupleQuery(QueryLanguage.SPARQL, indexSparqlString).evaluate(crh1);
            conn.prepareTupleQuery(QueryLanguage.SPARQL, indexSparqlString2).evaluate(crh2);
        } catch (TupleQueryResultHandlerException e2) {
            
            e2.printStackTrace();
        } catch (QueryEvaluationException e2) {
            
            e2.printStackTrace();
        } catch (MalformedQueryException e2) {
            
            e2.printStackTrace();
        } catch (RepositoryException e2) {
            
            e2.printStackTrace();
        }

       

        List<ExternalTupleSet> index = Lists.newArrayList();
        AccumuloIndexSet ais1 = null;
        AccumuloIndexSet ais2 = null;
                
        try {
            ais1 = new AccumuloIndexSet(indexSparqlString, conn, accCon, tablename);
            ais2 = new AccumuloIndexSet(indexSparqlString2, conn, accCon, "table2");
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        } catch (SailException e) {
            
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            
            e.printStackTrace();
        } catch (MutationsRejectedException e) {
            
            e.printStackTrace();
        } catch (TableNotFoundException e) {
            
            e.printStackTrace();
        }
      
        
        index.add(ais2);
        index.add(ais1);
        
        
        Assert.assertEquals((double)crh1.getCount(), ais1.cardinality());
        Assert.assertEquals((double)crh2.getCount(), ais2.cardinality());
          
  
        crh1 = new CountingResultHandler();
        crh2 = new CountingResultHandler();
        
        
        try {
            conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh1);
        } catch (TupleQueryResultHandlerException e1) {
            
            e1.printStackTrace();
        } catch (QueryEvaluationException e1) {
            
            e1.printStackTrace();
        } catch (MalformedQueryException e1) {
            
            e1.printStackTrace();
        } catch (RepositoryException e1) {
            
            e1.printStackTrace();
        }

        
       
        
        ParsedQuery pq = null;
        SPARQLParser sp = new SPARQLParser();
        try {
            pq = sp.parseQuery(queryString, null);
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        }
       
        ExternalProcessor processor = new ExternalProcessor(index);
        

        
        Sail processingSail = new ExternalSail(s, processor);
        SailRepository smartSailRepo = new SailRepository(processingSail);
        try {
            smartSailRepo.initialize();
        } catch (RepositoryException e) {
            
            e.printStackTrace();
        }
        
     

        try {
            smartSailRepo.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh2);
        } catch (TupleQueryResultHandlerException e) {
            
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            
            e.printStackTrace();
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        } catch (RepositoryException e) {
            
            e.printStackTrace();
        }
        
        Assert.assertEquals(crh1.getCount(), crh2.getCount());
        
        
        
        
    }
    
    
    
    @Test
    public void testEvaluateTwoIndexThreeVarInvalidOrder2() {
        
        
        
        URI superclass = new URIImpl("uri:superclass");
        URI superclass2 = new URIImpl("uri:superclass2");
       

        try {
            conn.add(subclass, RDF.TYPE, superclass);
            conn.add(subclass2, RDF.TYPE, superclass2);
            conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
            conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));
        } catch (RepositoryException e5) {
            
            e5.printStackTrace();
        }
        
        
        try {
            if(accCon.tableOperations().exists("table2")){
                accCon.tableOperations().delete("table2");
            }
            accCon.tableOperations().create("table2");
        } catch (AccumuloException e4) {
            
            e4.printStackTrace();
        } catch (AccumuloSecurityException e4) {
            
            e4.printStackTrace();
        } catch (TableExistsException e4) {
            
            e4.printStackTrace();
        } catch (TableNotFoundException e) {
            
            e.printStackTrace();
        }
        
        
        try {
            conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
            conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));
        } catch (RepositoryException e3) {
            
            e3.printStackTrace();
        }
       
        
        
        // TODO Auto-generated method stub
        String indexSparqlString = ""//
                + "SELECT ?c ?e ?l  " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//
        
        
        String indexSparqlString2 = ""//
                + "SELECT ?o ?e ?f ?c ?l  " //
                + "{" //
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?c a ?f . " //
                + "}";//
        
        
        String queryString = ""//
                + "SELECT ?e ?c ?l ?f ?o " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?c a ?f . " //
                + "}";//

        
        
        
        

        
        CountingResultHandler crh1 = new CountingResultHandler();
        CountingResultHandler crh2 = new CountingResultHandler();
        try {
            conn.prepareTupleQuery(QueryLanguage.SPARQL, indexSparqlString).evaluate(crh1);
            conn.prepareTupleQuery(QueryLanguage.SPARQL, indexSparqlString2).evaluate(crh2);
        } catch (TupleQueryResultHandlerException e2) {
            
            e2.printStackTrace();
        } catch (QueryEvaluationException e2) {
            
            e2.printStackTrace();
        } catch (MalformedQueryException e2) {
            
            e2.printStackTrace();
        } catch (RepositoryException e2) {
            
            e2.printStackTrace();
        }

       

        List<ExternalTupleSet> index = Lists.newArrayList();
        AccumuloIndexSet ais1 = null;
        AccumuloIndexSet ais2 = null;
                
        try {
            ais1 = new AccumuloIndexSet(indexSparqlString, conn, accCon, tablename);
            ais2 = new AccumuloIndexSet(indexSparqlString2, conn, accCon, "table2");
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        } catch (SailException e) {
            
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            
            e.printStackTrace();
        } catch (MutationsRejectedException e) {
            
            e.printStackTrace();
        } catch (TableNotFoundException e) {
            
            e.printStackTrace();
        }
      
        
        index.add(ais2);
        index.add(ais1);
        
        
        Assert.assertEquals((double)crh1.getCount(), ais1.cardinality());
        Assert.assertEquals((double)crh2.getCount(), ais2.cardinality());
          
  
        crh1 = new CountingResultHandler();
        crh2 = new CountingResultHandler();
        
        
        try {
            conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh1);
        } catch (TupleQueryResultHandlerException e1) {
            
            e1.printStackTrace();
        } catch (QueryEvaluationException e1) {
            
            e1.printStackTrace();
        } catch (MalformedQueryException e1) {
            
            e1.printStackTrace();
        } catch (RepositoryException e1) {
            
            e1.printStackTrace();
        }

        
        
        
        ParsedQuery pq = null;
        SPARQLParser sp = new SPARQLParser();
        try {
            pq = sp.parseQuery(queryString, null);
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        }
       
        ExternalProcessor processor = new ExternalProcessor(index);
       

        
        Sail processingSail = new ExternalSail(s, processor);
        SailRepository smartSailRepo = new SailRepository(processingSail);
        try {
            smartSailRepo.initialize();
        } catch (RepositoryException e) {
            
            e.printStackTrace();
        }
        

        boolean throwsException = false;
        
        try {
            smartSailRepo.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh2);
        } catch (TupleQueryResultHandlerException e) {
            
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            
            e.printStackTrace();
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        } catch (RepositoryException e) {
            
            e.printStackTrace();
        } catch(IllegalStateException e) {
            throwsException = true;
        }
        
        Assert.assertTrue(throwsException);
        
        
        
        
    }
    
    
    
    @Test
    public void testEvaluateOneIndex() {
        
      
        
        URI superclass = new URIImpl("uri:superclass");
        URI superclass2 = new URIImpl("uri:superclass2");
       

        try {
            conn.add(subclass, RDF.TYPE, superclass);
            conn.add(subclass2, RDF.TYPE, superclass2);
            conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
            conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));
        } catch (RepositoryException e5) {
            
            e5.printStackTrace();
        }
        
        
        try {
            if(accCon.tableOperations().exists("table2")){
                accCon.tableOperations().delete("table2");
            }
            accCon.tableOperations().create("table2");
        } catch (AccumuloException e4) {
            
            e4.printStackTrace();
        } catch (AccumuloSecurityException e4) {
            
            e4.printStackTrace();
        } catch (TableExistsException e4) {
            
            e4.printStackTrace();
        } catch (TableNotFoundException e) {
            
            e.printStackTrace();
        }
        
        
        try {
            conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
            conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));
        } catch (RepositoryException e3) {
            
            e3.printStackTrace();
        }
       
        
        
        // TODO Auto-generated method stub
        String indexSparqlString = ""//
                + "SELECT ?dog ?pig ?duck  " //
                + "{" //
                + "  ?pig a ?dog . "//
                + "  ?pig <http://www.w3.org/2000/01/rdf-schema#label> ?duck "//
                + "}";//
        
          
        
        

        
        CountingResultHandler crh1 = new CountingResultHandler();
       
       

        List<ExternalTupleSet> index = Lists.newArrayList();
        AccumuloIndexSet ais1 = null;
                
        try {
            ais1 = new AccumuloIndexSet(indexSparqlString, conn, accCon, tablename);
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        } catch (SailException e) {
            
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            
            e.printStackTrace();
        } catch (MutationsRejectedException e) {
            
            e.printStackTrace();
        } catch (TableNotFoundException e) {
            
            e.printStackTrace();
        }
      
        index.add(ais1);
        ExternalProcessor processor = new ExternalProcessor(index);
   
        Sail processingSail = new ExternalSail(s, processor);
        SailRepository smartSailRepo = new SailRepository(processingSail);
        try {
            smartSailRepo.initialize();
        } catch (RepositoryException e) {
            
            e.printStackTrace();
        }
        
     
       

        try {
            smartSailRepo.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, indexSparqlString).evaluate(crh1);
        } catch (TupleQueryResultHandlerException e) {
            
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            
            e.printStackTrace();
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        } catch (RepositoryException e) {
            
            e.printStackTrace();
        } 
        
        
        
        
        
    }
    
    
    
    
    
    @Test
    public void testEvaluateTwoIndexThreeVarOrder3() {
        

        
        URI superclass = new URIImpl("uri:superclass");
        URI superclass2 = new URIImpl("uri:superclass2");
       

        try {
            conn.add(subclass, RDF.TYPE, superclass);
            conn.add(subclass2, RDF.TYPE, superclass2);
            conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
            conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));
        } catch (RepositoryException e5) {
            
            e5.printStackTrace();
        }
        
        
        try {
            if(accCon.tableOperations().exists("table2")){
                accCon.tableOperations().delete("table2");
            }
            accCon.tableOperations().create("table2");
        } catch (AccumuloException e4) {
            
            e4.printStackTrace();
        } catch (AccumuloSecurityException e4) {
            
            e4.printStackTrace();
        } catch (TableExistsException e4) {
            
            e4.printStackTrace();
        } catch (TableNotFoundException e) {
            
            e.printStackTrace();
        }
        
        
        try {
            conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
            conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));
        } catch (RepositoryException e3) {
            
            e3.printStackTrace();
        }
       
        
        
        // TODO Auto-generated method stub
        String indexSparqlString = ""//
                + "SELECT ?dog ?pig ?duck  " //
                + "{" //
                + "  ?pig a ?dog . "//
                + "  ?pig <http://www.w3.org/2000/01/rdf-schema#label> ?duck "//
                + "}";//
        
        
        String indexSparqlString2 = ""//
                + "SELECT ?o ?f ?e ?c ?l  " //
                + "{" //
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?c a ?f . " //
                + "}";//
        
        
        String queryString = ""//
                + "SELECT ?e ?c ?l ?f ?o " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?c a ?f . " //
                + "}";//

        
        
        
        

        
        CountingResultHandler crh1 = new CountingResultHandler();
        CountingResultHandler crh2 = new CountingResultHandler();
        try {
            conn.prepareTupleQuery(QueryLanguage.SPARQL, indexSparqlString).evaluate(crh1);
            conn.prepareTupleQuery(QueryLanguage.SPARQL, indexSparqlString2).evaluate(crh2);
        } catch (TupleQueryResultHandlerException e2) {
            
            e2.printStackTrace();
        } catch (QueryEvaluationException e2) {
            
            e2.printStackTrace();
        } catch (MalformedQueryException e2) {
            
            e2.printStackTrace();
        } catch (RepositoryException e2) {
            
            e2.printStackTrace();
        }

       

        List<ExternalTupleSet> index = Lists.newArrayList();
        AccumuloIndexSet ais1 = null;
        AccumuloIndexSet ais2 = null;
                
        try {
            ais1 = new AccumuloIndexSet(indexSparqlString, conn, accCon, tablename);
            ais2 = new AccumuloIndexSet(indexSparqlString2, conn, accCon, "table2");
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        } catch (SailException e) {
            
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            
            e.printStackTrace();
        } catch (MutationsRejectedException e) {
            
            e.printStackTrace();
        } catch (TableNotFoundException e) {
            
            e.printStackTrace();
        }
      
        
        index.add(ais2);
        index.add(ais1);
        
        
        Assert.assertEquals((double)crh1.getCount(), ais1.cardinality());
        Assert.assertEquals((double)crh2.getCount(), ais2.cardinality());
        
//        System.out.println("Counts are " + crh1.getCount() + " and " + crh2.getCount());
          
  
        crh1 = new CountingResultHandler();
        crh2 = new CountingResultHandler();
        
        
        try {
            conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh1);
        } catch (TupleQueryResultHandlerException e1) {
            
            e1.printStackTrace();
        } catch (QueryEvaluationException e1) {
            
            e1.printStackTrace();
        } catch (MalformedQueryException e1) {
            
            e1.printStackTrace();
        } catch (RepositoryException e1) {
            
            e1.printStackTrace();
        }

        

        
        ParsedQuery pq = null;
        SPARQLParser sp = new SPARQLParser();
        try {
            pq = sp.parseQuery(queryString, null);
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        }
       
        ExternalProcessor processor = new ExternalProcessor(index);


        
        Sail processingSail = new ExternalSail(s, processor);
        SailRepository smartSailRepo = new SailRepository(processingSail);
        try {
            smartSailRepo.initialize();
        } catch (RepositoryException e) {
            
            e.printStackTrace();
        }
        
     
       

        try {
            smartSailRepo.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh2);
        } catch (TupleQueryResultHandlerException e) {
            
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            
            e.printStackTrace();
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        } catch (RepositoryException e) {
            
            e.printStackTrace();
        } 
        Assert.assertEquals(crh1.getCount(), crh2.getCount());
        
        
        
        
    }
    
   
    

    @Test
    public void testSupportedVarOrders1() {
        
      
        
        URI superclass = new URIImpl("uri:superclass");
        URI superclass2 = new URIImpl("uri:superclass2");
       

        try {
            conn.add(subclass, RDF.TYPE, superclass);
            conn.add(subclass2, RDF.TYPE, superclass2);
            conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
            conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));
        } catch (RepositoryException e5) {
            
            e5.printStackTrace();
        }
        
        
        try {
            if(accCon.tableOperations().exists("table2")){
                accCon.tableOperations().delete("table2");
            }
            accCon.tableOperations().create("table2");
        } catch (AccumuloException e4) {
            
            e4.printStackTrace();
        } catch (AccumuloSecurityException e4) {
            
            e4.printStackTrace();
        } catch (TableExistsException e4) {
            
            e4.printStackTrace();
        } catch (TableNotFoundException e) {
            
            e.printStackTrace();
        }

        
        
        try {
            conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
            conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));
        } catch (RepositoryException e3) {
            
            e3.printStackTrace();
        }
       
        
        
        // TODO Auto-generated method stub
        String indexSparqlString = ""//
                + "SELECT ?dog ?pig ?duck  " //
                + "{" //
                + "  ?pig a ?dog . "//
                + "  ?pig <http://www.w3.org/2000/01/rdf-schema#label> ?duck "//
                + "}";//
        
        
        String indexSparqlString2 = ""//
                + "SELECT ?o ?f ?e ?c ?l  " //
                + "{" //
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?c a ?f . " //
                + "}";//
        
        String indexSparqlString3 = ""//
                + "SELECT ?a ?b ?c  " //
                + "{" //
                + "  ?b a ?a . "//
                + "  ?b <http://www.w3.org/2000/01/rdf-schema#label> ?c "//
                + "}";//
        
  
      
        AccumuloIndexSet ais1 = null;
        AccumuloIndexSet ais2 = null;
                
        try {
            ais1 = new AccumuloIndexSet(indexSparqlString, conn, accCon, tablename);
            ais2 = new AccumuloIndexSet(indexSparqlString2, conn, accCon, "table2");
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        } catch (SailException e) {
            
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            
            e.printStackTrace();
        } catch (MutationsRejectedException e) {
            
            e.printStackTrace();
        } catch (TableNotFoundException e) {
            
            e.printStackTrace();
        }
      
       
        Set<String> ais1Set1 = Sets.newHashSet();
        ais1Set1.add("dog");
        
        Assert.assertTrue(ais1.supportsBindingSet(ais1Set1));
        ais1Set1.add("duck");
        
        Assert.assertTrue(ais1.supportsBindingSet(ais1Set1));
        
        ais1Set1.add("chicken");
        
        Assert.assertTrue(ais1.supportsBindingSet(ais1Set1));
        
        
        Set<String> ais2Set1 = Sets.newHashSet();
        ais2Set1.add("f");
        
        Assert.assertTrue(ais2.supportsBindingSet(ais2Set1));
        ais2Set1.add("e");
        
        Assert.assertTrue(ais2.supportsBindingSet(ais2Set1));
        
        ais2Set1.add("o");
        
        Assert.assertTrue(ais2.supportsBindingSet(ais2Set1));
        
        ais2Set1.add("l");
        
        Assert.assertTrue(ais2.supportsBindingSet(ais2Set1));
        
        Set<String> ais2Set2 = Sets.newHashSet();
        ais2Set2.add("f");
        
        Assert.assertTrue(ais2.supportsBindingSet(ais2Set2));
        
        ais2Set2.add("o");
        
        Assert.assertTrue(ais2.supportsBindingSet(ais2Set2));
        
        ais2Set2.add("c");
        
        Assert.assertTrue(!ais2.supportsBindingSet(ais2Set2));
        
        Set<String> ais2Set3 = Sets.newHashSet();
        ais2Set3.add("c");
        
        Assert.assertTrue(ais2.supportsBindingSet(ais2Set3));
        
        ais2Set3.add("e");
        
        Assert.assertTrue(ais2.supportsBindingSet(ais2Set3));
        
        ais2Set3.add("l");
        
        Assert.assertTrue(ais2.supportsBindingSet(ais2Set3));


        List<ExternalTupleSet> eList = Lists.newArrayList();
        eList.add(ais1);
        SPARQLParser p = new SPARQLParser();
        ParsedQuery pq = null;
        try {
            pq = p.parseQuery(indexSparqlString3, null);
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        }
//        
//        System.out.println("Supported single order is " + ais1.getSupportedVariableOrders());
//        
//        IndexedExecutionPlanGenerator iep = new IndexedExecutionPlanGenerator(pq.getTupleExpr(), eList);
//        List<ExternalTupleSet> indices = iep.getNormalizedIndices();
//        System.out.println("Number of indices is " + indices.size());
//        
//        for(ExternalTupleSet e: indices) {
//            System.out.println("Index is " + e.getTupleExpr() + " and supported orders are " + e.getSupportedVariableOrders());
//        }
        
        
    }
    
    
    

    @Test
    public void testEvaluateTwoIndexThreeVarOrder() {
        
      
        
        URI superclass = new URIImpl("uri:superclass");
        URI superclass2 = new URIImpl("uri:superclass2");
       

        try {
            conn.add(subclass, RDF.TYPE, superclass);
            conn.add(subclass2, RDF.TYPE, superclass2);
            conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
            conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));
        } catch (RepositoryException e5) {
            
            e5.printStackTrace();
        }
        
        
        try {
            if(accCon.tableOperations().exists("table2")){
                accCon.tableOperations().delete("table2");
            }
            accCon.tableOperations().create("table2");
        } catch (AccumuloException e4) {
            
            e4.printStackTrace();
        } catch (AccumuloSecurityException e4) {
            
            e4.printStackTrace();
        } catch (TableExistsException e4) {
            
            e4.printStackTrace();
        } catch (TableNotFoundException e) {
            
            e.printStackTrace();
        }
        
        
        try {
            conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
            conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));
        } catch (RepositoryException e3) {
            
            e3.printStackTrace();
        }
       
        
        
        // TODO Auto-generated method stub
        String indexSparqlString = ""//
                + "SELECT ?dog ?pig ?duck  " //
                + "{" //
                + "  ?pig a ?dog . "//
                + "  ?pig <http://www.w3.org/2000/01/rdf-schema#label> ?duck "//
                + "}";//
        
        
        String indexSparqlString2 = ""//
                + "SELECT ?o ?f ?e ?c ?l  " //
                + "{" //
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?c a ?f . " //
                + "}";//
        
        
        String queryString = ""//
                + "SELECT ?e ?c ?l ?f ?o " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?c a ?f . " //
                + "}";//

        
        
        
        

        
        CountingResultHandler crh1 = new CountingResultHandler();
        CountingResultHandler crh2 = new CountingResultHandler();
        try {
            conn.prepareTupleQuery(QueryLanguage.SPARQL, indexSparqlString).evaluate(crh1);
            conn.prepareTupleQuery(QueryLanguage.SPARQL, indexSparqlString2).evaluate(crh2);
        } catch (TupleQueryResultHandlerException e2) {
            
            e2.printStackTrace();
        } catch (QueryEvaluationException e2) {
            
            e2.printStackTrace();
        } catch (MalformedQueryException e2) {
            
            e2.printStackTrace();
        } catch (RepositoryException e2) {
            
            e2.printStackTrace();
        }

       

        List<ExternalTupleSet> index = Lists.newArrayList();
        AccumuloIndexSet ais1 = null;
        AccumuloIndexSet ais2 = null;
                
        try {
            ais1 = new AccumuloIndexSet(indexSparqlString, conn, accCon, tablename);
            ais2 = new AccumuloIndexSet(indexSparqlString2, conn, accCon, "table2");
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        } catch (SailException e) {
            
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            
            e.printStackTrace();
        } catch (MutationsRejectedException e) {
            
            e.printStackTrace();
        } catch (TableNotFoundException e) {
            
            e.printStackTrace();
        }
      
        
        index.add(ais2);
        index.add(ais1);
        
        
        Assert.assertEquals((double)crh1.getCount(), ais1.cardinality());
        Assert.assertEquals((double)crh2.getCount(), ais2.cardinality());
        
//        System.out.println("Counts are " + crh1.getCount() + " and " + crh2.getCount());
          
  
        crh1 = new CountingResultHandler();
        crh2 = new CountingResultHandler();
        
        
        try {
            conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh1);
        } catch (TupleQueryResultHandlerException e1) {
            
            e1.printStackTrace();
        } catch (QueryEvaluationException e1) {
            
            e1.printStackTrace();
        } catch (MalformedQueryException e1) {
            
            e1.printStackTrace();
        } catch (RepositoryException e1) {
            
            e1.printStackTrace();
        }

        
      
        
        ParsedQuery pq = null;
        SPARQLParser sp = new SPARQLParser();
        try {
            pq = sp.parseQuery(queryString, null);
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        }
       
        ExternalProcessor processor = new ExternalProcessor(index);
   

        
        Sail processingSail = new ExternalSail(s, processor);
        SailRepository smartSailRepo = new SailRepository(processingSail);
        try {
            smartSailRepo.initialize();
        } catch (RepositoryException e) {
            
            e.printStackTrace();
        }
        
     
       

        try {
            smartSailRepo.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh2);
        } catch (TupleQueryResultHandlerException e) {
            
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            
            e.printStackTrace();
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        } catch (RepositoryException e) {
            
            e.printStackTrace();
        } 
        Assert.assertEquals(crh1.getCount(), crh2.getCount());
        
        
        
        
    }
    
    
    
    

    @Test
    public void testEvaluateTwoIndexValidate() {
        
      
        
        URI superclass = new URIImpl("uri:superclass");
        URI superclass2 = new URIImpl("uri:superclass2");
       

        try {
            conn.add(subclass, RDF.TYPE, superclass);
            conn.add(subclass2, RDF.TYPE, superclass2);
            conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
            conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));
        } catch (RepositoryException e5) {
            
            e5.printStackTrace();
        }
        
        
        try {
            if(accCon.tableOperations().exists("table2")){
                accCon.tableOperations().delete("table2");
            }
            accCon.tableOperations().create("table2");
        } catch (AccumuloException e4) {
            
            e4.printStackTrace();
        } catch (AccumuloSecurityException e4) {
            
            e4.printStackTrace();
        } catch (TableExistsException e4) {
            
            e4.printStackTrace();
        } catch (TableNotFoundException e) {
            
            e.printStackTrace();
        }
        
        
        try {
            conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
            conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));
        } catch (RepositoryException e3) {
            
            e3.printStackTrace();
        }
       
        
        
        // TODO Auto-generated method stub
        String indexSparqlString = ""//
                + "SELECT ?dog ?pig ?duck  " //
                + "{" //
                + "  ?pig a ?dog . "//
                + "  ?pig <http://www.w3.org/2000/01/rdf-schema#label> ?duck "//
                + "}";//
        
        
        String indexSparqlString2 = ""//
                + "SELECT ?o ?f ?e ?c ?l  " //
                + "{" //
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?c a ?f . " //
                + "}";//
        
        
        String queryString = ""//
                + "SELECT ?e ?c ?l ?f ?o " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?c a ?f . " //
                + "}";//

        
        
        
        

        
        CountingResultHandler crh1 = new CountingResultHandler();
        CountingResultHandler crh2 = new CountingResultHandler();
        try {
            conn.prepareTupleQuery(QueryLanguage.SPARQL, indexSparqlString).evaluate(crh1);
            conn.prepareTupleQuery(QueryLanguage.SPARQL, indexSparqlString2).evaluate(crh2);
        } catch (TupleQueryResultHandlerException e2) {
            
            e2.printStackTrace();
        } catch (QueryEvaluationException e2) {
            
            e2.printStackTrace();
        } catch (MalformedQueryException e2) {
            
            e2.printStackTrace();
        } catch (RepositoryException e2) {
            
            e2.printStackTrace();
        }

       

        List<ExternalTupleSet> index = Lists.newArrayList();
        AccumuloIndexSet ais1 = null;
        AccumuloIndexSet ais2 = null;
                
        try {
            ais1 = new AccumuloIndexSet(indexSparqlString, conn, accCon, tablename);
            ais2 = new AccumuloIndexSet(indexSparqlString2, conn, accCon, "table2");
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        } catch (SailException e) {
            
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            
            e.printStackTrace();
        } catch (MutationsRejectedException e) {
            
            e.printStackTrace();
        } catch (TableNotFoundException e) {
            
            e.printStackTrace();
        }
      
        index.add(ais1);
        index.add(ais2);
        
        
        
        ParsedQuery pq = null;
        SPARQLParser sp = new SPARQLParser();
        try {
            pq = sp.parseQuery(queryString, null);
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        }
       
        ExternalProcessor processor = new ExternalProcessor(index);
   
        List<TupleExpr> teList = Lists.newArrayList();
        TupleExpr te = processor.process(pq.getTupleExpr());
        
        ExternalTupleVstor etn = new ExternalTupleVstor();
        te.visit(etn);
        
        teList.add(te);
        
        IndexPlanValidator ipv = new IndexPlanValidator(false);
        
        Assert.assertTrue(ipv.isValid(te));
        
      
       
        
    }
    
    
    
    
    

    @Test
    public void testEvaluateThreeIndexValidate() {
        
      
        
        URI superclass = new URIImpl("uri:superclass");
        URI superclass2 = new URIImpl("uri:superclass2");
        
        URI sub = new URIImpl("uri:entity");
        subclass = new URIImpl("uri:class");
        obj = new URIImpl("uri:obj");
        talksTo = new URIImpl("uri:talksTo");
       
        URI howlsAt = new URIImpl("uri:howlsAt");
        URI subType = new URIImpl("uri:subType");
        URI superSuperclass = new URIImpl("uri:super_superclass");
        
        
        try {
            conn.add(subclass, RDF.TYPE, superclass);
            conn.add(subclass2, RDF.TYPE, superclass2);
            conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
            conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));
            conn.add(sub, howlsAt, superclass);
            conn.add(superclass, subType,superSuperclass);
        } catch (RepositoryException e5) {
            
            e5.printStackTrace();
        }
        
        
        try {
            if(accCon.tableOperations().exists("table2")){
                accCon.tableOperations().delete("table2");
            }
            accCon.tableOperations().create("table2");
            
            if(accCon.tableOperations().exists("table3")){
                accCon.tableOperations().delete("table3");
            }
            accCon.tableOperations().create("table3");
        } catch (AccumuloException e4) {
            
            e4.printStackTrace();
        } catch (AccumuloSecurityException e4) {
            
            e4.printStackTrace();
        } catch (TableExistsException e4) {
            
            e4.printStackTrace();
        } catch (TableNotFoundException e) {
            
            e.printStackTrace();
        }
        
        
        try {
            conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
            conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));
        } catch (RepositoryException e3) {
            
            e3.printStackTrace();
        }
       
        
        
        // TODO Auto-generated method stub
        String indexSparqlString = ""//
                + "SELECT ?dog ?pig ?duck  " //
                + "{" //
                + "  ?pig a ?dog . "//
                + "  ?pig <http://www.w3.org/2000/01/rdf-schema#label> ?duck "//
                + "}";//
        
        
        String indexSparqlString2 = ""//
                + "SELECT ?o ?f ?e ?c ?l  " //
                + "{" //
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?c a ?f . " //
                + "}";//
        
        
        
        String indexSparqlString3 = ""//
                + "SELECT ?wolf ?sheep ?chicken  " //
                + "{" //
                + "  ?wolf <uri:howlsAt> ?sheep . "//
                + "  ?sheep <uri:subType> ?chicken. "//
                + "}";//
        
        
        
        String queryString = ""//
                + "SELECT ?e ?c ?l ?f ?o " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?c a ?f . " //
                + "  ?e <uri:howlsAt> ?f. "//
                + "  ?f <uri:subType> ?o. "//  
                + "}";//

        
        
        
        

        
        CountingResultHandler crh1 = new CountingResultHandler();
        CountingResultHandler crh2 = new CountingResultHandler();
        try {
            conn.prepareTupleQuery(QueryLanguage.SPARQL, indexSparqlString).evaluate(crh1);
            conn.prepareTupleQuery(QueryLanguage.SPARQL, indexSparqlString2).evaluate(crh2);
        } catch (TupleQueryResultHandlerException e2) {
            
            e2.printStackTrace();
        } catch (QueryEvaluationException e2) {
            
            e2.printStackTrace();
        } catch (MalformedQueryException e2) {
            
            e2.printStackTrace();
        } catch (RepositoryException e2) {
            
            e2.printStackTrace();
        }

       

        List<ExternalTupleSet> index = Lists.newArrayList();
        AccumuloIndexSet ais1 = null;
        AccumuloIndexSet ais2 = null;
        AccumuloIndexSet ais3 = null;
                
        try {
            ais1 = new AccumuloIndexSet(indexSparqlString, conn, accCon, tablename);
            ais2 = new AccumuloIndexSet(indexSparqlString2, conn, accCon, "table2");
            ais3 = new AccumuloIndexSet(indexSparqlString3, conn, accCon, "table3");
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        } catch (SailException e) {
            
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            
            e.printStackTrace();
        } catch (MutationsRejectedException e) {
            
            e.printStackTrace();
        } catch (TableNotFoundException e) {
            
            e.printStackTrace();
        }
      
        index.add(ais1);
        index.add(ais3);
        index.add(ais2);
       
        
        
        
        ParsedQuery pq = null;
        SPARQLParser sp = new SPARQLParser();
        try {
            pq = sp.parseQuery(queryString, null);
        } catch (MalformedQueryException e) {
            
            e.printStackTrace();
        }
       
        ExternalProcessor processor = new ExternalProcessor(index);
   
        List<TupleExpr> teList = Lists.newArrayList();
        TupleExpr te = processor.process(pq.getTupleExpr());
        
//        ExternalTupleVstor etn = new ExternalTupleVstor();
//        te.visit(etn);
//        
//        for(QueryModelNode q: etn.getExtTup()) {
//            System.out.println("Ext tup maps are " + ((ExternalTupleSet)q).getTableVarMap());
//        }
//        
        teList.add(te);
        
        IndexPlanValidator ipv = new IndexPlanValidator(false);
        
        Assert.assertTrue(ipv.isValid(te));
        
      
       
        
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
            count++;
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
