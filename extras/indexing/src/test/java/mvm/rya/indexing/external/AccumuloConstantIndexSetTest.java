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


import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;

import junit.framework.Assert;
import mvm.rya.indexing.IndexPlanValidator.IndexPlanValidator;
import mvm.rya.indexing.external.tupleSet.AccumuloIndexSet;
import mvm.rya.indexing.external.tupleSet.ExternalProcessorTest.ExternalTupleVstor;
import mvm.rya.indexing.external.tupleSet.ExternalTupleSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
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

public class AccumuloConstantIndexSetTest {

    
    private SailRepositoryConnection conn;
    private Connector accCon;
    String tablename = "table";
    Sail s;
    URI obj, obj2, subclass, subclass2, talksTo;

    @Before
    public void init() throws RepositoryException, TupleQueryResultHandlerException, QueryEvaluationException,
            MalformedQueryException, AccumuloException, AccumuloSecurityException, TableExistsException {

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
    public void testEvaluateTwoIndexVarInstantiate1() {

        URI superclass = new URIImpl("uri:superclass");
        URI superclass2 = new URIImpl("uri:superclass2");

        try {
            conn.add(subclass, RDF.TYPE, superclass);
            conn.add(subclass2, RDF.TYPE, superclass2);
            conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
            conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));
        } catch (RepositoryException e5) {
            // TODO Auto-generated catch block
            e5.printStackTrace();
        }

        try {
            if (accCon.tableOperations().exists("table2")) {
                accCon.tableOperations().delete("table2");
            }
            accCon.tableOperations().create("table2");
        } catch (AccumuloException e4) {
            // TODO Auto-generated catch block
            e4.printStackTrace();
        } catch (AccumuloSecurityException e4) {
            // TODO Auto-generated catch block
            e4.printStackTrace();
        } catch (TableExistsException e4) {
            // TODO Auto-generated catch block
            e4.printStackTrace();
        } catch (TableNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        try {
            conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
            conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));
        } catch (RepositoryException e3) {
            // TODO Auto-generated catch block
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
                + "SELECT ?c ?l ?f ?o " //
                + "{" //
                + "  <uri:entity> a ?c . "//
                + "  <uri:entity> <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  <uri:entity> <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?c a ?f . " //
                + "}";//

     

        List<ExternalTupleSet> index = Lists.newArrayList();
        AccumuloIndexSet ais1 = null;
        AccumuloIndexSet ais2 = null;

        try {
            ais1 = new AccumuloIndexSet(indexSparqlString, conn, accCon, tablename);
            ais2 = new AccumuloIndexSet(indexSparqlString2, conn, accCon, "table2");
        } catch (MalformedQueryException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (SailException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (MutationsRejectedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (TableNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

      
       
        CountingResultHandler crh1 = new CountingResultHandler();
        CountingResultHandler crh2 = new CountingResultHandler();

        try {
            conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh1);
        } catch (TupleQueryResultHandlerException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        } catch (QueryEvaluationException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        } catch (MalformedQueryException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        } catch (RepositoryException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

        ParsedQuery pq = null;
        SPARQLParser sp = new SPARQLParser();
        try {
            pq = sp.parseQuery(queryString, null);
        } catch (MalformedQueryException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        index.add(ais1);
        index.add(ais2);
        
        ExternalProcessor processor = new ExternalProcessor(index);

        Sail processingSail = new ExternalSail(s, processor);
        SailRepository smartSailRepo = new SailRepository(processingSail);
        try {
            smartSailRepo.initialize();
        } catch (RepositoryException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        
        try {
            smartSailRepo.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh2);
        } catch (TupleQueryResultHandlerException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (MalformedQueryException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (RepositoryException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
       
        Assert.assertEquals(crh1.getCount(), crh2.getCount());

    }
    
    
    
    
    
  @Test
    public void testEvaluateThreeIndexVarInstantiate() {

        URI superclass = new URIImpl("uri:superclass");
        URI superclass2 = new URIImpl("uri:superclass2");

        URI sub = new URIImpl("uri:entity");
        subclass = new URIImpl("uri:class");
        obj = new URIImpl("uri:obj");
        talksTo = new URIImpl("uri:talksTo");

        URI howlsAt = new URIImpl("uri:howlsAt");
        URI subType = new URIImpl("uri:subType");
        

        try {
            conn.add(subclass, RDF.TYPE, superclass);
            conn.add(subclass2, RDF.TYPE, superclass2);
            conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
            conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));
            conn.add(sub, howlsAt, superclass);
            conn.add(superclass, subType, obj);
        } catch (RepositoryException e5) {
            // TODO Auto-generated catch block
            e5.printStackTrace();
        }

        try {
            if (accCon.tableOperations().exists("table2")) {
                accCon.tableOperations().delete("table2");
            }
            accCon.tableOperations().create("table2");

            if (accCon.tableOperations().exists("table3")) {
                accCon.tableOperations().delete("table3");
            }
            accCon.tableOperations().create("table3");
        } catch (AccumuloException e4) {
            // TODO Auto-generated catch block
            e4.printStackTrace();
        } catch (AccumuloSecurityException e4) {
            // TODO Auto-generated catch block
            e4.printStackTrace();
        } catch (TableExistsException e4) {
            // TODO Auto-generated catch block
            e4.printStackTrace();
        } catch (TableNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        try {
            conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
            conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));
        } catch (RepositoryException e3) {
            // TODO Auto-generated catch block
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
                + "SELECT ?c ?l ?f ?o " //
                + "{" //
                + "  <uri:entity> a ?c . "//
                + "  <uri:entity> <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  <uri:entity> <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?c a ?f . " //
                + "  <uri:entity> <uri:howlsAt> ?f. "//
                + "  ?f <uri:subType> <uri:obj>. "//
                + "}";//


        List<ExternalTupleSet> index = Lists.newArrayList();
        AccumuloIndexSet ais1 = null;
        AccumuloIndexSet ais2 = null;
        AccumuloIndexSet ais3 = null;

        try {
            ais1 = new AccumuloIndexSet(indexSparqlString, conn, accCon, tablename);
            ais2 = new AccumuloIndexSet(indexSparqlString2, conn, accCon, "table2");
            ais3 = new AccumuloIndexSet(indexSparqlString3, conn, accCon, "table3");
        } catch (MalformedQueryException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (SailException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (MutationsRejectedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (TableNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        index.add(ais1);
        index.add(ais3);
        index.add(ais2);

        CountingResultHandler crh1 = new CountingResultHandler();
        CountingResultHandler crh2 = new CountingResultHandler();

        try {
            conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh1);
        } catch (TupleQueryResultHandlerException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        } catch (QueryEvaluationException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        } catch (MalformedQueryException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        } catch (RepositoryException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

        ExternalProcessor processor = new ExternalProcessor(index);

        Sail processingSail = new ExternalSail(s, processor);
        SailRepository smartSailRepo = new SailRepository(processingSail);
        try {
            smartSailRepo.initialize();
        } catch (RepositoryException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        
        try {
            smartSailRepo.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh2);
        } catch (TupleQueryResultHandlerException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (MalformedQueryException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (RepositoryException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
       
        
        
        
       
//         Scanner s = null;
//        try {
//            s = accCon.createScanner("table3", new Authorizations());
//        } catch (TableNotFoundException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        } 
//         s.setRange(new Range());       
//         Iterator<Entry<Key,Value>> i = s.iterator();
//         
//         while (i.hasNext()) {
//             Entry<Key, Value> entry = i.next();
//             Key k = entry.getKey();
//             System.out.println(k);
//
//         } 
        
        
        
        
        
        
        Assert.assertEquals(crh1.getCount(), crh2.getCount());

        
        
        
        

    }
    
    
    
    
    
    
  @Test
  public void testEvaluateFilterInstantiate() {

      URI e1 = new URIImpl("uri:e1");
      URI e2 = new URIImpl("uri:e2");
      URI e3 = new URIImpl("uri:e3");
      URI f1 = new URIImpl("uri:f1");
      URI f2 = new URIImpl("uri:f2");
      URI f3 = new URIImpl("uri:f3");
      URI g1 = new URIImpl("uri:g1");
      URI g2 = new URIImpl("uri:g2");
      URI g3 = new URIImpl("uri:g3");
      

      
      try {
          conn.add(e1, talksTo, f1);
          conn.add(f1, talksTo, g1);
          conn.add(g1, talksTo, e1);
          conn.add(e2, talksTo, f2);
          conn.add(f2, talksTo, g2);
          conn.add(g2, talksTo, e2);
          conn.add(e3, talksTo, f3);
          conn.add(f3, talksTo, g3);
          conn.add(g3, talksTo, e3);
      } catch (RepositoryException e5) {
          // TODO Auto-generated catch block
          e5.printStackTrace();
      }


      String queryString = ""//
              + "SELECT ?x ?y ?z " //
              + "{" //
              + "Filter(?x = <uri:e1>) . " //
              + " ?x <uri:talksTo> ?y. " //
              + " ?y <uri:talksTo> ?z. " //
              + " ?z <uri:talksTo> <uri:e1>. " //
              + "}";//
      
      
      
      String indexSparqlString = ""//
              + "SELECT ?a ?b ?c ?d " //
              + "{" //
              + "Filter(?a = ?d) . " //
              + " ?a <uri:talksTo> ?b. " //
              + " ?b <uri:talksTo> ?c. " //
              + " ?c <uri:talksTo> ?d. " //
              + "}";//
      


      List<ExternalTupleSet> index = Lists.newArrayList();
      AccumuloIndexSet ais1 = null;

      try {
          ais1 = new AccumuloIndexSet(indexSparqlString, conn, accCon, tablename);
      } catch (MalformedQueryException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      } catch (SailException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      } catch (QueryEvaluationException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      } catch (MutationsRejectedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      } catch (TableNotFoundException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      }

      index.add(ais1);
   
      CountingResultHandler crh1 = new CountingResultHandler();
      CountingResultHandler crh2 = new CountingResultHandler();

      try {
          conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh1);
      } catch (TupleQueryResultHandlerException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      } catch (QueryEvaluationException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      } catch (MalformedQueryException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      } catch (RepositoryException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      }

      ExternalProcessor processor = new ExternalProcessor(index);

      Sail processingSail = new ExternalSail(s, processor);
      SailRepository smartSailRepo = new SailRepository(processingSail);
      try {
          smartSailRepo.initialize();
      } catch (RepositoryException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      }

      
      try {
          smartSailRepo.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh2);
      } catch (TupleQueryResultHandlerException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      } catch (QueryEvaluationException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      } catch (MalformedQueryException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      } catch (RepositoryException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      }
      
    
      
       
      
      
      
      
      Assert.assertEquals(crh1.getCount(), crh2.getCount());

      
      
      
      

  }
  
  
  
  
  @Test
  public void testEvaluateCompoundFilterInstantiate() {

      URI e1 = new URIImpl("uri:e1");
      URI f1 = new URIImpl("uri:f1");
      
      
      try {
          conn.add(e1, talksTo, e1);
          conn.add(e1, talksTo, f1);
          conn.add(f1, talksTo, e1);

      } catch (RepositoryException e5) {
          // TODO Auto-generated catch block
          e5.printStackTrace();
      }


      String queryString = ""//
              + "SELECT ?x ?y ?z " //
              + "{" //
              + "Filter(?x = <uri:e1> && ?y = <uri:e1>) . " //
              + " ?x <uri:talksTo> ?y. " //
              + " ?y <uri:talksTo> ?z. " //
              + " ?z <uri:talksTo> <uri:e1>. " //
              + "}";//
      
      
      
      String indexSparqlString = ""//
              + "SELECT ?a ?b ?c ?d " //
              + "{" //
              + "Filter(?a = ?d && ?b = ?d) . " //
              + " ?a <uri:talksTo> ?b. " //
              + " ?b <uri:talksTo> ?c. " //
              + " ?c <uri:talksTo> ?d. " //
              + "}";//
      


      List<ExternalTupleSet> index = Lists.newArrayList();
      AccumuloIndexSet ais1 = null;

      try {
          ais1 = new AccumuloIndexSet(indexSparqlString, conn, accCon, tablename);
      } catch (MalformedQueryException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      } catch (SailException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      } catch (QueryEvaluationException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      } catch (MutationsRejectedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      } catch (TableNotFoundException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      }

      index.add(ais1);
   
      CountingResultHandler crh1 = new CountingResultHandler();
      CountingResultHandler crh2 = new CountingResultHandler();

      try {
          conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh1);
      } catch (TupleQueryResultHandlerException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      } catch (QueryEvaluationException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      } catch (MalformedQueryException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      } catch (RepositoryException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      }

      ExternalProcessor processor = new ExternalProcessor(index);

      Sail processingSail = new ExternalSail(s, processor);
      SailRepository smartSailRepo = new SailRepository(processingSail);
      try {
          smartSailRepo.initialize();
      } catch (RepositoryException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      }

      
      try {
          smartSailRepo.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh2);
      } catch (TupleQueryResultHandlerException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      } catch (QueryEvaluationException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      } catch (MalformedQueryException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      } catch (RepositoryException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      }
      
//      System.out.println("Counts are " + crh1.getCount() + " and " + crh2.getCount());
//      
//      
//    Scanner s = null;
//   try {
//       s = accCon.createScanner(tablename, new Authorizations());
//   } catch (TableNotFoundException e) {
//       // TODO Auto-generated catch block
//       e.printStackTrace();
//   } 
//    s.setRange(new Range());       
//    Iterator<Entry<Key,Value>> i = s.iterator();
//    
//    while (i.hasNext()) {
//        Entry<Key, Value> entry = i.next();
//        Key k = entry.getKey();
//        System.out.println(k);
//
//    } 
      
      
    Assert.assertEquals(2, crh1.getCount());
    
    Assert.assertEquals(crh1.getCount(), crh2.getCount());

     
      
      

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
