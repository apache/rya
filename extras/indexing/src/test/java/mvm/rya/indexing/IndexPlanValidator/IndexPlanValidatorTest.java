package mvm.rya.indexing.IndexPlanValidator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import junit.framework.Assert;
import mvm.rya.indexing.IndexPlanValidator.ThreshholdPlanSelectorTest.NodeCollector;
import mvm.rya.indexing.external.ExternalProcessor;
import mvm.rya.indexing.external.tupleSet.ExternalTupleSet;
import mvm.rya.indexing.external.tupleSet.SimpleExternalTupleSet;

import org.junit.Test;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

import com.google.common.collect.Lists;

public class IndexPlanValidatorTest {

    
    @Test
    public void testEvaluateTwoIndexTwoVarOrder1() {
        
        System.out.println("********************Test number 1***************************");
        
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

        
       
        SPARQLParser sp = new SPARQLParser();
        ParsedQuery index1 = null;
        ParsedQuery index2 = null;
        try {
            index1 = sp.parseQuery(indexSparqlString, null);
            index2 = sp.parseQuery(indexSparqlString2, null);
        } catch (MalformedQueryException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        
          
        List<ExternalTupleSet> index = Lists.newArrayList();
        
        SimpleExternalTupleSet ais1 = new SimpleExternalTupleSet((Projection)index1.getTupleExpr());
        SimpleExternalTupleSet ais2 = new SimpleExternalTupleSet((Projection)index2.getTupleExpr());;
                
        index.add(ais1);
        index.add(ais2);
        
        ParsedQuery pq = null;

        try {
            pq = sp.parseQuery(queryString, null);
        } catch (MalformedQueryException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
       
        ExternalProcessor processor = new ExternalProcessor(index);
        TupleExpr tup = processor.process(pq.getTupleExpr());

        System.out.println("TupleExpr is " + tup);
        
        IndexPlanValidator ipv = new IndexPlanValidator(false);
        Assert.assertEquals(false, ipv.isValid(tup));
        
    }
    
    
    
    @Test
    public void testEvaluateTwoIndexTwoVarOrder2() {
        
        System.out.println("********************Test number 2***************************");
        
        // TODO Auto-generated method stub
        String indexSparqlString = ""//
                + "SELECT ?e ?l ?c  " //
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

        
       
        SPARQLParser sp = new SPARQLParser();
        ParsedQuery index1 = null;
        ParsedQuery index2 = null;
        try {
            index1 = sp.parseQuery(indexSparqlString, null);
            index2 = sp.parseQuery(indexSparqlString2, null);
        } catch (MalformedQueryException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        
          
        List<ExternalTupleSet> index = Lists.newArrayList();
        
        SimpleExternalTupleSet ais1 = new SimpleExternalTupleSet((Projection)index1.getTupleExpr());
        SimpleExternalTupleSet ais2 = new SimpleExternalTupleSet((Projection)index2.getTupleExpr());;
                
        index.add(ais1);
        index.add(ais2);
        
        ParsedQuery pq = null;

        try {
            pq = sp.parseQuery(queryString, null);
        } catch (MalformedQueryException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
       
        ExternalProcessor processor = new ExternalProcessor(index);
        TupleExpr tup = processor.process(pq.getTupleExpr());

        System.out.println("TupleExpr is " + tup);
        
        IndexPlanValidator ipv = new IndexPlanValidator(false);
        Assert.assertEquals(true, ipv.isValid(tup));
        
    }
    
    
    
    
    
    @Test
    public void testEvaluateTwoIndexTwoVarOrder3() {
      
        
        System.out.println("********************Test number 3***************************");
        
        // TODO Auto-generated method stub
        String indexSparqlString = ""//
                + "SELECT ?l ?e ?c  " //
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

        
       
        SPARQLParser sp = new SPARQLParser();
        ParsedQuery index1 = null;
        ParsedQuery index2 = null;
        try {
            index1 = sp.parseQuery(indexSparqlString, null);
            index2 = sp.parseQuery(indexSparqlString2, null);
        } catch (MalformedQueryException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        
          
        List<ExternalTupleSet> index = Lists.newArrayList();
        
        SimpleExternalTupleSet ais1 = new SimpleExternalTupleSet((Projection)index1.getTupleExpr());
        SimpleExternalTupleSet ais2 = new SimpleExternalTupleSet((Projection)index2.getTupleExpr());;
                
        index.add(ais1);
        index.add(ais2);
        
        ParsedQuery pq = null;

        try {
            pq = sp.parseQuery(queryString, null);
        } catch (MalformedQueryException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
       
        ExternalProcessor processor = new ExternalProcessor(index);
        TupleExpr tup = processor.process(pq.getTupleExpr());

        System.out.println("TupleExpr is " + tup);
        
        IndexPlanValidator ipv = new IndexPlanValidator(false);
        Assert.assertEquals(true, ipv.isValid(tup));
        
    }
    
    
    
    @Test
    public void testEvaluateTwoIndexTwoVarOrder4() {
        
        
        System.out.println("********************Test number 4***************************");
        
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

        
       
        SPARQLParser sp = new SPARQLParser();
        ParsedQuery index1 = null;
        ParsedQuery index2 = null;
        try {
            index1 = sp.parseQuery(indexSparqlString, null);
            index2 = sp.parseQuery(indexSparqlString2, null);
        } catch (MalformedQueryException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        
          
        List<ExternalTupleSet> index = Lists.newArrayList();
        
        SimpleExternalTupleSet ais1 = new SimpleExternalTupleSet((Projection)index1.getTupleExpr());
        SimpleExternalTupleSet ais2 = new SimpleExternalTupleSet((Projection)index2.getTupleExpr());;
                
        index.add(ais1);
        index.add(ais2);
        
        ParsedQuery pq = null;

        try {
            pq = sp.parseQuery(queryString, null);
        } catch (MalformedQueryException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
       
        ExternalProcessor processor = new ExternalProcessor(index);
        TupleExpr tup = processor.process(pq.getTupleExpr());

        System.out.println("TupleExpr is " + tup);
        
        IndexPlanValidator ipv = new IndexPlanValidator(false);
        Assert.assertEquals(false, ipv.isValid(tup));
        
    }
    
    
    
    @Test
    public void testEvaluateTwoIndexTwoVarOrder5() {
        
        System.out.println("********************Test number 5***************************");
        
        // TODO Auto-generated method stub
        String indexSparqlString = ""//
                + "SELECT ?e ?l ?c  " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//
        
        
        String indexSparqlString2 = ""//
                + "SELECT ?l ?o ?e " //
                + "{" //
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l ."//
                + "  ?e <uri:talksTo> ?o . "//
                + "}";//
        
        
        String queryString = ""//
                + "SELECT ?e ?c ?l ?o " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//

        
       
        SPARQLParser sp = new SPARQLParser();
        ParsedQuery index1 = null;
        ParsedQuery index2 = null;
        try {
            index1 = sp.parseQuery(indexSparqlString, null);
            index2 = sp.parseQuery(indexSparqlString2, null);
        } catch (MalformedQueryException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        
        
          
        List<ExternalTupleSet> index = Lists.newArrayList();
        
        SimpleExternalTupleSet ais1 = new SimpleExternalTupleSet((Projection)index1.getTupleExpr());
        SimpleExternalTupleSet ais2 = new SimpleExternalTupleSet((Projection)index2.getTupleExpr());
        
        System.out.println("Supported variable orders are " + ais1.getSupportedVariableOrders() + ", " + ais2.getSupportedVariableOrders());
                
        index.add(ais2);
        index.add(ais1);
        
        
        ParsedQuery pq = null;

        try {
            pq = sp.parseQuery(queryString, null);
        } catch (MalformedQueryException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
       
        System.out.println("query assured binding names are " + pq.getTupleExpr().getAssuredBindingNames());
        
        ExternalProcessor processor = new ExternalProcessor(index);
        TupleExpr tup = processor.process(pq.getTupleExpr());

        System.out.println("TupleExpr is " + tup);
        
        IndexPlanValidator ipv = new IndexPlanValidator(false);
        Assert.assertEquals(false, ipv.isValid(tup));
        
    }
    
    
    
    
    
    @Test
    public void testEvaluateTwoIndexTwoVarOrder6() {
        
        
        System.out.println("********************Test number 6***************************");
        
        // TODO Auto-generated method stub
        String indexSparqlString = ""//
                + "SELECT ?e ?l ?c  " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//
        
        
        String indexSparqlString2 = ""//
                + "SELECT ?l ?e ?o " //
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

        
       
        SPARQLParser sp = new SPARQLParser();
        ParsedQuery index1 = null;
        ParsedQuery index2 = null;
        try {
            index1 = sp.parseQuery(indexSparqlString, null);
            index2 = sp.parseQuery(indexSparqlString2, null);
        } catch (MalformedQueryException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        
        
      
        List<ExternalTupleSet> index = Lists.newArrayList();
        
        SimpleExternalTupleSet ais1 = new SimpleExternalTupleSet((Projection)index1.getTupleExpr());
        SimpleExternalTupleSet ais2 = new SimpleExternalTupleSet((Projection)index2.getTupleExpr());;
                
        index.add(ais2);
        index.add(ais1);
        
        
        ParsedQuery pq = null;

        try {
            pq = sp.parseQuery(queryString, null);
        } catch (MalformedQueryException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
       
        ExternalProcessor processor = new ExternalProcessor(index);
        TupleExpr tup = processor.process(pq.getTupleExpr());

        System.out.println("TupleExpr is " + tup);
        
        IndexPlanValidator ipv = new IndexPlanValidator(false);
        Assert.assertEquals(true, ipv.isValid(tup));
        
    }
    
    
    
    
    @Test
    public void testEvaluateTwoIndexCrossProduct1() {
        
        System.out.println("********************Test number 7***************************");
        
        // TODO Auto-generated method stub
        String indexSparqlString = ""//
                + "SELECT ?e ?l ?c  " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//
        
        
        String indexSparqlString2 = ""//
                + "SELECT ?e ?l ?o " //
                + "{" //
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//
        
        
        String queryString = ""//
                + "SELECT ?e ?c ?l ?o ?f ?g " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
                + "  ?f <uri:talksTo> ?g . " //
                + "}";//

        
       
        SPARQLParser sp = new SPARQLParser();
        ParsedQuery index1 = null;
        ParsedQuery index2 = null;
        try {
            index1 = sp.parseQuery(indexSparqlString, null);
            index2 = sp.parseQuery(indexSparqlString2, null);
        } catch (MalformedQueryException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        
       
          
        List<ExternalTupleSet> index = Lists.newArrayList();
        
        SimpleExternalTupleSet ais1 = new SimpleExternalTupleSet((Projection)index1.getTupleExpr());
        SimpleExternalTupleSet ais2 = new SimpleExternalTupleSet((Projection)index2.getTupleExpr());;
                
        index.add(ais2);
        index.add(ais1);
        
        
        ParsedQuery pq = null;

        try {
            pq = sp.parseQuery(queryString, null);
        } catch (MalformedQueryException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
       
        ExternalProcessor processor = new ExternalProcessor(index);
        TupleExpr tup = processor.process(pq.getTupleExpr());

        System.out.println("TupleExpr is " + tup);
        
        IndexPlanValidator ipv = new IndexPlanValidator(true);
        Assert.assertEquals(false, ipv.isValid(tup));
        
    }
    
    
    
    
    @Test
    public void testEvaluateTwoIndexCrossProduct2() {
        
        System.out.println("********************Test number 8***************************");
        
        // TODO Auto-generated method stub
        String indexSparqlString = ""//
                + "SELECT ?e ?l ?c  " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//
        
        
        String indexSparqlString2 = ""//
                + "SELECT ?e ?l ?o " //
                + "{" //
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//
        
        
        String queryString = ""//
                + "SELECT ?e ?c ?l ?o ?f ?g " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
                + "  ?f <uri:talksTo> ?g . " //
                + "}";//

        
       
        SPARQLParser sp = new SPARQLParser();
        ParsedQuery index1 = null;
        ParsedQuery index2 = null;
        try {
            index1 = sp.parseQuery(indexSparqlString, null);
            index2 = sp.parseQuery(indexSparqlString2, null);
        } catch (MalformedQueryException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        
          
        List<ExternalTupleSet> index = Lists.newArrayList();
        
        SimpleExternalTupleSet ais1 = new SimpleExternalTupleSet((Projection)index1.getTupleExpr());
        SimpleExternalTupleSet ais2 = new SimpleExternalTupleSet((Projection)index2.getTupleExpr());;
                
        
        index.add(ais1);
        index.add(ais2);
        
        
        ParsedQuery pq = null;

        try {
            pq = sp.parseQuery(queryString, null);
        } catch (MalformedQueryException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
       
        ExternalProcessor processor = new ExternalProcessor(index);
        TupleExpr tup = processor.process(pq.getTupleExpr());

        System.out.println("TupleExpr is " + tup);
        
        IndexPlanValidator ipv = new IndexPlanValidator(true);
        Assert.assertEquals(false, ipv.isValid(tup));
        
    }
    
    
    
    @Test
    public void testEvaluateTwoIndexCrossProduct3() {
        
        System.out.println("********************Test number 9***************************");
        
        // TODO Auto-generated method stub
        String indexSparqlString = ""//
                + "SELECT ?e ?l ?c  " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//
        
        
        String indexSparqlString2 = ""//
                + "SELECT ?e ?l ?o " //
                + "{" //
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//
        
        
        String queryString = ""//
                + "SELECT ?e ?c ?l ?o ?f ?g " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
                + "  ?f <uri:talksTo> ?g . " //
                + "}";//

        
       
        SPARQLParser sp = new SPARQLParser();
        ParsedQuery index1 = null;
        ParsedQuery index2 = null;
        try {
            index1 = sp.parseQuery(indexSparqlString, null);
            index2 = sp.parseQuery(indexSparqlString2, null);
        } catch (MalformedQueryException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        
          
        List<ExternalTupleSet> index = Lists.newArrayList();
        
        SimpleExternalTupleSet ais1 = new SimpleExternalTupleSet((Projection)index1.getTupleExpr());
        SimpleExternalTupleSet ais2 = new SimpleExternalTupleSet((Projection)index2.getTupleExpr());;
                
        
        index.add(ais1);
        index.add(ais2);
        
        
        ParsedQuery pq = null;

        try {
            pq = sp.parseQuery(queryString, null);
        } catch (MalformedQueryException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
       
        ExternalProcessor processor = new ExternalProcessor(index);
        TupleExpr tup = processor.process(pq.getTupleExpr());

        System.out.println("TupleExpr is " + tup);
        
        IndexPlanValidator ipv = new IndexPlanValidator(false);
        Assert.assertEquals(true, ipv.isValid(tup));
        
    }
    
    
    
    
    
    
   
    
    @Test
    public void testEvaluateTwoIndexDiffVars() {
        
        System.out.println("********************Test number 10***************************");
        
        // TODO Auto-generated method stub
        String indexSparqlString = ""//
                + "SELECT ?chicken ?dog ?pig  " //
                + "{" //
                + "  ?dog a ?chicken . "//
                + "  ?dog <http://www.w3.org/2000/01/rdf-schema#label> ?pig "//
                + "}";//
        
        
        String indexSparqlString2 = ""//
                + "SELECT ?fish ?ant ?turkey " //
                + "{" //
                + "  ?fish <uri:talksTo> ?turkey . "//
                + "  ?turkey <http://www.w3.org/2000/01/rdf-schema#label> ?ant "//
                + "}";//
        
        
        String queryString = ""//
                + "SELECT ?e ?c ?l ?o ?f ?g " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
                + "  ?f <uri:talksTo> ?g . " //
                + "}";//

        
       
        SPARQLParser sp = new SPARQLParser();
        ParsedQuery index1 = null;
        ParsedQuery index2 = null;
        try {
            index1 = sp.parseQuery(indexSparqlString, null);
            index2 = sp.parseQuery(indexSparqlString2, null);
        } catch (MalformedQueryException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        
          
        List<ExternalTupleSet> index = Lists.newArrayList();
        
        SimpleExternalTupleSet ais1 = new SimpleExternalTupleSet((Projection)index1.getTupleExpr());
        SimpleExternalTupleSet ais2 = new SimpleExternalTupleSet((Projection)index2.getTupleExpr());;
                
        
        index.add(ais1);
        index.add(ais2);
        
        
        ParsedQuery pq = null;

        try {
            pq = sp.parseQuery(queryString, null);
        } catch (MalformedQueryException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
       
        ExternalProcessor processor = new ExternalProcessor(index);
        TupleExpr tup = processor.process(pq.getTupleExpr());

        System.out.println("TupleExpr is " + tup);
        
        IndexPlanValidator ipv = new IndexPlanValidator(false);
        Assert.assertEquals(false, ipv.isValid(tup));
        
    }
    
    
    
    @Test
    public void testEvaluateTwoIndexDiffVars2() {
        
        System.out.println("********************Test number 11***************************");
        
        // TODO Auto-generated method stub
        String indexSparqlString = ""//
                + "SELECT ?dog ?pig ?chicken  " //
                + "{" //
                + "  ?dog a ?chicken . "//
                + "  ?dog <http://www.w3.org/2000/01/rdf-schema#label> ?pig "//
                + "}";//
        
        
        String indexSparqlString2 = ""//
                + "SELECT ?fish ?ant ?turkey " //
                + "{" //
                + "  ?fish <uri:talksTo> ?turkey . "//
                + "  ?turkey <http://www.w3.org/2000/01/rdf-schema#label> ?ant "//
                + "}";//
        
        
        String queryString = ""//
                + "SELECT ?e ?c ?l ?o ?f ?g " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
                + "  ?f <uri:talksTo> ?g . " //
                + "}";//

        
       
        SPARQLParser sp = new SPARQLParser();
        ParsedQuery index1 = null;
        ParsedQuery index2 = null;
        try {
            index1 = sp.parseQuery(indexSparqlString, null);
            index2 = sp.parseQuery(indexSparqlString2, null);
        } catch (MalformedQueryException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        
          
        List<ExternalTupleSet> index = Lists.newArrayList();
        
        SimpleExternalTupleSet ais1 = new SimpleExternalTupleSet((Projection)index1.getTupleExpr());
        SimpleExternalTupleSet ais2 = new SimpleExternalTupleSet((Projection)index2.getTupleExpr());;
                
        
        index.add(ais1);
        index.add(ais2);
        
        
        ParsedQuery pq = null;

        try {
            pq = sp.parseQuery(queryString, null);
        } catch (MalformedQueryException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
       
        ExternalProcessor processor = new ExternalProcessor(index);
        TupleExpr tup = processor.process(pq.getTupleExpr());

        System.out.println("TupleExpr is " + tup);
        
        IndexPlanValidator ipv = new IndexPlanValidator(false);
        Assert.assertEquals(true, ipv.isValid(tup));
        
    }
    
    
    @Test
    public void testEvaluateTwoIndexDiffVars3() {
        
        System.out.println("********************Test number 11***************************");
        
        // TODO Auto-generated method stub
        String indexSparqlString = ""//
                + "SELECT ?pig ?dog ?chicken  " //
                + "{" //
                + "  ?dog a ?chicken . "//
                + "  ?dog <http://www.w3.org/2000/01/rdf-schema#label> ?pig "//
                + "}";//
        
        
        String indexSparqlString2 = ""//
                + "SELECT ?fish ?ant ?turkey " //
                + "{" //
                + "  ?fish <uri:talksTo> ?turkey . "//
                + "  ?turkey <http://www.w3.org/2000/01/rdf-schema#label> ?ant "//
                + "}";//
        
        
        String queryString = ""//
                + "SELECT ?e ?c ?l ?o ?f ?g " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
                + "  ?f <uri:talksTo> ?g . " //
                + "}";//

        
       
        SPARQLParser sp = new SPARQLParser();
        ParsedQuery index1 = null;
        ParsedQuery index2 = null;
        try {
            index1 = sp.parseQuery(indexSparqlString, null);
            index2 = sp.parseQuery(indexSparqlString2, null);
        } catch (MalformedQueryException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        
          
        List<ExternalTupleSet> index = Lists.newArrayList();
        
        SimpleExternalTupleSet ais1 = new SimpleExternalTupleSet((Projection)index1.getTupleExpr());
        SimpleExternalTupleSet ais2 = new SimpleExternalTupleSet((Projection)index2.getTupleExpr());;
                
        
        index.add(ais1);
        index.add(ais2);
        
        
        ParsedQuery pq = null;

        try {
            pq = sp.parseQuery(queryString, null);
        } catch (MalformedQueryException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
       
        ExternalProcessor processor = new ExternalProcessor(index);
        TupleExpr tup = processor.process(pq.getTupleExpr());

        System.out.println("TupleExpr is " + tup);
        
        IndexPlanValidator ipv = new IndexPlanValidator(false);
        Assert.assertEquals(true, ipv.isValid(tup));
        
    }
    
    
    
    
    @Test
    public void testEvaluateTwoIndexDiffVarsDirProd() {
        
        System.out.println("********************Test number 12***************************");
        
        // TODO Auto-generated method stub
        String indexSparqlString = ""//
                + "SELECT ?pig ?dog ?chicken  " //
                + "{" //
                + "  ?dog a ?chicken . "//
                + "  ?dog <http://www.w3.org/2000/01/rdf-schema#label> ?pig "//
                + "}";//
        
        
        String indexSparqlString2 = ""//
                + "SELECT ?fish ?ant ?turkey " //
                + "{" //
                + "  ?fish <uri:talksTo> ?turkey . "//
                + "  ?turkey <http://www.w3.org/2000/01/rdf-schema#label> ?ant "//
                + "}";//
        
        
        String queryString = ""//
                + "SELECT ?e ?c ?l ?o ?f ?g " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
                + "  ?f <uri:talksTo> ?g . " //
                + "}";//

        
       
        SPARQLParser sp = new SPARQLParser();
        ParsedQuery index1 = null;
        ParsedQuery index2 = null;
        try {
            index1 = sp.parseQuery(indexSparqlString, null);
            index2 = sp.parseQuery(indexSparqlString2, null);
        } catch (MalformedQueryException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        
          
        List<ExternalTupleSet> index = Lists.newArrayList();
        
        SimpleExternalTupleSet ais1 = new SimpleExternalTupleSet((Projection)index1.getTupleExpr());
        SimpleExternalTupleSet ais2 = new SimpleExternalTupleSet((Projection)index2.getTupleExpr());;
                
        
        index.add(ais1);
        index.add(ais2);
        
        
        ParsedQuery pq = null;

        try {
            pq = sp.parseQuery(queryString, null);
        } catch (MalformedQueryException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
       
        ExternalProcessor processor = new ExternalProcessor(index);
        TupleExpr tup = processor.process(pq.getTupleExpr());

        System.out.println("TupleExpr is " + tup);
        
        IndexPlanValidator ipv = new IndexPlanValidator(true);
        Assert.assertEquals(false, ipv.isValid(tup));
        
    }
    
    
    
    @Test
    public void testValidTupleIterator() throws Exception {
        
        System.out.println("********************Test number 13***************************");
        
        String q1 = ""//
                + "SELECT ?f ?m ?d ?h ?i " //
                + "{" //
                + "  ?f a ?m ."//
                + "  ?m <http://www.w3.org/2000/01/rdf-schema#label> ?d ."//
                + "  ?d <uri:talksTo> ?f . "//
                + "  ?d <uri:hangOutWith> ?f ." //
                + "  ?f <uri:hangOutWith> ?h ." //
                + "  ?f <uri:associatesWith> ?i ." //
                + "  ?i <uri:associatesWith> ?h ." //
                + "}";//

        String q2 = ""//
                + "SELECT ?t ?s ?u " //
                + "{" //
                + "  ?s a ?t ."//
                + "  ?t <http://www.w3.org/2000/01/rdf-schema#label> ?u ."//
                + "  ?u <uri:talksTo> ?s . "//
                + "}";//
        
        
        String q3 = ""//
                + "SELECT ?s ?t ?u " //
                + "{" //
                + "  ?s <uri:hangOutWith> ?t ." //
                + "  ?t <uri:hangOutWith> ?u ." //
                + "}";//
        
        String q4 = ""//
                + "SELECT ?s ?t ?u " //
                + "{" //
                + "  ?s <uri:associatesWith> ?t ." //
                + "  ?t <uri:associatesWith> ?u ." //
                + "}";//
        
      
        
        
        

        SPARQLParser parser = new SPARQLParser();

        ParsedQuery pq1 = parser.parseQuery(q1, null);
        ParsedQuery pq2 = parser.parseQuery(q2, null);
        ParsedQuery pq3 = parser.parseQuery(q3, null);
        ParsedQuery pq4 = parser.parseQuery(q4, null);
     

        SimpleExternalTupleSet extTup1 = new SimpleExternalTupleSet((Projection) pq2.getTupleExpr());
        SimpleExternalTupleSet extTup2 = new SimpleExternalTupleSet((Projection) pq3.getTupleExpr());
        SimpleExternalTupleSet extTup3 = new SimpleExternalTupleSet((Projection) pq4.getTupleExpr());
        
        List<ExternalTupleSet> list = new ArrayList<ExternalTupleSet>();

        list.add(extTup2);
        list.add(extTup1);
        list.add(extTup3);

        IndexedExecutionPlanGenerator iep = new IndexedExecutionPlanGenerator(pq1.getTupleExpr(), list);

        Iterator<TupleExpr> plans = (new TupleExecutionPlanGenerator()).getPlans(iep.getIndexedTuples());
        IndexPlanValidator ipv = new IndexPlanValidator(true);
        Iterator<TupleExpr> validPlans = ipv.getValidTuples(plans);
        
        int size = 0;
        
        while(validPlans.hasNext()) {
            Assert.assertTrue(validPlans.hasNext());
            validPlans.next();
            size++;
        }
        
        Assert.assertTrue(!validPlans.hasNext());
        Assert.assertEquals(732, size);

       

    }
    
    
    
    
    
    
    
    
    
    
    
    
    
}
