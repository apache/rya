package mvm.rya.indexing.IndexPlanValidator;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import junit.framework.Assert;
import mvm.rya.indexing.external.ExternalProcessor;
import mvm.rya.indexing.external.tupleSet.ExternalTupleSet;
import mvm.rya.indexing.external.tupleSet.SimpleExternalTupleSet;

import org.junit.Test;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class GeneralizedExternalProcessorTest {

    private String q7 = ""//
            + "SELECT ?s ?t ?u " //
            + "{" //
            + "  ?s a ?t ."//
            + "  ?t <http://www.w3.org/2000/01/rdf-schema#label> ?u ."//
            + "  ?u <uri:talksTo> ?s . "//
            + "}";//
    
    
    private String q8 = ""//
            + "SELECT ?f ?m ?d ?e ?l ?c ?n ?o ?p ?a ?h ?r " //
            + "{" //
            + "  ?h <http://www.w3.org/2000/01/rdf-schema#label> ?r ."//
            + "  ?f a ?m ."//
            + "  ?p <uri:talksTo> ?n . "//
            + "  ?e a ?l ."//
            + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?p ."//
            + "  ?d <uri:talksTo> ?f . "//
            + "  ?c <uri:talksTo> ?e . "//
            + "  ?n a ?o ."//
            + "  ?a a ?h ."//
            + "  ?m <http://www.w3.org/2000/01/rdf-schema#label> ?d ."//
            + "  ?l <http://www.w3.org/2000/01/rdf-schema#label> ?c ."//
            + "  ?r <uri:talksTo> ?a . "//
            + "}";//
    
    
    
    
    private String q11 = ""//
            + "SELECT ?f ?m ?d ?e ?l ?c ?n ?o ?p ?a ?h ?r ?x ?y ?w ?t ?duck ?chicken ?pig ?rabbit " //
            + "{" //
            + "  ?w a ?t ."//
            + "  ?x a ?y ."//
            + "  ?duck a ?chicken ."//
            + "  ?pig a ?rabbit ."//
            + "  ?h <http://www.w3.org/2000/01/rdf-schema#label> ?r ."//
            + "  ?f a ?m ."//
            + "  ?p <uri:talksTo> ?n . "//
            + "  ?e a ?l ."//
            + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?p ."//
            + "  ?d <uri:talksTo> ?f . "//
            + "  ?c <uri:talksTo> ?e . "//
            + "  ?n a ?o ."//
            + "  ?a a ?h ."//
            + "  ?m <http://www.w3.org/2000/01/rdf-schema#label> ?d ."//
            + "  ?l <http://www.w3.org/2000/01/rdf-schema#label> ?c ."//
            + "  ?r <uri:talksTo> ?a . "//
            + "}";//
    
    
    private String q12 = ""//
            + "SELECT ?b ?p ?dog ?cat " //
            + "{" //
            + "  ?b a ?p ."//
            + "  ?dog a ?cat. "//
            + "}";//
    
    
    
    private String q13 = ""//
            + "SELECT ?f ?m ?d ?e ?l ?c ?n ?o ?p ?a ?h ?r ?x ?y ?w ?t ?duck ?chicken ?pig ?rabbit ?dick ?jane ?betty " //
            + "{" //
            + "  ?w a ?t ."//
            + "  ?x a ?y ."//
            + "  ?duck a ?chicken ."//
            + "  ?pig a ?rabbit ."//
            + "  ?h <http://www.w3.org/2000/01/rdf-schema#label> ?r ."//
            + "  ?f a ?m ."//
            + "  ?p <uri:talksTo> ?n . "//
            + "  ?e a ?l ."//
            + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?p ."//
            + "  ?d <uri:talksTo> ?f . "//
            + "  ?c <uri:talksTo> ?e . "//
            + "  ?n a ?o ."//
            + "  ?a a ?h ."//
            + "  ?m <http://www.w3.org/2000/01/rdf-schema#label> ?d ."//
            + "  ?l <http://www.w3.org/2000/01/rdf-schema#label> ?c ."//
            + "  ?r <uri:talksTo> ?a . "//
            + "  ?dick <uri:talksTo> ?jane . "//
            + "  ?jane <uri:talksTo> ?betty . "//
            + "}";//
    
    private String q14 = ""//
            + "SELECT ?f ?m ?d ?e ?l ?c ?n ?o ?p ?a ?h ?r ?x ?y ?w ?t ?duck ?chicken ?pig ?rabbit " //
            + "{" //
            + "  ?w a ?t ."//
            + "  ?x a ?y ."//
            + "  ?duck a ?chicken ."//
            + "  ?pig a ?rabbit ."//
            + "  ?h <http://www.w3.org/2000/01/rdf-schema#label> ?r ."//
            + "  ?f a ?m ."//
            + "  ?p <uri:talksTo> ?n . "//
            + "  ?e a ?l ."//
            + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?p ."//
            + "  ?d <uri:talksTo> ?f . "//
            + "  ?c <uri:talksTo> ?e . "//
            + "  ?n a ?o ."//
            + "  ?a a ?h ."//
            + "  ?m <http://www.w3.org/2000/01/rdf-schema#label> ?d ."//
            + "  ?l <http://www.w3.org/2000/01/rdf-schema#label> ?c ."//
            + "  ?r <uri:talksTo> ?a . "//
            + "  ?d <uri:talksTo> ?a . "//
            + "}";//
    
    
    private String q15 = ""//
            + "SELECT ?f ?m ?d ?e ?l ?c " //
            + "{" //
            + "  ?f a ?m ."//
            + "  ?e a ?l ."//
            + "  ?d <uri:talksTo> ?f . "//
            + "  ?c <uri:talksTo> ?e . "//
            + "  ?m <http://www.w3.org/2000/01/rdf-schema#label> ?d ."//
            + "  ?l <http://www.w3.org/2000/01/rdf-schema#label> ?c ."//
            + "}";//
    
    private String q16 = ""//
            + "SELECT ?f ?m ?d ?e ?l ?c " //
            + "{" //
            + "  ?d <uri:talksTo> ?f . "//
            + "  ?c <uri:talksTo> ?e . "//
            + "  ?m <http://www.w3.org/2000/01/rdf-schema#label> ?d ."//
            + "  ?l <http://www.w3.org/2000/01/rdf-schema#label> ?c ."//
            + "}";//
    
    private String q17 = ""//
            + "SELECT ?dog ?cat ?chicken " //
            + "{" //
            + "  ?chicken <uri:talksTo> ?dog . "//
            + "  ?cat <http://www.w3.org/2000/01/rdf-schema#label> ?chicken ."//
            + "}";//
    
    private String q18 = ""//
            + "SELECT ?dog ?chicken " //
            + "{" //
            + "  ?chicken <uri:talksTo> ?dog . "//
            + "}";//
    
    private String q19 = ""//
            + "SELECT ?cat ?chicken " //
            + "{" //
            + "  ?cat <http://www.w3.org/2000/01/rdf-schema#label> ?chicken ."//
            + "}";//
    
    
    
    
    
    
    
    
    
    //@Test
    public void testTwoIndexLargeQuery() throws Exception {

        SPARQLParser parser = new SPARQLParser();
        

        ParsedQuery pq1 = parser.parseQuery(q15, null);
        ParsedQuery pq2 = parser.parseQuery(q7, null);
        ParsedQuery pq3 = parser.parseQuery(q12, null);
       
        

        System.out.println("Query is " + pq1.getTupleExpr());
        
        SimpleExternalTupleSet extTup1 = new SimpleExternalTupleSet(new Projection(pq2.getTupleExpr()));
        SimpleExternalTupleSet extTup2 = new SimpleExternalTupleSet(new Projection(pq3.getTupleExpr()));
        //SimpleExternalTupleSet extTup3 = new SimpleExternalTupleSet(new Projection(pq5.getTupleExpr()));
       

        List<ExternalTupleSet> list = new ArrayList<ExternalTupleSet>();
       
        list.add(extTup2);
       //list.add(extTup3);
       list.add(extTup1);
        

        IndexedExecutionPlanGenerator iep = new IndexedExecutionPlanGenerator(pq1.getTupleExpr(),list);
        List<ExternalTupleSet> indexSet = iep.getNormalizedIndices();
        Assert.assertEquals(4, indexSet.size());
        
//        System.out.println("Normalized indices are: ");
//        for(ExternalTupleSet e: indexSet) {
//            System.out.println(e.getTupleExpr());
//        }
        
        Set<TupleExpr> processedTups = Sets.newHashSet(iep.getIndexedTuples());
        
        Assert.assertEquals(5, processedTups.size());
        
     //   System.out.println("Size is " + processedTups.size());
        
//        System.out.println("Indexed tuples are :" );
//        for(TupleExpr te: processedTups) {
//            System.out.println(te);
//        }
        
        
       
        


    }
    
    
    
    
    
    @Test
    public void testThreeIndexQuery() throws Exception {

        SPARQLParser parser = new SPARQLParser();
        

        ParsedQuery pq1 = parser.parseQuery(q16, null);
        ParsedQuery pq2 = parser.parseQuery(q17, null);
        ParsedQuery pq3 = parser.parseQuery(q18, null);
        ParsedQuery pq4 = parser.parseQuery(q19, null);
       
        

        System.out.println("Query is " + pq1.getTupleExpr());
        
        SimpleExternalTupleSet extTup1 = new SimpleExternalTupleSet(new Projection(pq2.getTupleExpr()));
        SimpleExternalTupleSet extTup2 = new SimpleExternalTupleSet(new Projection(pq3.getTupleExpr()));
        SimpleExternalTupleSet extTup3 = new SimpleExternalTupleSet(new Projection(pq4.getTupleExpr()));
       

        List<ExternalTupleSet> list = new ArrayList<ExternalTupleSet>();
       
        list.add(extTup2);
        list.add(extTup3);
        list.add(extTup1);
        

        IndexedExecutionPlanGenerator iep = new IndexedExecutionPlanGenerator(pq1.getTupleExpr(),list);
        List<ExternalTupleSet> indexSet = iep.getNormalizedIndices();
        Assert.assertEquals(6, indexSet.size());
        
//        System.out.println("Normalized indices are: ");
//        for(ExternalTupleSet e: indexSet) {
//            System.out.println(e.getTupleExpr());
//        }
        
        Set<TupleExpr> processedTups = Sets.newHashSet(iep.getIndexedTuples());
        
        Assert.assertEquals(17, processedTups.size());
        
      //  System.out.println("Size is " + processedTups.size());
        
//        System.out.println("Indexed tuples are :" );
//        for(TupleExpr te: processedTups) {
//            System.out.println(te);
//        }
        
        
        TupleExecutionPlanGenerator tep = new TupleExecutionPlanGenerator();
        List<TupleExpr> plans = Lists.newArrayList(tep.getPlans(processedTups.iterator()));
        
        
      System.out.println("Size is " + plans.size());
        
      System.out.println("Possible indexed tuple plans are :" );
      for(TupleExpr te: plans) {
          System.out.println(te);
      }
       
        


    }
    
    
    
    
    


}
