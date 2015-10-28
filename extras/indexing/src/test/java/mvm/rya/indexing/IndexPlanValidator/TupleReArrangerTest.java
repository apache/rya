package mvm.rya.indexing.IndexPlanValidator;

import java.util.List;

import junit.framework.Assert;

import org.junit.Test;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.evaluation.impl.FilterOptimizer;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

public class TupleReArrangerTest {

    @Test
    public void tupleReArrangeTest1() throws MalformedQueryException {
        
        String queryString = ""//
                + "SELECT ?a ?b ?c ?d ?e" //
                + "{" //
                + "{ ?a a ?b .  ?a <http://www.w3.org/2000/01/rdf-schema#label> ?c  }"//
                + " UNION { ?a <uri:talksTo> ?d .  ?a <http://www.w3.org/2000/01/rdf-schema#label> ?e  }"//
                + "}";//
        
        SPARQLParser sp = new SPARQLParser();
        ParsedQuery pq = sp.parseQuery(queryString, null);
        List<TupleExpr> tuples = TupleReArranger.getTupleReOrderings(pq.getTupleExpr());
        
        Assert.assertEquals(4, tuples.size());
        
    }
    
    
    
    @Test
    public void tupleReArrangeTest2() throws MalformedQueryException {
        
        String queryString = ""//
                + "SELECT ?a ?b ?c ?d ?e ?x ?y" //
                + "{" //
                + " ?e <uri:laughsAt> ?x ." //
                + " ?e <uri:livesIn> ?y . "//
                + "{ ?a a ?b .  ?a <http://www.w3.org/2000/01/rdf-schema#label> ?c  }"//
                + " UNION { ?a <uri:talksTo> ?d .  ?a <http://www.w3.org/2000/01/rdf-schema#label> ?e  }"//
                + "}";//
        
        SPARQLParser sp = new SPARQLParser();
        ParsedQuery pq = sp.parseQuery(queryString, null);
        List<TupleExpr> tuples = TupleReArranger.getTupleReOrderings(pq.getTupleExpr());
        
        
        Assert.assertEquals(24, tuples.size());
        
    }
    
    
    
    
    
    @Test
    public void tupleReArrangeTest3() throws MalformedQueryException {
        
        String queryString = ""//
                + "SELECT ?a ?b ?c ?d ?e ?x ?y" //
                + "{" //
                + " Filter(?c = <uri:label2>)" //
                + " Filter(?x = <uri:somethingFunny>) "//
                + " ?e <uri:laughsAt> ?x ." //
                + " ?e <uri:livesIn> ?y . "//
                + "{ ?a a ?b .  ?a <http://www.w3.org/2000/01/rdf-schema#label> ?c  }"//
                + " UNION { ?a <uri:talksTo> ?d .  ?a <http://www.w3.org/2000/01/rdf-schema#label> ?e  }"//
                + "}";//
        
        SPARQLParser sp = new SPARQLParser();
        ParsedQuery pq = sp.parseQuery(queryString, null);
        List<TupleExpr> tuples = TupleReArranger.getTupleReOrderings(pq.getTupleExpr());
        
        Assert.assertEquals(24, tuples.size());
        
    }
    
    
    
    
    
    
    
    @Test
    public void tupleReArrangeTest4() throws MalformedQueryException {
        
        String queryString = ""//
                + "SELECT ?a ?b ?c ?d ?e ?x ?y" //
                + "{" //
                + " Filter(?c = <uri:label2>)" //
                + " Filter(?x = <uri:somethingFunny>) "//
                + " Filter(?d = <uri:Fred> ) " //
                + " ?e <uri:laughsAt> ?x ." //
                + " ?e <uri:livesIn> ?y . "//
                + "{ ?a a ?b .  ?a <http://www.w3.org/2000/01/rdf-schema#label> ?c  }"//
                + " UNION { ?a <uri:talksTo> ?d .  ?a <http://www.w3.org/2000/01/rdf-schema#label> ?e  }"//
                + "}";//
        
        SPARQLParser sp = new SPARQLParser();
        ParsedQuery pq = sp.parseQuery(queryString, null);
        TupleExpr te = pq.getTupleExpr();
        (new FilterOptimizer()).optimize(te, null, null);
        System.out.println(te);
        List<TupleExpr> tuples = TupleReArranger.getTupleReOrderings(te);
        System.out.println(tuples);
        
        Assert.assertEquals(24, tuples.size());
        
    }
    
    
    
    
    

}
