package mvm.rya.indexing.IndexPlanValidator;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import mvm.rya.indexing.external.tupleSet.ExternalTupleSet;

import org.openrdf.query.algebra.TupleExpr;

public interface ExternalIndexMatcher {
    
    
    public Iterator<TupleExpr> getIndexedTuples();
   
    

}
