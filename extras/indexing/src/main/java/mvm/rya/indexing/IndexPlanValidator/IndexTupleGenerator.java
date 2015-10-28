package mvm.rya.indexing.IndexPlanValidator;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.openrdf.query.algebra.TupleExpr;

public interface IndexTupleGenerator {
    
    
    public Iterator<TupleExpr> getPlans(Iterator<TupleExpr> indexPlans);
    

}
