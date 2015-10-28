package mvm.rya.indexing.IndexPlanValidator;

import java.util.Iterator;
import java.util.List;

import org.openrdf.query.algebra.TupleExpr;

public interface IndexedQueryPlanSelector {
    
    public TupleExpr getThreshholdQueryPlan(Iterator<TupleExpr> tupleList, double threshhold, 
            double indexWeight, double commonVarWeight, double dirProdWeight);

}
