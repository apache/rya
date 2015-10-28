package mvm.rya.indexing.IndexPlanValidator;

import java.util.Iterator;
import java.util.List;

import org.openrdf.query.algebra.TupleExpr;

public interface TupleValidator {

    public boolean isValid(TupleExpr te);
    
    public Iterator<TupleExpr> getValidTuples(Iterator<TupleExpr> tupleList);
    
    
}
