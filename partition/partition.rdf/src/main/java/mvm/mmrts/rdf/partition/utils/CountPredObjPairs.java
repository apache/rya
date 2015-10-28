package mvm.mmrts.rdf.partition.utils;

import mvm.mmrts.rdf.partition.query.operators.ShardSubjectLookup;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Var;

import java.util.List;
import java.util.Map;

/**
 * Class CountPredObjPairs
 * Date: Apr 12, 2011
 * Time: 1:31:05 PM
 */
public class CountPredObjPairs {

    public CountPredObjPairs() {
    }

    public double getCount(TupleExpr expr) {
        int count = 100;
        if (expr instanceof ShardSubjectLookup) {
            ShardSubjectLookup lookup = (ShardSubjectLookup) expr;
            List<Map.Entry<Var, Var>> entries = lookup.getPredicateObjectPairs();
            count -= (lookup.getSubject().hasValue()) ? 1 : 0;
            count -= (lookup.getTimePredicate() != null) ? 1 : 0;
            for (Map.Entry<Var, Var> entry : entries) {
                count -= (entry.getValue().hasValue() && entry.getKey().hasValue()) ? 1 : 0;
            }
        } else if (expr instanceof StatementPattern) {
            StatementPattern sp = (StatementPattern) expr;
            count -= (sp.getSubjectVar().hasValue()) ? 1 : 0;
            count -= (sp.getPredicateVar().hasValue() && sp.getObjectVar().hasValue()) ? 1 : 0;
        }
        return count;
    }

}
