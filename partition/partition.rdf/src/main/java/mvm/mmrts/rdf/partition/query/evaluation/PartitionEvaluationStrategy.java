package mvm.mmrts.rdf.partition.query.evaluation;

import cloudbase.core.client.Connector;
import info.aduna.iteration.CloseableIteration;
import mvm.mmrts.rdf.partition.PartitionTripleSource;
import mvm.mmrts.rdf.partition.query.operators.ShardSubjectLookup;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.QueryRoot;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.evaluation.TripleSource;
import org.openrdf.query.algebra.evaluation.impl.EvaluationStrategyImpl;

import java.util.Map;

/**
 * Class PartitionEvaluationStrategy
 * Date: Jul 14, 2011
 * Time: 4:10:03 PM
 */
public class PartitionEvaluationStrategy extends EvaluationStrategyImpl {

    public PartitionEvaluationStrategy(PartitionTripleSource tripleSource, Dataset dataset) {
        super(tripleSource, dataset);
    }

    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(TupleExpr expr, BindingSet bindings) throws QueryEvaluationException {
        if (expr instanceof QueryRoot) {
            System.out.println(expr);
        } else if (expr instanceof ShardSubjectLookup) {
            return this.evaluate((ShardSubjectLookup) expr, bindings);
        }
        return super.evaluate(expr, bindings);
    }

    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(ShardSubjectLookup lookup, BindingSet bindings) throws QueryEvaluationException {
        if (bindings.size() > 0) {
            Var subjVar = lookup.getSubject();
            if(bindings.hasBinding(subjVar.getName())){
                subjVar.setValue(bindings.getValue(subjVar.getName()));
            }
            //populate the lookup
            for (Map.Entry<Var, Var> predObj : lookup.getPredicateObjectPairs()) {
                Var predVar = predObj.getKey();
                Var objVar = predObj.getValue();

                if(bindings.hasBinding(predVar.getName())) {
                    predVar.setValue(bindings.getValue(predVar.getName()));
                }
                if(bindings.hasBinding(objVar.getName())) {
                    objVar.setValue(bindings.getValue(objVar.getName()));
                }
            }
        }
        return ((PartitionTripleSource) tripleSource).getStatements(lookup, bindings, new Resource[0]);
    }

    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(StatementPattern sp, BindingSet bindings) throws QueryEvaluationException {
        ShardSubjectLookup lookup = new ShardSubjectLookup(sp.getSubjectVar());
        lookup.addPredicateObjectPair(sp.getPredicateVar(), sp.getObjectVar());
        return this.evaluate((ShardSubjectLookup) lookup, bindings);
    }
}
