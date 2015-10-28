package mvm.mmrts.rdf.partition.query.evaluation;

import mvm.mmrts.rdf.partition.query.operators.ShardSubjectLookup;
import mvm.mmrts.rdf.partition.utils.CountPredObjPairs;
import org.apache.hadoop.conf.Configuration;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.evaluation.QueryOptimizer;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static mvm.mmrts.rdf.partition.PartitionConstants.*;

/**
 * Date: Jul 14, 2011
 * Time: 4:14:16 PM
 */
public class SubjectGroupingOptimizer implements QueryOptimizer {

    private static final Comparator<Var> VAR_COMPARATOR = new VarComparator();
    private static final Comparator<StatementPattern> SP_SUBJ_COMPARATOR = new SubjectComparator();
    private static final Comparator<TupleExpr> STATS_SHARD_COMPARATOR = new ShardLookupComparator();
    private static final CountPredObjPairs STATISTICS = new CountPredObjPairs();
    private Configuration conf;

    public SubjectGroupingOptimizer(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindingSet) {
        tupleExpr.visit(new FlattenJoinVisitor());
    }

    protected class FlattenJoinVisitor extends QueryModelVisitorBase<RuntimeException> {
        @Override
        public void meet(Join node) throws RuntimeException {
            List<StatementPattern> flatten = getJoinArgs(node, new ArrayList<StatementPattern>());
            //order by subject
            Collections.sort(flatten, SP_SUBJ_COMPARATOR);

            List<TupleExpr> shardLookups = new ArrayList<TupleExpr>();
            Var current = null;
            ShardSubjectLookup shardLookupCurrent = null;
            for (StatementPattern sp : flatten) {
                if (!sp.getSubjectVar().hasValue() && !sp.getPredicateVar().hasValue()) {
                    // if there is nothing set in the subject or predicate, we treat it as a single item
                    // might be ?s ?p ?o
                    shardLookups.add(sp);
                } else {
                    Var subjectVar = sp.getSubjectVar();
                    if (VAR_COMPARATOR.compare(current, subjectVar) != 0) {
                        current = subjectVar;
                        shardLookupCurrent = new ShardSubjectLookup(current);
                        populateLookup(shardLookupCurrent);
                        shardLookups.add(shardLookupCurrent);
                    }
                    shardLookupCurrent.addPredicateObjectPair(sp.getPredicateVar(), sp.getObjectVar());
                }
            }

            int i = 0;
            Collections.sort(shardLookups, STATS_SHARD_COMPARATOR);
            TupleExpr replacement = shardLookups.get(i);
            for (i++; i < shardLookups.size(); i++) {
                replacement = new Join(replacement, shardLookups.get(i));
            }

            node.replaceWith(replacement);
        }

        @Override
        public void meet(StatementPattern node) throws RuntimeException {
            ShardSubjectLookup lookup = new ShardSubjectLookup(node.getSubjectVar());
            lookup.addPredicateObjectPair(node.getPredicateVar(), node.getObjectVar());
            populateLookup(lookup);
            node.replaceWith(lookup);
        }
    }

    protected <L extends List<StatementPattern>> L getJoinArgs(TupleExpr tupleExpr, L joinArgs) {
        if (tupleExpr instanceof Join) {
            Join join = (Join) tupleExpr;
            getJoinArgs(join.getLeftArg(), joinArgs);
            getJoinArgs(join.getRightArg(), joinArgs);
        } else if (tupleExpr instanceof StatementPattern) {
            joinArgs.add((StatementPattern) tupleExpr);
        }

        return joinArgs;
    }

    protected ShardSubjectLookup populateLookup(ShardSubjectLookup lookup) {
        String timePredicate = conf.get(lookup.getSubject().getName() + "." + TIME_PREDICATE);
        if (timePredicate != null) {
            lookup.setTimePredicate(timePredicate);
            lookup.setStartTimeRange(conf.get(lookup.getSubject().getName() + "." + START_BINDING));
            lookup.setEndTimeRange(conf.get(lookup.getSubject().getName() + "." + END_BINDING));
            lookup.setTimeType(TimeType.valueOf(conf.get(lookup.getSubject().getName() + "." + TIME_TYPE_PROP, TimeType.XMLDATETIME.name())));
        }

        String shardRange = conf.get(lookup.getSubject().getName() + "." + SHARDRANGE_BINDING);
        if(shardRange != null) {
            lookup.setShardStartTimeRange(conf.get(lookup.getSubject().getName() + "." + SHARDRANGE_START));
            lookup.setShardEndTimeRange(conf.get(lookup.getSubject().getName() + "." + SHARDRANGE_END));
        }

        return lookup;
    }

    protected static class SubjectComparator implements Comparator<StatementPattern> {

        @Override
        public int compare(StatementPattern a, StatementPattern b) {
            if (a == b)
                return 0;

            if (a == null || b == null)
                return 1;

            if (a.getSubjectVar().equals(b.getSubjectVar())) {
                if (a.getPredicateVar().hasValue() && b.getPredicateVar().hasValue())
                    return 0;
                if (a.getPredicateVar().hasValue() && !b.getPredicateVar().hasValue())
                    return -1;
                if (!a.getPredicateVar().hasValue() && b.getPredicateVar().hasValue())
                    return 1;
                return 0;
            }

            if (a.getSubjectVar().getValue() != null && b.getSubjectVar().getValue() != null &&
                    a.getSubjectVar().getValue().equals(b.getSubjectVar().getValue()))
                return 0;

            return 1;
        }
    }

    protected static class ShardLookupComparator implements Comparator<TupleExpr> {

        @Override
        public int compare(TupleExpr a, TupleExpr b) {
            double a_c = STATISTICS.getCount(a);
            double b_c = STATISTICS.getCount(b);
            double diff = a_c - b_c;
            return (int) (diff / Math.abs(diff));
        }
    }

    protected static class VarComparator implements Comparator<Var> {

        @Override
        public int compare(Var a, Var b) {
            if (a == b)
                return 0;
            if (a == null || b == null)
                return 1;

            if (a.equals(b))
                return 0;

            if (a.getValue() != null &&
                    b.getValue() != null &&
                    a.getValue().equals(b.getValue()))
                return 0;

            return 1;
        }
    }
}
