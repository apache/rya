package mvm.mmrts.rdf.partition.query.evaluation;

import org.apache.hadoop.conf.Configuration;
import org.openrdf.model.impl.BooleanLiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.*;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

import java.util.List;

import static mvm.mmrts.rdf.partition.PartitionConstants.*;

/**
 * Class FilterTimeIndexVisitor
 * Date: Apr 11, 2011
 * Time: 10:16:15 PM
 */
public class FilterTimeIndexVisitor extends QueryModelVisitorBase {

    private Configuration conf;

    public FilterTimeIndexVisitor(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public void meet(Filter node) throws Exception {
        super.meet(node);

        ValueExpr arg = node.getCondition();
        if (arg instanceof FunctionCall) {
            FunctionCall fc = (FunctionCall) arg;
            if (SHARDRANGE.stringValue().equals(fc.getURI())) {
                List<ValueExpr> valueExprs = fc.getArgs();
                if (valueExprs.size() != 3) {
                    throw new QueryEvaluationException("mvm:shardRange must have 3 parameters: subject to run time index on, startTime(ms), endTime(ms)");
                }
                ValueExpr subj = valueExprs.get(0);
                String subj_s = null;
                if (subj instanceof Var) {
                    subj_s = ((Var) subj).getName();
                } else if (subj instanceof ValueConstant) {
                    subj_s = ((ValueConstant) subj).getValue().stringValue();
                }
                if (subj_s == null)
                    return; //no changes, need to figure out what shard lookup to add this time predicate to

                String startTime = ((ValueConstant) valueExprs.get(1)).getValue().stringValue();
                String endTime = ((ValueConstant) valueExprs.get(2)).getValue().stringValue();

                this.conf.set(subj_s + "." + SHARDRANGE_BINDING, "true");
                this.conf.set(subj_s + "." + SHARDRANGE_START, startTime);
                this.conf.set(subj_s + "." + SHARDRANGE_END, endTime);

                node.setCondition(new ValueConstant(BooleanLiteralImpl.TRUE));
            }
            if (TIMERANGE.stringValue().equals(fc.getURI())) {
                List<ValueExpr> valueExprs = fc.getArgs();
                if (valueExprs.size() != 4 && valueExprs.size() != 5) {
                    throw new QueryEvaluationException("mvm:timeRange must have 4/5 parameters: subject to run time index on, time uri to index, startTime, endTime, time type(XMLDATETIME, TIMESTAMP)");
                }

                ValueExpr subj = valueExprs.get(0);
                String subj_s = null;
                if (subj instanceof Var) {
                    subj_s = ((Var) subj).getName();
                } else if (subj instanceof ValueConstant) {
                    subj_s = ((ValueConstant) subj).getValue().stringValue();
                }
                if (subj_s == null)
                    return; //no changes, need to figure out what shard lookup to add this time predicate to

                ValueConstant timeUri_s = (ValueConstant) valueExprs.get(1);
                URIImpl timeUri = new URIImpl(timeUri_s.getValue().stringValue());
                String startTime = ((ValueConstant) valueExprs.get(2)).getValue().stringValue();
                String endTime = ((ValueConstant) valueExprs.get(3)).getValue().stringValue();
                TimeType timeType = TimeType.XMLDATETIME;
                if (valueExprs.size() > 4)
                    timeType = TimeType.valueOf(((ValueConstant) valueExprs.get(4)).getValue().stringValue());


                this.conf.set(subj_s + "." + TIME_PREDICATE, timeUri.stringValue());
                this.conf.set(subj_s + "." + START_BINDING, startTime);
                this.conf.set(subj_s + "." + END_BINDING, endTime);
                this.conf.set(subj_s + "." + TIME_TYPE_PROP, timeType.name());

                //not setting global times
                //set global start-end times
//                String startTime_global = conf.get(START_BINDING);
//                String endTime_global = conf.get(END_BINDING);
//                if (startTime_global != null) {
//                    long startTime_l = Long.parseLong(startTime);
//                    long startTime_lg = Long.parseLong(startTime_global);
//                    if (startTime_l < startTime_lg)
//                        conf.set(START_BINDING, startTime);
//                } else
//                    conf.set(START_BINDING, startTime);
//
//                if (endTime_global != null) {
//                    long endTime_l = Long.parseLong(endTime);
//                    long endTime_lg = Long.parseLong(endTime_global);
//                    if (endTime_l > endTime_lg)
//                        conf.set(END_BINDING, endTime);
//                } else
//                    conf.set(END_BINDING, endTime);

                node.setCondition(new ValueConstant(BooleanLiteralImpl.TRUE));
            }
        }
    }

}
