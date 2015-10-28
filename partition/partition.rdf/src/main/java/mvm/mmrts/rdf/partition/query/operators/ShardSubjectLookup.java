package mvm.mmrts.rdf.partition.query.operators;

import mvm.mmrts.rdf.partition.PartitionConstants;
import org.openrdf.query.algebra.QueryModelNodeBase;
import org.openrdf.query.algebra.QueryModelVisitor;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Var;

import java.util.*;

/**
 * Here the subject is not null, but there will be a list of
 * predicate/object paired vars that may or may not be null
 * <p/>
 * Class ShardSubjectLookup
 * Date: Jul 14, 2011
 * Time: 3:32:33 PM
 */
public class ShardSubjectLookup extends QueryModelNodeBase implements TupleExpr {

    private Var subject;
    private List<Map.Entry<Var, Var>> predicateObjectPairs;

    private String timePredicate;
    private String startTimeRange;
    private String endTimeRange;
    private String shardStartTimeRange;
    private String shardEndTimeRange;
    private PartitionConstants.TimeType timeType;

    public ShardSubjectLookup(Var subject) {
        this(subject, new ArrayList<Map.Entry<Var, Var>>());
    }

    public ShardSubjectLookup(Var subject, List<Map.Entry<Var, Var>> predicateObjectPairs) {
        this.subject = subject.clone();
        this.predicateObjectPairs = new ArrayList<Map.Entry<Var, Var>>(predicateObjectPairs);
    }

    @Override
    public <X extends Exception> void visit(QueryModelVisitor<X> visitor) throws X {
        visitor.meetOther(this);
    }

    @Override
    public <X extends Exception> void visitChildren(QueryModelVisitor<X> visitor) throws X {
        visitor.meet(subject);
        for (Map.Entry<Var, Var> predObj : predicateObjectPairs) {
            visitor.meet(predObj.getKey());
            visitor.meet(predObj.getValue());
        }
    }

    @Override
    public Set<String> getBindingNames() {
        return getAssuredBindingNames();
    }

    @Override
    public Set<String> getAssuredBindingNames() {
        Set<String> bindingNames = new HashSet<String>(8);

        if (subject != null) {
            bindingNames.add(subject.getName());
        }
        for (Map.Entry<Var, Var> predObj : predicateObjectPairs) {
            bindingNames.add(predObj.getKey().getName());
            bindingNames.add(predObj.getValue().getName());
        }

        return bindingNames;
    }

    public void addPredicateObjectPair(Var predicate, Var object) {
        this.predicateObjectPairs.add(new HashMap.SimpleEntry<Var, Var>(predicate, object));
    }

    public Var getSubject() {
        return subject;
    }

    public void setSubject(Var subject) {
        this.subject = subject;
    }

    public List<Map.Entry<Var, Var>> getPredicateObjectPairs() {
        return predicateObjectPairs;
    }

    public void setPredicateObjectPairs(List<Map.Entry<Var, Var>> predicateObjectPairs) {
        this.predicateObjectPairs = predicateObjectPairs;
    }

    public String getEndTimeRange() {
        return endTimeRange;
    }

    public void setEndTimeRange(String endTimeRange) {
        this.endTimeRange = endTimeRange;
    }

    public String getStartTimeRange() {
        return startTimeRange;
    }

    public void setStartTimeRange(String startTimeRange) {
        this.startTimeRange = startTimeRange;
    }

    public String getTimePredicate() {
        return timePredicate;
    }

    public void setTimePredicate(String timePredicate) {
        this.timePredicate = timePredicate;
    }

    public PartitionConstants.TimeType getTimeType() {
        return timeType;
    }

    public void setTimeType(PartitionConstants.TimeType timeType) {
        this.timeType = timeType;
    }

    public String getShardStartTimeRange() {
        return shardStartTimeRange;
    }

    public void setShardStartTimeRange(String shardStartTimeRange) {
        this.shardStartTimeRange = shardStartTimeRange;
    }

    public String getShardEndTimeRange() {
        return shardEndTimeRange;
    }

    public void setShardEndTimeRange(String shardEndTimeRange) {
        this.shardEndTimeRange = shardEndTimeRange;
    }

    public ShardSubjectLookup clone() {
        return (ShardSubjectLookup) super.clone();
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ShardSubjectLookup && super.equals(other);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ "ShardSubjectLookup".hashCode();
    }

    @Override
    public String toString() {
        return "ShardSubjectLookup{" +
                "subject=" + subject +
                ", predicateObjectPairs=" + predicateObjectPairs +
                ", timePredicate='" + timePredicate + '\'' +
                ", startTimeRange='" + startTimeRange + '\'' +
                ", endTimeRange='" + endTimeRange + '\'' +
                ", timeType=" + timeType +
                '}';
    }
}
