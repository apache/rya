package mvm.rya.prospector.plans

import mvm.rya.api.domain.RyaStatement
import mvm.rya.prospector.domain.IntermediateProspect
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.Reducer
import org.openrdf.model.vocabulary.XMLSchema
import mvm.rya.prospector.domain.IndexEntry

/**
 * Date: 12/3/12
 * Time: 11:12 AM
 */
public interface IndexWorkPlan {

    public static final String URITYPE = XMLSchema.ANYURI.stringValue()
    public static final LongWritable ONE = new LongWritable(1)
    public static final String DELIM = "\u0000";

    public Collection<Map.Entry<IntermediateProspect, LongWritable>> map(RyaStatement ryaStatement)

    public Collection<Map.Entry<IntermediateProspect, LongWritable>> combine(IntermediateProspect prospect, Iterable<LongWritable> counts);

    public void reduce(IntermediateProspect prospect, Iterable<LongWritable> counts, Date timestamp, Reducer.Context context)

    public String getIndexType()

	public String getCompositeValue(List<String> indices)
	
    public List<IndexEntry> query(def connector, String tableName, List<Long> prospectTimes, String type, String index, String dataType, String[] auths)

}