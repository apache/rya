package mvm.mmrts.rdf.partition.mr.transform;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;

/**
 * Since each subject is located at most on one tablet, we should be able to assume that
 * no reducer is needed.  The Combine phase should aggregate properly.
 * <p/>
 * Class AggregateTriplesBySubjectReducer
 * Date: Sep 1, 2011
 * Time: 5:39:24 PM
 */
public class AggregateTriplesBySubjectCombiner extends Reducer<Text, MapWritable, Text, MapWritable> {
//    private LongWritable lwout = new LongWritable();
    private MapWritable mwout = new MapWritable();

    @Override
    protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
        for (MapWritable value : values) {
            for (Map.Entry<Writable, Writable> entry : value.entrySet()) {
                mwout.put(WritableUtils.clone(entry.getKey(), context.getConfiguration()),
                        WritableUtils.clone(entry.getValue(), context.getConfiguration()));
            }
        }
        context.write(key, mwout);
    }
}
