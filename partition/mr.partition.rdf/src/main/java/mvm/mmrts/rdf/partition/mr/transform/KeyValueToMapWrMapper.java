package mvm.mmrts.rdf.partition.mr.transform;

import cloudbase.core.data.Key;
import cloudbase.core.data.Value;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import mvm.mmrts.rdf.partition.PartitionConstants;
import mvm.mmrts.rdf.partition.utils.RdfIO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.openrdf.model.Statement;

import java.io.IOException;
import java.util.*;

import static mvm.mmrts.rdf.partition.mr.transform.SparqlCloudbaseIFTransformerConstants.*;

/**
 * Will take a triple and output: <subject, predObj map>
 * <p/>
 * Class KeyValueToMapWrMapper
 * Date: Sep 1, 2011
 * Time: 4:56:42 PM
 */
public class KeyValueToMapWrMapper extends Mapper<Key, Value, Text, MapWritable> {

//    private List<String> predicateFilter = new ArrayList<String>();

    private Text subjNameTxt;
    private Text keyout = new Text();
    private Text predout = new Text();
    private Text objout = new Text();

    private Map<String, String> predValueName = new HashMap();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        //find the values to filter on
        Configuration conf = context.getConfiguration();
        String[] filter = conf.getStrings(SELECT_FILTER);
        if (filter != null) {
            for (String predValue : filter) {
                String predName = conf.get(predValue);
                if (predName != null)
                    predValueName.put(predValue, predName);
            }
        }

        String subjName = conf.get(SUBJECT_NAME);
        if (subjName != null) {
            //not sure it will ever be null
            subjNameTxt = new Text(subjName);
        }
    }

    @Override
    protected void map(Key key, Value value, Context context) throws IOException, InterruptedException {
        Statement stmt = RdfIO.readStatement(ByteStreams.newDataInput(key.getColumnQualifier().getBytes()), PartitionConstants.VALUE_FACTORY);
        String predName = predValueName.get(stmt.getPredicate().stringValue());
        if (predName == null)
            return;

        keyout.set(stmt.getSubject().stringValue());
        predout.set(predName);
        objout.set(stmt.getObject().stringValue());
        MapWritable mw = new MapWritable();
        mw.put(predout, objout);
        if (subjNameTxt != null) {
            mw.put(subjNameTxt, keyout);
        }
        context.write(keyout, mw);
    }

}
