package mvm.rya.cloudbase.giraph.format;

import cloudbase.core.data.Key;
import cloudbase.core.data.Value;
import com.google.common.collect.Maps;
import mvm.rya.api.RdfCloudTripleStoreConstants;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.resolver.RyaContext;
import mvm.rya.api.resolver.triple.TripleRow;
import org.apache.giraph.graph.BspUtils;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Map;

/**
 * Date: 7/27/12
 * Time: 1:39 PM
 */
public class CloudbaseRyaVertexInputFormat
        extends CloudbaseVertexInputFormat<Text, Text, Text, Text> {

    private Configuration conf;

    public VertexReader<Text, Text, Text, Text>
    createVertexReader(InputSplit split, TaskAttemptContext context)
            throws IOException {
        try {

            return new CloudbaseEdgeVertexReader(
                    cloudbaseInputFormat.createRecordReader(split, context)) {
            };
        } catch (InterruptedException e) {
            throw new IOException(e);
        }

    }

    /*
       Reader takes Key/Value pairs from the underlying input format.
    */
    public static class CloudbaseEdgeVertexReader
            extends CloudbaseVertexReader<Text, Text, Text, Text> {

        private RyaContext ryaContext = RyaContext.getInstance();

        public CloudbaseEdgeVertexReader(RecordReader<Key, Value> recordReader) {
            super(recordReader);
        }


        public boolean nextVertex() throws IOException, InterruptedException {
            return getRecordReader().nextKeyValue();
        }

        /*
       Each Key/Value contains the information needed to construct the vertices.
         */
        public Vertex<Text, Text, Text, Text> getCurrentVertex()
                throws IOException, InterruptedException {
            try {
                Key key = getRecordReader().getCurrentKey();
                Value value = getRecordReader().getCurrentValue();
                RyaStatement ryaStatement = ryaContext.deserializeTriple(RdfCloudTripleStoreConstants.TABLE_LAYOUT.SPO,
                        new TripleRow(key.getRow().getBytes(), key.getColumnFamily().getBytes(),
                                key.getColumnQualifier().getBytes()));//TODO: assume spo for now
                Vertex<Text, Text, Text, Text> vertex =
                        BspUtils.<Text, Text, Text, Text>createVertex(
                                getContext().getConfiguration());
                Text vertexId = new Text(ryaStatement.getSubject().getData()); //TODO: set Text?
                Map<Text, Text> edges = Maps.newHashMap();
                Text edgeId = new Text(ryaStatement.getPredicate().getData());
                edges.put(edgeId, new Text(ryaStatement.getObject().getData()));
                vertex.initialize(vertexId, new Text(), edges, null);

                return vertex;
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
    }
}
