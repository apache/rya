/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package mvm.rya.cloudbase.giraph.format;

import cloudbase.core.data.Mutation;
import cloudbase.core.data.Value;
import mvm.rya.api.RdfCloudTripleStoreConstants;
import mvm.rya.api.domain.RyaType;
import mvm.rya.api.domain.RyaURI;
import mvm.rya.cloudbase.RyaTableMutationsFactory;
import org.apache.giraph.graph.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexWriter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/*
 Example subclass for writing vertices back to Cloudbase.
 */
public class CloudbaseRyaVertexOutputFormat
        extends CloudbaseVertexOutputFormat<Text, Text, Text> {

    public VertexWriter<Text, Text, Text>
    createVertexWriter(TaskAttemptContext context)
            throws IOException, InterruptedException {
        RecordWriter<Text, Mutation> writer =
                cloudbaseOutputFormat.getRecordWriter(context);
        String tableName = context.getConfiguration().get(OUTPUT_TABLE);
        if (tableName == null)
            throw new IOException("Forgot to set table name " +
                    "using CloudbaseVertexOutputFormat.OUTPUT_TABLE");
        return new CloudbaseEdgeVertexWriter(writer, tableName);
    }

    /*
    Wraps RecordWriter for writing Mutations back to the configured Cloudbase Table.
     */
    public static class CloudbaseEdgeVertexWriter
            extends CloudbaseVertexWriter<Text, Text, Text> {

        public static final RyaTableMutationsFactory RYA_TABLE_MUTATIONS_FACTORY = new RyaTableMutationsFactory();
        private final Text CF = new Text("cf");
        private final Text PARENT = new Text("parent");
        private Text tableName;

        public CloudbaseEdgeVertexWriter(
                RecordWriter<Text, Mutation> writer, String tableName) {
            super(writer);
            this.tableName = new Text(tableName);
        }

        /*
        Write back a mutation that adds a qualifier for 'parent' containing the vertex value
        as the cell value. Assume the vertex ID corresponds to a key.
        */
        public void writeVertex(Vertex<Text, Text, Text, ?> vertex)
                throws IOException, InterruptedException {
            RecordWriter<Text, Mutation> writer = getRecordWriter();
            Text subj = vertex.getId();
            Iterable<Edge<Text, Text>> edges = vertex.getEdges();
            for (Edge<Text, Text> edge : edges) {
                Text pred = edge.getTargetVertexId();
                Text obj = edge.getValue();
                Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize =
                        RYA_TABLE_MUTATIONS_FACTORY.serialize(new RyaURI(subj.toString()),
                                new RyaURI(pred.toString()), new RyaType(obj.toString()), null);
                Collection<Mutation> mutations = serialize.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.SPO);
                for (Mutation mut : mutations) {
                    writer.write(tableName, mut); //TODO: Assuming SPO
                }
            }
        }
    }
}
