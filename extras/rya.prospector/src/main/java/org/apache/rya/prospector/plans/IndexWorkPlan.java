/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.prospector.plans;

import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.prospector.domain.IndexEntry;
import org.apache.rya.prospector.domain.IntermediateProspect;
import org.apache.rya.prospector.mr.ProspectorCombiner;
import org.apache.rya.prospector.mr.ProspectorMapper;
import org.openrdf.model.vocabulary.XMLSchema;

/**
 * Contains the methods that perform each of the Map Reduce functions that result
 * in the final {@link IndexEntry} values as well as a way to query those values
 * once they have been written.
 */
public interface IndexWorkPlan {

    public static final String URITYPE = XMLSchema.ANYURI.stringValue();
    public static final LongWritable ONE = new LongWritable(1);
    public static final String DELIM = "\u0000";

    /**
     * This method is invoked by {@link ProspectorMapper}. It's used to pull
     * input from an Accumulo Rya instance into the Map Reduce framework.
     * </p>
     * It must use the values of a {@link RyaStatement} to derive a bunch of
     * {@link IntermediateProspect} and {@code LongWritable} pairs. This is only
     * useful for prospecting jobs that count things. The {@link IntermediateProspect}
     * value will be used as the key within {@link #combine(IntermediateProspect, Iterable)} and
     * {@link #reduce(IntermediateProspect, Iterable, Date, org.apache.hadoop.mapreduce.Reducer.Context)}.
     *
     * @param ryaStatement - The RDF Statement that needs to be mapped.
     * @return A collection of intermediate keys and counts.
     */
    public Collection<Map.Entry<IntermediateProspect, LongWritable>> map(RyaStatement ryaStatement);

    /**
     * This method is invoked by {@link ProspectorCombiner}. It is used by to
     * combine the results of {@link ProspectorMapper} before the shuffle operation
     * of the Map Reduce framework.
     *
     * @param prospect - The intermediate prospect that is being combined.
     * @param counts - The counts that need to be combined together.
     * @return A collection containing the combined results.
     */
    public Collection<Map.Entry<IntermediateProspect, LongWritable>> combine(IntermediateProspect prospect, Iterable<LongWritable> counts);

    /**
     * This method is invoked by {@link ProsectorReducer}. It is used to reduce
     * the counts to their final states and write them to output via the
     * {@code context}.l
     *
     * @param prospect - The intermediate prospect that is being reduced.
     * @param counts - The counts that need to be reduced.
     * @param timestamp - The timestamp that identifies this Prospector run.
     * @param context - The reducer context the reduced values will be written to.
     * @throws IOException A problem was encountered while writing to the context.
     * @throws InterruptedException Writes to the context were interrupted.
     */
    public void reduce(IntermediateProspect prospect, Iterable<LongWritable> counts, Date timestamp, Reducer.Context context) throws IOException, InterruptedException;

    /**
     * @return A unique name that indicates which {@link IndexEntry}s came from this plan.
     */
    public String getIndexType();

    /**
     * TODO Not sure what this generically is for. It is used by the count job to
     *      place a null delimiter between any {@link IndexEntry}s whose data
     *      section is two difference pieces of information together.
     */
    public String getCompositeValue(List<String> indices);

    /**
     * Search for {@link IndexEntry}s that have values matching the provided parameters.
     *
     * @param connector - The Accumulo Connector used to find the table holding the data.
     * @param tableName - The name of the table the Prospector results are stored within.
     * @param prospectTimes - Indicates which Prospect runs will be part of the query.
     * @param type - The name of the index the {@link IndexEntry}s are stored within.
     * @param index - The data portion of the {@link IndexEntry}s that may be returned.
     * @param dataType - The data type of the {@link IndexEntry}s that may be returned.
     * @param auths - The authorizations used to search for the entries.
     * @return The {@link IndexEntries} that match the provided values.
     * @throws TableNotFoundException No table exists for {@code tableName}.
     */
    public List<IndexEntry> query(Connector connector, String tableName, List<Long> prospectTimes, String type, String index, String dataType, String[] auths) throws TableNotFoundException;
}