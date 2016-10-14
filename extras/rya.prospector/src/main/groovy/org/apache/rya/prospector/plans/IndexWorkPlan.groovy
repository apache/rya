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

package org.apache.rya.prospector.plans

import org.apache.rya.api.domain.RyaStatement
import org.apache.rya.prospector.domain.IntermediateProspect
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.Reducer
import org.openrdf.model.vocabulary.XMLSchema
import org.apache.rya.prospector.domain.IndexEntry

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
