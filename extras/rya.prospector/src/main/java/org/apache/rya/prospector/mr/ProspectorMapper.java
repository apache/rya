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
package org.apache.rya.prospector.mr;

import java.io.IOException;
import java.util.Collection;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.api.RdfCloudTripleStoreConstants;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.resolver.RyaTripleContext;
import org.apache.rya.api.resolver.triple.TripleRow;
import org.apache.rya.api.resolver.triple.TripleRowResolverException;
import org.apache.rya.prospector.domain.IntermediateProspect;
import org.apache.rya.prospector.plans.IndexWorkPlan;
import org.apache.rya.prospector.plans.IndexWorkPlanManager;
import org.apache.rya.prospector.plans.impl.ServicesBackedIndexWorkPlanManager;

/**
 * Loads {@link RyaStatement}s from Accumulo and maps them into {@link IntermediateProspect}s
 * paired with count information during the Map portion of the Hadoop Map Reduce framework.
 */
public class ProspectorMapper extends Mapper<Key, Value, IntermediateProspect, LongWritable> {

    private RyaTripleContext ryaContext;
    private final IndexWorkPlanManager manager = new ServicesBackedIndexWorkPlanManager();
    private final Collection<IndexWorkPlan> plans = manager.getPlans();

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        ryaContext = RyaTripleContext.getInstance(new AccumuloRdfConfiguration(context.getConfiguration()));
    }

    @Override
    public void map(Key row, Value data, Context context) throws IOException, InterruptedException {
        RyaStatement ryaStatement = null;
        try {
            ryaStatement = ryaContext.deserializeTriple(RdfCloudTripleStoreConstants.TABLE_LAYOUT.SPO,
                    new TripleRow(
                            row.getRow().getBytes(),
                            row.getColumnFamily().getBytes(),
                            row.getColumnQualifier().getBytes(),
                            row.getTimestamp(),
                            row.getColumnVisibility().getBytes(),
                            data.get()
                    )
            );
        } catch (final TripleRowResolverException e) {
            // Do nothing. The row didn't contain a Rya Statement.
        }

        if(ryaStatement != null) {
            for(final IndexWorkPlan plan : plans) {
                final Collection<Entry<IntermediateProspect, LongWritable>> coll = plan.map(ryaStatement);
                for(final Entry<IntermediateProspect, LongWritable> entry : coll) {
                    context.write(entry.getKey(), entry.getValue());
                }
            }
        }
    }
}