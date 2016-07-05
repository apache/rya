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

package mvm.rya.prospector.mr

import mvm.rya.accumulo.AccumuloRdfConfiguration
import mvm.rya.api.RdfCloudTripleStoreConstants
import mvm.rya.api.domain.RyaStatement
import mvm.rya.api.resolver.RyaTripleContext
import mvm.rya.api.resolver.triple.TripleRow
import mvm.rya.prospector.plans.IndexWorkPlan
import mvm.rya.prospector.plans.IndexWorkPlanManager
import mvm.rya.prospector.plans.impl.ServicesBackedIndexWorkPlanManager

import org.apache.commons.lang.time.DateUtils
import org.apache.hadoop.mapreduce.Mapper

/**
 * Date: 12/3/12
 * Time: 11:06 AM
 */
class ProspectorMapper extends Mapper {

    private Date truncatedDate;
    private RyaTripleContext ryaContext;
    private IndexWorkPlanManager manager = new ServicesBackedIndexWorkPlanManager()
    private Collection<IndexWorkPlan> plans = manager.plans

    @Override
    public void setup(Mapper.Context context) throws IOException, InterruptedException {
        super.setup(context);

        long now = context.getConfiguration().getLong("DATE", System.currentTimeMillis());
		ryaContext = RyaTripleContext.getInstance(new AccumuloRdfConfiguration(context.getConfiguration()));
        truncatedDate = DateUtils.truncate(new Date(now), Calendar.MINUTE);
    }

    @Override
    public void map(def row, def data, Mapper.Context context) {
        RyaStatement ryaStatement = ryaContext.deserializeTriple(RdfCloudTripleStoreConstants.TABLE_LAYOUT.SPO,
                new TripleRow(
                        row.row.bytes,
                        row.columnFamily.bytes,
                        row.columnQualifier.bytes,
                        row.timestamp,
                        row.columnVisibility.bytes,
                        data.get()
                )
        )
        plans.each { plan ->
            def coll = plan.map(ryaStatement)
            if (coll != null) {
                coll.each { entry ->
                    context.write(entry.key, entry.value)
                }
            }
        }
    }
}
