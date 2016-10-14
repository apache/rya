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

package org.apache.rya.prospector.mr

import org.apache.rya.prospector.plans.IndexWorkPlan
import org.apache.rya.prospector.plans.IndexWorkPlanManager
import org.apache.rya.prospector.plans.impl.ServicesBackedIndexWorkPlanManager
import org.apache.commons.lang.time.DateUtils
import org.apache.hadoop.mapreduce.Reducer
import org.apache.rya.prospector.utils.ProspectorUtils

/**
 * Date: 12/3/12
 * Time: 11:06 AM
 */
class ProspectorCombiner extends Reducer {

    private Date truncatedDate;
    private IndexWorkPlanManager manager = new ServicesBackedIndexWorkPlanManager()
    Map<String, IndexWorkPlan> plans

    @Override
    public void setup(Reducer.Context context) throws IOException, InterruptedException {
        super.setup(context);

        long now = context.getConfiguration().getLong("DATE", System.currentTimeMillis());
        truncatedDate = DateUtils.truncate(new Date(now), Calendar.MINUTE);

        this.plans = ProspectorUtils.planMap(manager.plans)
    }

    @Override
    protected void reduce(def prospect, Iterable values, Reducer.Context context) {
        def plan = plans.get(prospect.index)
        if (plan != null) {
            def coll = plan.combine(prospect, values)
            if (coll != null) {
                coll.each { entry ->
                    context.write(entry.key, entry.value)
                }
            }
        }
    }
}
