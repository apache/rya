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
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.rya.prospector.domain.IntermediateProspect;
import org.apache.rya.prospector.plans.IndexWorkPlan;
import org.apache.rya.prospector.plans.IndexWorkPlanManager;
import org.apache.rya.prospector.plans.impl.ServicesBackedIndexWorkPlanManager;
import org.apache.rya.prospector.utils.ProspectorUtils;

/**
 * Reduces the {@link IntermediateProspect} counts into their final values and
 * writes them to their final storage location during the Reduce step of the
 * Hadoop Map Reduce framework.
 */
public class ProspectorReducer extends Reducer<IntermediateProspect, LongWritable, IntermediateProspect, LongWritable> {

    private Date truncatedDate;
    private final IndexWorkPlanManager manager = new ServicesBackedIndexWorkPlanManager();
    private Map<String, IndexWorkPlan> plans;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        final Configuration conf = context.getConfiguration();
        final long now = conf.getLong("DATE", System.currentTimeMillis());
        truncatedDate = DateUtils.truncate(new Date(now), Calendar.MINUTE);

        this.plans = ProspectorUtils.planMap(manager.getPlans());
    }

    @Override
    protected void reduce(IntermediateProspect prospect, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        final IndexWorkPlan plan = plans.get(prospect.getIndex());
        if (plan != null) {
            plan.reduce(prospect, values, truncatedDate, context);
        }
    }
}