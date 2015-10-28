package mvm.rya.prospector.plans.impl

import mvm.rya.prospector.plans.IndexWorkPlan
import com.google.common.collect.Lists
import mvm.rya.prospector.plans.IndexWorkPlanManager

/**
 * Date: 12/3/12
 * Time: 11:24 AM
 */
class ServicesBackedIndexWorkPlanManager implements IndexWorkPlanManager {

    def Collection<IndexWorkPlan> plans

    ServicesBackedIndexWorkPlanManager() {
        def iterator = ServiceLoader.load(IndexWorkPlan.class).iterator();
        plans = Lists.newArrayList(iterator)
    }
}
