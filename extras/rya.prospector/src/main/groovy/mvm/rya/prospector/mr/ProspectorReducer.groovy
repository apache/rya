package mvm.rya.prospector.mr

import mvm.rya.prospector.plans.IndexWorkPlan
import mvm.rya.prospector.plans.IndexWorkPlanManager
import mvm.rya.prospector.plans.impl.ServicesBackedIndexWorkPlanManager
import org.apache.commons.lang.time.DateUtils
import org.apache.hadoop.mapreduce.Reducer
import mvm.rya.prospector.utils.ProspectorUtils

/**
 * Date: 12/3/12
 * Time: 11:06 AM
 */
class ProspectorReducer extends Reducer {

    private Date truncatedDate;
    private IndexWorkPlanManager manager = new ServicesBackedIndexWorkPlanManager()
    Map<String, IndexWorkPlan> plans

    @Override
    public void setup(Reducer.Context context) throws IOException, InterruptedException {
        super.setup(context);

        def conf = context.getConfiguration()
        long now = conf.getLong("DATE", System.currentTimeMillis());
        truncatedDate = DateUtils.truncate(new Date(now), Calendar.MINUTE);

        this.plans = ProspectorUtils.planMap(manager.plans)
    }

    @Override
    protected void reduce(def prospect, Iterable values, Reducer.Context context) {
        def plan = plans.get(prospect.index)
        if (plan != null) {
            plan.reduce(prospect, values, truncatedDate, context)
        }
    }
}