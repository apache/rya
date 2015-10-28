package mvm.rya.generic.mr.api

import org.apache.hadoop.conf.Configuration

/**
 * Date: 12/5/12
 * Time: 1:32 PM
 */
class MRInfoContext {

    private static currentMrInfo;

    public static MRInfo currentMRInfo() {
        return currentMRInfo(null);
    }

    public static MRInfo currentMRInfo(Configuration config) {
        if (currentMrInfo == null) {
            def iter = ServiceLoader.load(MRInfo.class, Thread.currentThread().getContextClassLoader()).iterator()
            if (iter.hasNext()) {
                currentMrInfo = iter.next()
                if (config != null) currentMrInfo.setConf(config)
            }
        }
        return currentMrInfo
    }

}
