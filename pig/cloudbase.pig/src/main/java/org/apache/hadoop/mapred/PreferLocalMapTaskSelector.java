//package org.apache.hadoop.mapred;
//
//import org.apache.hadoop.net.Node;
//
//import java.io.IOException;
//import java.util.Arrays;
//
///**
// */
//public class PreferLocalMapTaskSelector extends DefaultTaskSelector {
//
//    @Override
//    public Task obtainNewMapTask(TaskTrackerStatus taskTracker, JobInProgress job) throws IOException {
//        return this.obtainNewLocalMapTask(taskTracker, job);
//    }
//
//    public Task obtainNewLocalMapTask(TaskTrackerStatus taskTracker, JobInProgress job)
//            throws IOException {
//        ClusterStatus clusterStatus = taskTrackerManager.getClusterStatus();
//        int numTaskTrackers = clusterStatus.getTaskTrackers();
//        System.out.println(taskTracker.getHost());
//        for (TaskInProgress tip : job.maps) {
//            String[] splitLocations = tip.getSplitLocations();
//            System.out.println(Arrays.toString(splitLocations));
//            for (String loc : splitLocations) {
//                Node node = job.jobtracker.getNode(loc);
//                System.out.println(node);
//                if(!taskTracker.getHost().equals(loc)) {
//                    return null;
//                }
//            }
//        }
//
//        Node node = job.jobtracker.getNode(taskTracker.getHost());
//        System.out.println(node);
//        Task task = job.obtainNewLocalMapTask(taskTracker, numTaskTrackers, taskTrackerManager.getNumberOfUniqueHosts());
//        return task;
//    }
//}
