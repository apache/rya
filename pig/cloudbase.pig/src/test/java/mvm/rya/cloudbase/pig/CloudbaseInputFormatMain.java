//package mvm.mmrts.cloudbase.pig;
//
//import cloudbase.core.CBConstants;
//import cloudbase.core.client.mapreduce.CloudbaseInputFormat;
//import cloudbase.core.data.Range;
//import mvm.mmrts.api.RdfCloudTripleStoreConstants;
//import mvm.rya.cloudbase.query.DefineTripleQueryRangeFactory;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.mapreduce.InputSplit;
//import org.apache.hadoop.mapreduce.JobContext;
//import org.openrdf.model.ValueFactory;
//import org.openrdf.model.impl.ValueFactoryImpl;
//
//import java.util.Collections;
//import java.util.List;
//import java.util.Map;
//
///**
// * Created by IntelliJ IDEA.
// * User: RoshanP
// * Date: 4/5/12
// * Time: 4:52 PM
// * To change this template use File | Settings | File Templates.
// */
//public class CloudbaseInputFormatMain {
//    public static void main(String[] args) {
//        try {
//            ValueFactory vf = new ValueFactoryImpl();
//            CloudbaseInputFormat format = new CloudbaseInputFormat();
//            Configuration configuration = new Configuration();
//            JobContext context = new JobContext(configuration, null);
//            CloudbaseInputFormat.setZooKeeperInstance(context, "stratus", "stratus13:2181");
//            CloudbaseInputFormat.setInputInfo(context, "root", "password".getBytes(), "l_po", CBConstants.NO_AUTHS);
//            DefineTripleQueryRangeFactory queryRangeFactory = new DefineTripleQueryRangeFactory();
//            Map.Entry<RdfCloudTripleStoreConstants.TABLE_LAYOUT,Range> entry =
//                    queryRangeFactory.defineRange(null, vf.createURI("urn:lubm:rdfts#takesCourse"), null, context.getConfiguration());
//            CloudbaseInputFormat.setRanges(context, Collections.singleton(entry.getValue()));
//            List<InputSplit> splits = format.getSplits(context);
//            for (InputSplit inputSplit : splits) {
//                String[] locations = inputSplit.getLocations();
//                for (String loc : locations) {
//                    java.net.InetAddress inetAdd = java.net.InetAddress.getByName(loc);
//                    System.out.println("Hostname is: " + inetAdd.getHostName());
//                }
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//}
