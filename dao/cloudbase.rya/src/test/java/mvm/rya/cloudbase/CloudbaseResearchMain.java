//package mvm.mmrts.cloudbase;
//
//import cloudbase.core.CBConstants;
//import cloudbase.core.client.BatchScanner;
//import cloudbase.core.client.Connector;
//import cloudbase.core.client.Scanner;
//import cloudbase.core.client.ZooKeeperInstance;
//import cloudbase.core.client.impl.MasterClient;
//import cloudbase.core.data.Key;
//import cloudbase.core.data.Range;
//import cloudbase.core.data.Value;
//import cloudbase.core.master.thrift.MasterClientService;
//import cloudbase.core.master.thrift.MasterMonitorInfo;
//import cloudbase.core.master.thrift.TableInfo;
//import cloudbase.core.master.thrift.TabletServerStatus;
//import cloudbase.core.security.thrift.AuthInfo;
//import mvm.rya.cloudbase.utils.pri.PriorityIterator;
//import org.apache.hadoop.io.Text;
//
//import java.util.Collections;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Map;
//
///**
// * Created by IntelliJ IDEA.
// * User: RoshanP
// * Date: 3/28/12
// * Time: 5:32 PM
// * To change this template use File | Settings | File Templates.
// */
//public class CloudbaseResearchMain {
//
//
//    public static void main(String[] args) {
//        try {
//            ZooKeeperInstance instance = new ZooKeeperInstance("stratus", "stratus13:2181");
//
//            MasterClientService.Iface client = MasterClient.getConnection(instance, false);
//            MasterMonitorInfo mmi = client.getMasterStats(null, new AuthInfo("root", "password".getBytes(), "stratus"));
//
//            List<TabletServerStatus> tServerInfo = mmi.getTServerInfo();
//            for (TabletServerStatus tstatus : tServerInfo) {
//                System.out.println(tstatus.getName());
//                System.out.println(tstatus.getOsLoad());
//                Map<String, TableInfo> tableMap = tstatus.getTableMap();
//                double ingestRate = 0;
//                double queryRate = 0;
//                for (Map.Entry<String, TableInfo> entry : tableMap.entrySet()) {
//                    String tableName = entry.getKey();
//                    TableInfo tableInfo = entry.getValue();
//                    ingestRate += tableInfo.getIngestRate();
//                    queryRate += tableInfo.getQueryRate();
//                }
//                System.out.println(ingestRate);
//                System.out.println(queryRate);
//            }
//
//            Connector connector = instance.getConnector("root", "password".getBytes());
////            BatchScanner scanner = connector.createBatchScanner("l_spo", CBConstants.NO_AUTHS, 10);
////            scanner.setRanges(Collections.singleton(new Range(new Text("\0"), new Text("\uFFFD"))));
//            Scanner scanner = connector.createScanner("l_spo", CBConstants.NO_AUTHS);
//            scanner.setScanIterators(20, PriorityIterator.class.getName(), "pi");
//            Iterator<Map.Entry<Key,Value>> iter = scanner.iterator();
//            int count = 0;
//            while(iter.hasNext()) {
//                iter.next();
//                System.out.println(count++);
////                if(count == 100) break;
//            }
////            scanner.close();
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//}
