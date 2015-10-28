//package mvm.mmrts.rdf.partition;
//
//import cloudbase.core.CBConstants;
//import cloudbase.core.client.Connector;
//import cloudbase.core.client.ZooKeeperInstance;
//import cloudbase.core.client.mock.MockInstance;
//import cloudbase.core.data.Key;
//import cloudbase.core.data.Range;
//import cloudbase.core.data.Value;
//import cloudbase.core.util.TextUtil;
//import mvm.mmrts.cloudbase.utils.client.DocumentBatchScanner;
//import mvm.mmrts.cloudbase.utils.client.DocumentConnectorImpl;
//import mvm.mmrts.rdf.partition.shard.DateHashModShardValueGenerator;
//import org.apache.hadoop.io.Text;
//import org.apache.log4j.Level;
//import org.apache.log4j.Logger;
//import org.openrdf.model.ValueFactory;
//import org.openrdf.model.impl.StatementImpl;
//import org.openrdf.model.impl.ValueFactoryImpl;
//import org.openrdf.repository.RepositoryException;
//import org.openrdf.repository.sail.SailRepository;
//import org.openrdf.repository.sail.SailRepositoryConnection;
//import ss.cloudbase.core.iterators.SortedRangeIterator;
//
//import javax.xml.datatype.DatatypeConfigurationException;
//import javax.xml.datatype.DatatypeFactory;
//import java.util.Collections;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Map;
//
//import static mvm.mmrts.rdf.partition.PartitionConstants.*;
//
///**
// * Class TstDocumentReader
// * Date: Sep 8, 2011
// * Time: 9:11:27 AM
// */
//public class TstDocumentReader {
//    public static final String NAMESPACE = "http://here/2010/tracked-data-provenance/ns#";//44 len
//    public static final String RDF_NS = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
//    public static final String HBNAMESPACE = "http://here/2010/tracked-data-provenance/heartbeat/ns#";
//    public static final String HB_TIMESTAMP = HBNAMESPACE + "timestamp";
//
//    private static SailRepository repository;
//    private static SailRepositoryConnection connection;
//
//    private static ValueFactory vf = ValueFactoryImpl.getInstance();
//
//    private static final String TABLE = "partitionRdf";
//    private static final long START = 1309532965000l;
//    private static final long END = 1310566686000l;
//    private static String objectUuid = "objectuuid1";
//
//    public static void main(String[] args) {
//        try {
////            Logger.getRootLogger().setLevel(Level.TRACE);
//            DocumentConnectorImpl connector = new DocumentConnectorImpl(new ZooKeeperInstance("stratus", "stratus13:2181"), "root", "password".getBytes());
////            DocumentConnectorImpl connector = new DocumentConnectorImpl(new MockInstance(), "", "".getBytes());
//
////            PartitionSail sail = new PartitionSail(connector, TABLE, new DateHashModShardValueGenerator() {
////                @Override
////                public String generateShardValue(Object obj) {
////                    return this.generateShardValue(START + 1000, obj);
////                }
////            });
////
////            repository = new SailRepository(sail);
////            repository.initialize();
////            connection = repository.getConnection();
////
////            loadData();
//
//            DocumentBatchScanner bs = connector.createDocumentBatchScanner(TABLE, CBConstants.NO_AUTHS, 2);
//
//            bs.setScanIterators(20, SortedRangeIterator.class.getName(), "ri");
//            bs.setScanIteratorOption("ri", SortedRangeIterator.OPTION_DOC_COLF, DOC.toString());
//            bs.setScanIteratorOption("ri", SortedRangeIterator.OPTION_COLF, INDEX.toString());
//            bs.setScanIteratorOption("ri", SortedRangeIterator.OPTION_START_INCLUSIVE, "" + true);
//            bs.setScanIteratorOption("ri", SortedRangeIterator.OPTION_END_INCLUSIVE, "" + true);
//            bs.setScanIteratorOption("ri", SortedRangeIterator.OPTION_MULTI_DOC, "" + true);
//
//            bs.setScanIteratorOption("ri", SortedRangeIterator.OPTION_LOWER_BOUND,
//                    "\07http://here/2010/tracked-data-provenance/ns#reportedAt\u0001\u000B2011-08-26T18:01:51Z"
//            );
//            bs.setScanIteratorOption("ri", SortedRangeIterator.OPTION_UPPER_BOUND,
//                    "\07http://here/2010/tracked-data-provenance/ns#reportedAt\u0001\u000B2011-08-26T18:02:00Z"
//            );
//
//            Range range = new Range(
//                    new Key("2011-08\0"),
//                    new Key("2011-08\uFFFD")
//            );
//
//            bs.setRanges(Collections.singleton(range));
//
//            int count = 0;
//            int innerCount = 0;
//            Iterator<List<? extends Map.Entry<Key, Value>>> iter = bs.documentIterator();
//            while (iter.hasNext()) {
//                count++;
//                List<? extends Map.Entry<Key, Value>> entries = iter.next();
//                for (Map.Entry<Key, Value> entry : entries) {
//                    System.out.print(entry.getKey().getColumnQualifier());
//                    System.out.println(" ");
//                    innerCount++;
//                }
//                System.out.println();
//            }
//            System.out.println(count);
//            System.out.println(innerCount);
//
//            bs.close();
////            connection.close();
////            repository.shutDown();
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//    private static void loadData() throws RepositoryException, DatatypeConfigurationException {
//        connection.add(new StatementImpl(vf.createURI(NAMESPACE, objectUuid), vf.createURI(NAMESPACE, "name"), vf.createLiteral("objUuid")));
//        //created
//        String uuid = "uuid1";
//        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(RDF_NS, "type"), vf.createURI(NAMESPACE, "Created")));
//        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "createdItem"), vf.createURI(NAMESPACE, objectUuid)));
//        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "performedBy"), vf.createURI("urn:system:A")));
//        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "stringLit"), vf.createLiteral("stringLit1")));
//        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "stringLit"), vf.createLiteral("stringLit2")));
//        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "stringLit"), vf.createLiteral("stringLit3")));
//        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "stringLit"), vf.createLiteral("stringLit4")));
//        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "strLit1"), vf.createLiteral("strLit1")));
//        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "strLit1"), vf.createLiteral("strLit2")));
//        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "strLit1"), vf.createLiteral("strLit3")));
//        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "performedAt"), vf.createLiteral(DatatypeFactory.newInstance().newXMLGregorianCalendar(2011, 7, 12, 6, 0, 0, 0, 0))));
//        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "reportedAt"), vf.createLiteral(DatatypeFactory.newInstance().newXMLGregorianCalendar(2011, 7, 12, 6, 1, 0, 0, 0))));
//        //clicked
//        uuid = "uuid2";
//        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(RDF_NS, "type"), vf.createURI(NAMESPACE, "Clicked")));
//        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "clickedItem"), vf.createURI(NAMESPACE, objectUuid)));
//        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "performedBy"), vf.createURI("urn:system:B")));
//        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "performedAt"), vf.createLiteral(DatatypeFactory.newInstance().newXMLGregorianCalendar(2011, 7, 12, 6, 2, 0, 0, 0))));
//        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "reportedAt"), vf.createLiteral(DatatypeFactory.newInstance().newXMLGregorianCalendar(2011, 7, 12, 6, 3, 0, 0, 0))));
//        //deleted
//        uuid = "uuid3";
//        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(RDF_NS, "type"), vf.createURI(NAMESPACE, "Deleted")));
//        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "deletedItem"), vf.createURI(NAMESPACE, objectUuid)));
//        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "performedBy"), vf.createURI("urn:system:C")));
//        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "performedAt"), vf.createLiteral(DatatypeFactory.newInstance().newXMLGregorianCalendar(2011, 7, 12, 6, 4, 0, 0, 0))));
//        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "reportedAt"), vf.createLiteral(DatatypeFactory.newInstance().newXMLGregorianCalendar(2011, 7, 12, 6, 5, 0, 0, 0))));
//        //dropped
//        uuid = "uuid4";
//        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(RDF_NS, "type"), vf.createURI(NAMESPACE, "Dropped")));
//        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "droppedItem"), vf.createURI(NAMESPACE, objectUuid)));
//        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "performedBy"), vf.createURI("urn:system:D")));
//        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "performedAt"), vf.createLiteral(DatatypeFactory.newInstance().newXMLGregorianCalendar(2011, 7, 12, 6, 6, 0, 0, 0))));
//        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "reportedAt"), vf.createLiteral(DatatypeFactory.newInstance().newXMLGregorianCalendar(2011, 7, 12, 6, 7, 0, 0, 0))));
//        //received
//        uuid = "uuid5";
//        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(RDF_NS, "type"), vf.createURI(NAMESPACE, "Received")));
//        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "receivedItem"), vf.createURI(NAMESPACE, objectUuid)));
//        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "performedBy"), vf.createURI("urn:system:E")));
//        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "performedAt"), vf.createLiteral(DatatypeFactory.newInstance().newXMLGregorianCalendar(2011, 7, 12, 6, 8, 0, 0, 0))));
//        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "reportedAt"), vf.createLiteral(DatatypeFactory.newInstance().newXMLGregorianCalendar(2011, 7, 12, 6, 9, 0, 0, 0))));
//        //sent
//        uuid = "uuid6";
//        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(RDF_NS, "type"), vf.createURI(NAMESPACE, "Sent")));
//        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "sentItem"), vf.createURI(NAMESPACE, objectUuid)));
//        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "performedBy"), vf.createURI("urn:system:F")));
//        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "performedAt"), vf.createLiteral(DatatypeFactory.newInstance().newXMLGregorianCalendar(2011, 7, 12, 6, 10, 0, 0, 0))));
//        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "reportedAt"), vf.createLiteral(DatatypeFactory.newInstance().newXMLGregorianCalendar(2011, 7, 12, 6, 11, 0, 0, 0))));
//        //stored
//        uuid = "uuid7";
//        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(RDF_NS, "type"), vf.createURI(NAMESPACE, "Stored")));
//        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "storedItem"), vf.createURI(NAMESPACE, objectUuid)));
//        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "performedBy"), vf.createURI("urn:system:G")));
//        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "performedAt"), vf.createLiteral(DatatypeFactory.newInstance().newXMLGregorianCalendar(2011, 7, 12, 6, 12, 0, 0, 0))));
//        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "reportedAt"), vf.createLiteral(DatatypeFactory.newInstance().newXMLGregorianCalendar(2011, 7, 12, 6, 13, 0, 0, 0))));
//
//        //heartbeats
//        String hbuuid = "hbuuid1";
//        connection.add(new StatementImpl(vf.createURI(HBNAMESPACE, hbuuid), vf.createURI(RDF_NS, "type"), vf.createURI(HBNAMESPACE, "HeartbeatMeasurement")));
//        connection.add(new StatementImpl(vf.createURI(HBNAMESPACE, hbuuid), vf.createURI(HB_TIMESTAMP), vf.createLiteral((START + 1) + "")));
//        connection.add(new StatementImpl(vf.createURI(HBNAMESPACE, hbuuid), vf.createURI(HBNAMESPACE, "count"), vf.createLiteral(1 + "")));
//        connection.add(new StatementImpl(vf.createURI(HBNAMESPACE, hbuuid), vf.createURI(HBNAMESPACE, "systemName"), vf.createURI("urn:system:A")));
//        connection.add(new StatementImpl(vf.createURI("urn:system:A"), vf.createURI(HBNAMESPACE, "heartbeat"), vf.createURI(HBNAMESPACE, hbuuid)));
//
//        hbuuid = "hbuuid2";
//        connection.add(new StatementImpl(vf.createURI(HBNAMESPACE, hbuuid), vf.createURI(RDF_NS, "type"), vf.createURI(HBNAMESPACE, "HeartbeatMeasurement")));
//        connection.add(new StatementImpl(vf.createURI(HBNAMESPACE, hbuuid), vf.createURI(HB_TIMESTAMP), vf.createLiteral((START + 2) + "")));
//        connection.add(new StatementImpl(vf.createURI(HBNAMESPACE, hbuuid), vf.createURI(HBNAMESPACE, "count"), vf.createLiteral(2 + "")));
//        connection.add(new StatementImpl(vf.createURI(HBNAMESPACE, hbuuid), vf.createURI(HBNAMESPACE, "systemName"), vf.createURI("urn:system:B")));
//        connection.add(new StatementImpl(vf.createURI("urn:system:B"), vf.createURI(HBNAMESPACE, "heartbeat"), vf.createURI(HBNAMESPACE, hbuuid)));
//
//        hbuuid = "hbuuid3";
//        connection.add(new StatementImpl(vf.createURI(HBNAMESPACE, hbuuid), vf.createURI(RDF_NS, "type"), vf.createURI(HBNAMESPACE, "HeartbeatMeasurement")));
//        connection.add(new StatementImpl(vf.createURI(HBNAMESPACE, hbuuid), vf.createURI(HB_TIMESTAMP), vf.createLiteral((START + 3) + "")));
//        connection.add(new StatementImpl(vf.createURI(HBNAMESPACE, hbuuid), vf.createURI(HBNAMESPACE, "count"), vf.createLiteral(3 + "")));
//        connection.add(new StatementImpl(vf.createURI(HBNAMESPACE, hbuuid), vf.createURI(HBNAMESPACE, "systemName"), vf.createURI("urn:system:C")));
//        connection.add(new StatementImpl(vf.createURI("urn:system:C"), vf.createURI(HBNAMESPACE, "heartbeat"), vf.createURI(HBNAMESPACE, hbuuid)));
//
//        connection.add(new StatementImpl(vf.createURI("urn:subj1"), vf.createURI("urn:pred"), vf.createLiteral("obj1")));
//        connection.add(new StatementImpl(vf.createURI("urn:subj1"), vf.createURI("urn:pred"), vf.createLiteral("obj2")));
//        connection.add(new StatementImpl(vf.createURI("urn:subj1"), vf.createURI("urn:pred"), vf.createLiteral("obj3")));
//        connection.add(new StatementImpl(vf.createURI("urn:subj1"), vf.createURI("urn:pred"), vf.createLiteral("obj4")));
//        connection.add(new StatementImpl(vf.createURI("urn:subj2"), vf.createURI("urn:pred"), vf.createLiteral("obj1")));
//        connection.add(new StatementImpl(vf.createURI("urn:subj2"), vf.createURI("urn:pred"), vf.createLiteral("obj2")));
//        connection.add(new StatementImpl(vf.createURI("urn:subj2"), vf.createURI("urn:pred"), vf.createLiteral("obj3")));
//        connection.add(new StatementImpl(vf.createURI("urn:subj2"), vf.createURI("urn:pred"), vf.createLiteral("obj4")));
//        connection.add(new StatementImpl(vf.createURI("urn:subj3"), vf.createURI("urn:pred"), vf.createLiteral("obj1")));
//        connection.add(new StatementImpl(vf.createURI("urn:subj3"), vf.createURI("urn:pred"), vf.createLiteral("obj4")));
//        connection.commit();
//    }
//
//
//}
