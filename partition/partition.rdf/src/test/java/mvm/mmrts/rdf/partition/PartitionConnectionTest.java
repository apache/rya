package mvm.mmrts.rdf.partition;

import cloudbase.core.client.Connector;
import cloudbase.core.client.mock.MockInstance;
import junit.framework.TestCase;
import mvm.mmrts.rdf.partition.shard.DateHashModShardValueGenerator;
import org.openrdf.model.Namespace;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.*;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import java.util.List;

import static mvm.mmrts.rdf.partition.PartitionConstants.*;

/**
 * Class PartitionConnectionTest
 * Date: Jul 6, 2011
 * Time: 5:24:07 PM
 */
public class PartitionConnectionTest extends TestCase {
    public static final String NAMESPACE = "http://here/2010/tracked-data-provenance/ns#";//44 len
    public static final String RDF_NS = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
    public static final String HBNAMESPACE = "http://here/2010/tracked-data-provenance/heartbeat/ns#";
    public static final String HB_TIMESTAMP = HBNAMESPACE + "timestamp";

    private SailRepository repository;
    private SailRepositoryConnection connection;

    ValueFactory vf = ValueFactoryImpl.getInstance();

    private String objectUuid = "objectuuid1";
    private static final String TABLE = "rdfPartition";
    private static final String SHARD_TABLE = "rdfShardIndex";
    private String ancestor = "ancestor1";
    private String descendant = "descendant1";
    private static final long START = 1309532965000l;
    private static final long END = 1310566686000l;
    private Connector connector;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
//        connector = new ZooKeeperInstance("stratus", "stratus13:2181").getConnector("root", "password");
        connector = new MockInstance().getConnector("", "");

        PartitionSail sail = new PartitionSail(connector, TABLE, SHARD_TABLE, new DateHashModShardValueGenerator() {
            @Override
            public String generateShardValue(Object obj) {
                return this.generateShardValue(START + 1000, obj);
            }
        });

        repository = new SailRepository(sail);
        repository.initialize();
        connection = repository.getConnection();

        loadData();
    }

    private void loadData() throws RepositoryException, DatatypeConfigurationException {
        connection.add(new StatementImpl(vf.createURI(NAMESPACE, objectUuid), vf.createURI(NAMESPACE, "name"), vf.createLiteral("objUuid")));
        //created
        String uuid = "uuid1";
        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(RDF_NS, "type"), vf.createURI(NAMESPACE, "Created")));
        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "createdItem"), vf.createURI(NAMESPACE, objectUuid)));
        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "performedBy"), vf.createURI("urn:system:A")));
        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "stringLit"), vf.createLiteral("stringLit1")));
        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "stringLit"), vf.createLiteral("stringLit2")));
        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "stringLit"), vf.createLiteral("stringLit3")));
        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "stringLit"), vf.createLiteral("stringLit4")));
        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "strLit1"), vf.createLiteral("strLit1")));
        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "strLit1"), vf.createLiteral("strLit2")));
        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "strLit1"), vf.createLiteral("strLit3")));
        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "performedAt"), vf.createLiteral(DatatypeFactory.newInstance().newXMLGregorianCalendar(2011, 7, 12, 6, 0, 0, 0, 0))));
        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "reportedAt"), vf.createLiteral(DatatypeFactory.newInstance().newXMLGregorianCalendar(2011, 7, 12, 6, 1, 0, 0, 0))));
        //clicked
        uuid = "uuid2";
        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(RDF_NS, "type"), vf.createURI(NAMESPACE, "Clicked")));
        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "clickedItem"), vf.createURI(NAMESPACE, objectUuid)));
        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "performedBy"), vf.createURI("urn:system:B")));
        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "performedAt"), vf.createLiteral(DatatypeFactory.newInstance().newXMLGregorianCalendar(2011, 7, 12, 6, 2, 0, 0, 0))));
        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "reportedAt"), vf.createLiteral(DatatypeFactory.newInstance().newXMLGregorianCalendar(2011, 7, 12, 6, 3, 0, 0, 0))));
        //deleted
        uuid = "uuid3";
        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(RDF_NS, "type"), vf.createURI(NAMESPACE, "Deleted")));
        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "deletedItem"), vf.createURI(NAMESPACE, objectUuid)));
        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "performedBy"), vf.createURI("urn:system:C")));
        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "performedAt"), vf.createLiteral(DatatypeFactory.newInstance().newXMLGregorianCalendar(2011, 7, 12, 6, 4, 0, 0, 0))));
        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "reportedAt"), vf.createLiteral(DatatypeFactory.newInstance().newXMLGregorianCalendar(2011, 7, 12, 6, 5, 0, 0, 0))));
        //dropped
        uuid = "uuid4";
        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(RDF_NS, "type"), vf.createURI(NAMESPACE, "Dropped")));
        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "droppedItem"), vf.createURI(NAMESPACE, objectUuid)));
        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "performedBy"), vf.createURI("urn:system:D")));
        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "performedAt"), vf.createLiteral(DatatypeFactory.newInstance().newXMLGregorianCalendar(2011, 7, 12, 6, 6, 0, 0, 0))));
        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "reportedAt"), vf.createLiteral(DatatypeFactory.newInstance().newXMLGregorianCalendar(2011, 7, 12, 6, 7, 0, 0, 0))));
        //received
        uuid = "uuid5";
        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(RDF_NS, "type"), vf.createURI(NAMESPACE, "Received")));
        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "receivedItem"), vf.createURI(NAMESPACE, objectUuid)));
        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "performedBy"), vf.createURI("urn:system:E")));
        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "performedAt"), vf.createLiteral(DatatypeFactory.newInstance().newXMLGregorianCalendar(2011, 7, 12, 6, 8, 0, 0, 0))));
        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "reportedAt"), vf.createLiteral(DatatypeFactory.newInstance().newXMLGregorianCalendar(2011, 7, 12, 6, 9, 0, 0, 0))));
        //sent
        uuid = "uuid6";
        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(RDF_NS, "type"), vf.createURI(NAMESPACE, "Sent")));
        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "sentItem"), vf.createURI(NAMESPACE, objectUuid)));
        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "performedBy"), vf.createURI("urn:system:F")));
        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "performedAt"), vf.createLiteral(DatatypeFactory.newInstance().newXMLGregorianCalendar(2011, 7, 12, 6, 10, 0, 0, 0))));
        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "reportedAt"), vf.createLiteral(DatatypeFactory.newInstance().newXMLGregorianCalendar(2011, 7, 12, 6, 11, 0, 0, 0))));
        //stored
        uuid = "uuid7";
        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(RDF_NS, "type"), vf.createURI(NAMESPACE, "Stored")));
        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "storedItem"), vf.createURI(NAMESPACE, objectUuid)));
        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "performedBy"), vf.createURI("urn:system:G")));
        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "performedAt"), vf.createLiteral(DatatypeFactory.newInstance().newXMLGregorianCalendar(2011, 7, 12, 6, 12, 0, 0, 0))));
        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "reportedAt"), vf.createLiteral(DatatypeFactory.newInstance().newXMLGregorianCalendar(2011, 7, 12, 6, 13, 0, 0, 0))));

        //derivedFrom
        connection.add(new StatementImpl(vf.createURI(NAMESPACE, descendant), vf.createURI(NAMESPACE, "derivedFrom"), vf.createURI(NAMESPACE, ancestor)));
        connection.add(new StatementImpl(vf.createURI(NAMESPACE, descendant), vf.createURI(NAMESPACE, "name"), vf.createLiteral("descendantOne")));
        connection.add(new StatementImpl(vf.createURI(NAMESPACE, ancestor), vf.createURI(NAMESPACE, "name"), vf.createLiteral("ancestor1")));

        //heartbeats
        String hbuuid = "hbuuid1";
        connection.add(new StatementImpl(vf.createURI(HBNAMESPACE, hbuuid), vf.createURI(RDF_NS, "type"), vf.createURI(HBNAMESPACE, "HeartbeatMeasurement")));
        connection.add(new StatementImpl(vf.createURI(HBNAMESPACE, hbuuid), vf.createURI(HB_TIMESTAMP), vf.createLiteral((START + 1) + "")));
        connection.add(new StatementImpl(vf.createURI(HBNAMESPACE, hbuuid), vf.createURI(HBNAMESPACE, "count"), vf.createLiteral(1 + "")));
        connection.add(new StatementImpl(vf.createURI(HBNAMESPACE, hbuuid), vf.createURI(HBNAMESPACE, "systemName"), vf.createURI("urn:system:A")));
        connection.add(new StatementImpl(vf.createURI("urn:system:A"), vf.createURI(HBNAMESPACE, "heartbeat"), vf.createURI(HBNAMESPACE, hbuuid)));

        hbuuid = "hbuuid2";
        connection.add(new StatementImpl(vf.createURI(HBNAMESPACE, hbuuid), vf.createURI(RDF_NS, "type"), vf.createURI(HBNAMESPACE, "HeartbeatMeasurement")));
        connection.add(new StatementImpl(vf.createURI(HBNAMESPACE, hbuuid), vf.createURI(HB_TIMESTAMP), vf.createLiteral((START + 2) + "")));
        connection.add(new StatementImpl(vf.createURI(HBNAMESPACE, hbuuid), vf.createURI(HBNAMESPACE, "count"), vf.createLiteral(2 + "")));
        connection.add(new StatementImpl(vf.createURI(HBNAMESPACE, hbuuid), vf.createURI(HBNAMESPACE, "systemName"), vf.createURI("urn:system:B")));
        connection.add(new StatementImpl(vf.createURI("urn:system:B"), vf.createURI(HBNAMESPACE, "heartbeat"), vf.createURI(HBNAMESPACE, hbuuid)));

        hbuuid = "hbuuid3";
        connection.add(new StatementImpl(vf.createURI(HBNAMESPACE, hbuuid), vf.createURI(RDF_NS, "type"), vf.createURI(HBNAMESPACE, "HeartbeatMeasurement")));
        connection.add(new StatementImpl(vf.createURI(HBNAMESPACE, hbuuid), vf.createURI(HB_TIMESTAMP), vf.createLiteral((START + 3) + "")));
        connection.add(new StatementImpl(vf.createURI(HBNAMESPACE, hbuuid), vf.createURI(HBNAMESPACE, "count"), vf.createLiteral(3 + "")));
        connection.add(new StatementImpl(vf.createURI(HBNAMESPACE, hbuuid), vf.createURI(HBNAMESPACE, "systemName"), vf.createURI("urn:system:C")));
        connection.add(new StatementImpl(vf.createURI("urn:system:C"), vf.createURI(HBNAMESPACE, "heartbeat"), vf.createURI(HBNAMESPACE, hbuuid)));

        connection.add(new StatementImpl(vf.createURI("urn:subj1"), vf.createURI("urn:pred"), vf.createLiteral("obj1")));
        connection.add(new StatementImpl(vf.createURI("urn:subj1"), vf.createURI("urn:pred"), vf.createLiteral("obj2")));
        connection.add(new StatementImpl(vf.createURI("urn:subj1"), vf.createURI("urn:pred"), vf.createLiteral("obj3")));
        connection.add(new StatementImpl(vf.createURI("urn:subj1"), vf.createURI("urn:pred"), vf.createLiteral("obj4")));
        connection.add(new StatementImpl(vf.createURI("urn:subj2"), vf.createURI("urn:pred"), vf.createLiteral("obj1")));
        connection.add(new StatementImpl(vf.createURI("urn:subj2"), vf.createURI("urn:pred"), vf.createLiteral("obj2")));
        connection.add(new StatementImpl(vf.createURI("urn:subj2"), vf.createURI("urn:pred"), vf.createLiteral("obj3")));
        connection.add(new StatementImpl(vf.createURI("urn:subj2"), vf.createURI("urn:pred"), vf.createLiteral("obj4")));
        connection.add(new StatementImpl(vf.createURI("urn:subj3"), vf.createURI("urn:pred"), vf.createLiteral("obj1")));
        connection.add(new StatementImpl(vf.createURI("urn:subj3"), vf.createURI("urn:pred"), vf.createLiteral("obj4")));

        //MMRTS-150
        connection.add(new StatementImpl(vf.createURI("urn:mmrts150_1"), vf.createURI("urn:bool"), vf.createLiteral("true")));
        connection.add(new StatementImpl(vf.createURI("urn:mmrts150_2"), vf.createURI("urn:bool"), vf.createLiteral("true")));
        connection.add(new StatementImpl(vf.createURI("urn:mmrts150_3"), vf.createURI("urn:bool"), vf.createLiteral("true")));
        connection.add(new StatementImpl(vf.createURI("urn:mmrts150_4"), vf.createURI("urn:bool"), vf.createLiteral("true")));
        connection.add(new StatementImpl(vf.createURI("urn:mmrts150_4"), vf.createURI("urn:sentItem"), vf.createLiteral("thisItemNum")));
        connection.add(new StatementImpl(vf.createURI("urn:mmrts150_5"), vf.createURI("urn:bool"), vf.createLiteral("true")));
        connection.commit();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        connection.close();
        repository.shutDown();
    }

//    public void testScanAll() throws Exception {
//        Scanner sc = connector.createScanner(TABLE, ALL_AUTHORIZATIONS);
//        for (Object aSc : sc) System.out.println((Map.Entry<Key, Value>) aSc);
//
//    }

    public void testNamespace() throws Exception {
        String namespace = "urn:testNamespace#";
        String prefix = "pfx";
        connection.setNamespace(prefix, namespace);

        assertEquals(namespace, connection.getNamespace(prefix));
    }

    public void testGetNamespaces() throws Exception {
        String namespace = "urn:testNamespace#";
        String prefix = "pfx";
        connection.setNamespace(prefix, namespace);

        namespace = "urn:testNamespace2#";
        prefix = "pfx2";
        connection.setNamespace(prefix, namespace);

        RepositoryResult<Namespace> result = connection.getNamespaces();
        int count = 0;
        while (result.hasNext()) {
            result.next();
            count++;
        }

        assertEquals(count, 2);
    }

    public void testAddCommitStatement() throws Exception {
        StatementImpl stmt = new StatementImpl(vf.createURI("urn:namespace#subj"), vf.createURI("urn:namespace#pred"), vf.createLiteral("object"));
        connection.add(stmt);
        connection.commit();
    }

    public void testSelectOnlyQuery() throws Exception {
        String query = "PREFIX ns:<" + NAMESPACE + ">\n" +
                "select * where {\n" +
                "ns:uuid1 ns:createdItem ?cr.\n" +
                "ns:uuid1 ns:reportedAt ?ra.\n" +
                "ns:uuid1 ns:performedAt ?pa.\n" +
                "}\n";
        TupleQuery tupleQuery = connection.prepareTupleQuery(QueryLanguage.SPARQL, query);
//        tupleQuery.evaluate(new PrintTupleHandler());
        CountTupleHandler tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(1, tupleHandler.getCount());
    }

    //provenance Queries//////////////////////////////////////////////////////////////////////

    public void testEventInfo() throws Exception {
        String query = "PREFIX ns:<" + NAMESPACE + ">\n" +
                "select * where {\n" +
                "   ns:uuid1 ?p ?o.\n" +
                "}\n";

        TupleQuery tupleQuery = connection.prepareTupleQuery(QueryLanguage.SPARQL, query);
//                tupleQuery.evaluate(new PrintTupleHandler());
        CountTupleHandler tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(12, tupleHandler.getCount());
    }

    public void testAllAncestors() throws Exception {
        String query = "PREFIX ns:<" + NAMESPACE + ">\n" +
                "select * where {\n" +
                "ns:" + descendant + " ns:derivedFrom ?dr.\n" +
                "}\n";
        TupleQuery tupleQuery = connection.prepareTupleQuery(QueryLanguage.SPARQL, query);
        //        tupleQuery.evaluate(new PrintTupleHandler());
        CountTupleHandler tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(1, tupleHandler.getCount());
    }

    public void testAllDescendants() throws Exception {
        String query = "PREFIX ns:<" + NAMESPACE + ">\n" +
                "select * where {\n" +
                "?ds ns:derivedFrom ns:" + ancestor + ".\n" +
                "}\n";
        TupleQuery tupleQuery = connection.prepareTupleQuery(QueryLanguage.SPARQL, query);
//        tupleQuery.evaluate(new PrintTupleHandler());
        CountTupleHandler tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(1, tupleHandler.getCount());
    }

    public void testEventsForUri() throws Exception {
        String query = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "PREFIX ns:<" + NAMESPACE + ">\n" +
                "PREFIX mvmpart: <urn:mvm.mmrts.partition.rdf/08/2011#>\n" +
                "select * where {\n" +
                "{" +
                "   ?s rdf:type ns:Created.\n" +
                "   ?s ns:createdItem ns:objectuuid1.\n" +
                "   ?s ns:performedBy ?pb.\n" +
                "   FILTER(mvmpart:timeRange(?s, ns:performedAt, " + START + ", " + END + ", 'XMLDATETIME'))\n" +
                "}\n" +
                "UNION {" +
                "   ?s rdf:type ns:Clicked.\n" +
                "   ?s ns:clickedItem ns:objectuuid1.\n" +
                "   ?s ns:performedBy ?pb.\n" +
                "   FILTER(mvmpart:timeRange(?s, ns:performedAt, " + START + ", " + END + ", 'XMLDATETIME'))\n" +
                "}\n" +
                "UNION {" +
                "   ?s rdf:type ns:Deleted.\n" +
                "   ?s ns:deletedItem ns:objectuuid1.\n" +
                "   ?s ns:performedBy ?pb.\n" +
                "   FILTER(mvmpart:timeRange(?s, ns:performedAt, " + START + ", " + END + ", 'XMLDATETIME'))\n" +
                "}\n" +
                "UNION {" +
                "   ?s rdf:type ns:Dropped.\n" +
                "   ?s ns:droppedItem ns:objectuuid1.\n" +
                "   ?s ns:performedBy ?pb.\n" +
                "   FILTER(mvmpart:timeRange(?s, ns:performedAt, " + START + ", " + END + ", 'XMLDATETIME'))\n" +
                "}\n" +
                "UNION {" +
                "   ?s rdf:type ns:Received.\n" +
                "   ?s ns:receivedItem ns:objectuuid1.\n" +
                "   ?s ns:performedBy ?pb.\n" +
                "   FILTER(mvmpart:timeRange(?s, ns:performedAt, " + START + ", " + END + ", 'XMLDATETIME'))\n" +
                "}\n" +
                "UNION {" +
                "   ?s rdf:type ns:Stored.\n" +
                "   ?s ns:storedItem ns:objectuuid1.\n" +
                "   ?s ns:performedBy ?pb.\n" +
                "   FILTER(mvmpart:timeRange(?s, ns:performedAt, " + START + ", " + END + ", 'XMLDATETIME'))\n" +
                "}\n" +
                "UNION {" +
                "   ?s rdf:type ns:Sent.\n" +
                "   ?s ns:sentItem ns:objectuuid1.\n" +
                "   ?s ns:performedBy ?pb.\n" +
                "   FILTER(mvmpart:timeRange(?s, ns:performedAt, " + START + ", " + END + ", 'XMLDATETIME'))\n" +
                "}\n" +
                "}\n";
        TupleQuery tupleQuery = connection.prepareTupleQuery(QueryLanguage.SPARQL, query);
//        tupleQuery.setBinding(START_BINDING, vf.createLiteral(START));
//        tupleQuery.setBinding(END_BINDING, vf.createLiteral(END));
//        tupleQuery.setBinding(TIME_PREDICATE, vf.createURI(NAMESPACE, "performedAt"));
//                tupleQuery.evaluate(new PrintTupleHandler());
        CountTupleHandler tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(7, tupleHandler.getCount());
    }

    public void testAllEvents() throws Exception {
        String query = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "PREFIX ns:<" + NAMESPACE + ">\n" +
                "PREFIX mvmpart: <urn:mvm.mmrts.partition.rdf/08/2011#>\n" +
                "select * where {\n" +
                "{" +
                "   ?s rdf:type ns:Created.\n" +
                "   ?s ns:performedBy ?pb.\n" +
                "   FILTER(mvmpart:timeRange(?s, ns:performedAt, " + START + ", " + END + ", 'XMLDATETIME'))\n" +
                "}\n" +
                "UNION {" +
                "   ?s rdf:type ns:Clicked.\n" +
                "   ?s ns:performedBy ?pb.\n" +
                "   FILTER(mvmpart:timeRange(?s, ns:performedAt, " + START + ", " + END + ", 'XMLDATETIME'))\n" +
                "}\n" +
                "UNION {" +
                "   ?s rdf:type ns:Deleted.\n" +
                "   ?s ns:performedBy ?pb.\n" +
                "   FILTER(mvmpart:timeRange(?s, ns:performedAt, " + START + ", " + END + ", 'XMLDATETIME'))\n" +
                "}\n" +
                "UNION {" +
                "   ?s rdf:type ns:Dropped.\n" +
                "   ?s ns:performedBy ?pb.\n" +
                "   FILTER(mvmpart:timeRange(?s, ns:performedAt, " + START + ", " + END + ", 'XMLDATETIME'))\n" +
                "}\n" +
                "UNION {" +
                "   ?s rdf:type ns:Received.\n" +
                "   ?s ns:performedBy ?pb.\n" +
                "   FILTER(mvmpart:timeRange(?s, ns:performedAt, " + START + ", " + END + ", 'XMLDATETIME'))\n" +
                "}\n" +
                "UNION {" +
                "   ?s rdf:type ns:Stored.\n" +
                "   ?s ns:performedBy ?pb.\n" +
                "   FILTER(mvmpart:timeRange(?s, ns:performedAt, " + START + ", " + END + ", 'XMLDATETIME'))\n" +
                "}\n" +
                "UNION {" +
                "   ?s rdf:type ns:Sent.\n" +
                "   ?s ns:performedBy ?pb.\n" +
                "   FILTER(mvmpart:timeRange(?s, ns:performedAt, " + START + ", " + END + ", 'XMLDATETIME'))\n" +
                "}\n" +
                "}\n";
        TupleQuery tupleQuery = connection.prepareTupleQuery(QueryLanguage.SPARQL, query);
//        tupleQuery.setBinding(START_BINDING, vf.createLiteral(START));
//        tupleQuery.setBinding(END_BINDING, vf.createLiteral(END));
//        tupleQuery.setBinding(TIME_PREDICATE, vf.createURI(NAMESPACE, "performedAt"));
//                tupleQuery.evaluate(new PrintTupleHandler());
        CountTupleHandler tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(7, tupleHandler.getCount());
//        System.out.println(tupleHandler.getCount());
    }

    public void testEventsBtwnSystems() throws Exception {
        String query = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "PREFIX ns:<" + NAMESPACE + ">\n" +
                "PREFIX mvmpart: <urn:mvm.mmrts.partition.rdf/08/2011#>\n" +
                "select * where {\n" +
                "   ?sendEvent rdf:type ns:Sent;\n" +
                "              ns:sentItem ?objUuid;\n" +
                "              ns:performedBy <urn:system:F>.\n" +
                "   ?recEvent rdf:type ns:Received;\n" +
                "              ns:receivedItem ?objUuid;\n" +
                "              ns:performedBy <urn:system:E>.\n" +
                "   FILTER(mvmpart:timeRange(?sendEvent, ns:performedAt, " + START + ", " + END + ", 'XMLDATETIME'))\n" +
                "   FILTER(mvmpart:timeRange(?recEvent, ns:performedAt, " + START + ", " + END + ", 'XMLDATETIME'))\n" +
                "}\n";
        TupleQuery tupleQuery = connection.prepareTupleQuery(QueryLanguage.SPARQL, query);
//        tupleQuery.setBinding(START_BINDING, vf.createLiteral(START));
//        tupleQuery.setBinding(END_BINDING, vf.createLiteral(END));
//        tupleQuery.setBinding(TIME_PREDICATE, vf.createURI(NAMESPACE, "performedAt"));
//        tupleQuery.evaluate(new PrintTupleHandler());
        CountTupleHandler tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(1, tupleHandler.getCount());
    }

    public void testHeartbeatCounts() throws Exception {
        String query = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "PREFIX hns:<" + HBNAMESPACE + ">\n" +
                "PREFIX mvmpart: <urn:mvm.mmrts.partition.rdf/08/2011#>\n" +
                "select * where {\n" +
                "   ?hb rdf:type hns:HeartbeatMeasurement;\n" +
                "              hns:count ?count;\n" +
                "              hns:systemName ?systemName.\n" +
                "   FILTER(mvmpart:timeRange(?hb, hns:timestamp, " + START + ", " + (START + 2) + ", 'TIMESTAMP'))\n" +
                "}\n";
        System.out.println(query);
        TupleQuery tupleQuery = connection.prepareTupleQuery(QueryLanguage.SPARQL, query);
//        tupleQuery.setBinding(START_BINDING, vf.createLiteral(START));
//        tupleQuery.setBinding(END_BINDING, vf.createLiteral(START + 2));
//        tupleQuery.setBinding(TIME_PREDICATE, vf.createURI(HB_TIMESTAMP));
//        tupleQuery.setBinding(TIME_TYPE_PROP, vf.createLiteral(TimeType.TIMESTAMP.name()));
//                tupleQuery.evaluate(new PrintTupleHandler());
        CountTupleHandler tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(2, tupleHandler.getCount());
    }

    //provenance Queries//////////////////////////////////////////////////////////////////////

    public void testCreatedEvents() throws Exception {
        String query = "PREFIX ns:<" + NAMESPACE + ">\n" +
                "select * where {\n" +
                "   ?s ns:createdItem ns:objectuuid1.\n" +
                "   ?s ns:reportedAt ?ra.\n" +
                "}\n";
        TupleQuery tupleQuery = connection.prepareTupleQuery(QueryLanguage.SPARQL, query);
        tupleQuery.setBinding(START_BINDING, vf.createLiteral(START));
        tupleQuery.setBinding(END_BINDING, vf.createLiteral(END));
        tupleQuery.setBinding(TIME_PREDICATE, vf.createURI(NAMESPACE, "performedAt"));
//                tupleQuery.evaluate(new PrintTupleHandler());
        CountTupleHandler tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(1, tupleHandler.getCount());
    }

    public void testSelectAllAfterFilter() throws Exception {
        String query = "PREFIX ns:<" + NAMESPACE + ">\n" +
                "select * where {\n" +
                "   ?s ns:createdItem ns:objectuuid1.\n" +
                "   ?s ?p ?o.\n" +
                "}\n";
        TupleQuery tupleQuery = connection.prepareTupleQuery(QueryLanguage.SPARQL, query);
        tupleQuery.setBinding(START_BINDING, vf.createLiteral(START));
        tupleQuery.setBinding(END_BINDING, vf.createLiteral(END));
        tupleQuery.setBinding(TIME_PREDICATE, vf.createURI(NAMESPACE, "performedAt"));
//        tupleQuery.evaluate(new PrintTupleHandler());
        CountTupleHandler tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(12, tupleHandler.getCount());
    }

    public void testFilterQuery() throws Exception {
        String query = "PREFIX ns:<" + NAMESPACE + ">\n" +
                "select * where {\n" +
                "ns:uuid1 ns:createdItem ?cr.\n" +
                "ns:uuid1 ns:stringLit ?sl.\n" +
                "FILTER regex(?sl, \"stringLit1\")" +
                "ns:uuid1 ns:reportedAt ?ra.\n" +
                "ns:uuid1 ns:performedAt ?pa.\n" +
                "}\n";
        TupleQuery tupleQuery = connection.prepareTupleQuery(QueryLanguage.SPARQL, query);
        //        tupleQuery.evaluate(new PrintTupleHandler());
        CountTupleHandler tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(1, tupleHandler.getCount());
    }

    public void testMultiplePredicatesMultipleBindingSets() throws Exception {
        //MMRTS-121
        String query = "PREFIX ns:<" + NAMESPACE + ">\n" +
                "select * where {\n" +
                "?id ns:createdItem ns:objectuuid1.\n" +
                "?id ns:stringLit ?sl.\n" +
                "?id ns:strLit1 ?s2.\n" +
                "}\n";
        TupleQuery tupleQuery = connection.prepareTupleQuery(QueryLanguage.SPARQL, query);
//        tupleQuery.evaluate(new PrintTupleHandler());
        CountTupleHandler tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(12, tupleHandler.getCount());
    }

    public void testMultiShardLookupTimeRange() throws Exception {
        //MMRTS-113
        String query = "PREFIX hb: <http://here/2010/tracked-data-provenance/heartbeat/ns#>\n" +
                "PREFIX mvmpart: <urn:mvm.mmrts.partition.rdf/08/2011#>\n" +
                "SELECT * WHERE\n" +
                "{\n" +
                "?id hb:timestamp ?timestamp.\n" +
                "FILTER(mvmpart:timeRange(?id, hb:timestamp, " + START + " , " + (START + 2) + " , 'TIMESTAMP'))\n" +
                "?id hb:count ?count.\n" +
                "?system hb:heartbeat ?id.\n" +
                "}";
        TupleQuery tupleQuery = connection.prepareTupleQuery(QueryLanguage.SPARQL, query);
//        tupleQuery.evaluate(new PrintTupleHandler());
        CountTupleHandler tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(2, tupleHandler.getCount());
    }

    public void testMultiShardLookupTimeRangeValueConst() throws Exception {
        //MMRTS-113
        String query = "PREFIX hb: <http://here/2010/tracked-data-provenance/heartbeat/ns#>\n" +
                "PREFIX mvmpart: <urn:mvm.mmrts.partition.rdf/08/2011#>\n" +
                "SELECT * WHERE\n" +
                "{\n" +
                "<http://here/2010/tracked-data-provenance/heartbeat/ns#hbuuid2> hb:timestamp ?timestamp.\n" +
                "FILTER(mvmpart:timeRange(<http://here/2010/tracked-data-provenance/heartbeat/ns#hbuuid2>, hb:timestamp, " + START + " , " + END + " , 'TIMESTAMP'))\n" +
                "<http://here/2010/tracked-data-provenance/heartbeat/ns#hbuuid2> hb:count ?count.\n" +
                "?system hb:heartbeat <http://here/2010/tracked-data-provenance/heartbeat/ns#hbuuid2>.\n" +
                "}";
        TupleQuery tupleQuery = connection.prepareTupleQuery(QueryLanguage.SPARQL, query);
//        tupleQuery.evaluate(new PrintTupleHandler());
        CountTupleHandler tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(1, tupleHandler.getCount());
    }

    public void testGlobalTimeRange() throws Exception {
        //MMRTS-113
        String query = "PREFIX hb: <http://here/2010/tracked-data-provenance/heartbeat/ns#>\n" +
                "PREFIX mvmpart: <urn:mvm.mmrts.partition.rdf/08/2011#>\n" +
                "SELECT * WHERE\n" +
                "{\n" +
                "<http://here/2010/tracked-data-provenance/heartbeat/ns#hbuuid2> hb:timestamp ?timestamp.\n" +
                "<http://here/2010/tracked-data-provenance/heartbeat/ns#hbuuid2> hb:count ?count.\n" +
                "?system hb:heartbeat <http://here/2010/tracked-data-provenance/heartbeat/ns#hbuuid2>.\n" +
                "}";
        TupleQuery tupleQuery = connection.prepareTupleQuery(QueryLanguage.SPARQL, query);
        tupleQuery.setBinding(START_BINDING, vf.createLiteral(START));
        tupleQuery.setBinding(END_BINDING, vf.createLiteral(END));
        tupleQuery.setBinding(TIME_PREDICATE, vf.createURI(HBNAMESPACE, HB_TIMESTAMP));
        tupleQuery.setBinding(TIME_TYPE_PROP, vf.createLiteral(TimeType.TIMESTAMP.name()));
//        tupleQuery.evaluate(new PrintTupleHandler());
        CountTupleHandler tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(0, tupleHandler.getCount()); //because system does not have a timerange
    }

    public void testLinkQuery() throws Exception {
        String query = "PREFIX ns:<" + NAMESPACE + ">\n" +
                "SELECT * WHERE {\n" +
                "     <http://here/2010/tracked-data-provenance/ns#uuid1> ns:createdItem ?o .\n" +
                "     ?o ns:name ?n .\n" +
                "}";
        TupleQuery tupleQuery = connection.prepareTupleQuery(QueryLanguage.SPARQL, query);
//        tupleQuery.evaluate(new PrintTupleHandler());
        CountTupleHandler tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(1, tupleHandler.getCount());
    }

    public void testRangeOverDuplicateItems() throws Exception {
        String query = "PREFIX ns:<" + NAMESPACE + ">\n" +
                "SELECT * WHERE {\n" +
                "     ?subj <urn:pred> \"obj2\" .\n" +
                "}";
        TupleQuery tupleQuery = connection.prepareTupleQuery(QueryLanguage.SPARQL, query);
//        tupleQuery.evaluate(new PrintTupleHandler());
        CountTupleHandler tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(2, tupleHandler.getCount());
    }

    public void testMMRTS147SubjectOverMultipleShards() throws Exception {
        PartitionSail sail = new PartitionSail(connector, TABLE, SHARD_TABLE);

        SailRepository tmpRepo = new SailRepository(sail);
        tmpRepo.initialize();
        SailRepositoryConnection tmpConn = tmpRepo.getConnection();
        String uuid = "mmrts147subj";
        //add for the current date shard
        tmpConn.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "currentPred1"), vf.createLiteral("currentValue1")));

        //add for the old date shard
        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "currentPred2"), vf.createLiteral("currentValue2")));

        tmpConn.commit();
        connection.commit();

        String query = "PREFIX ns:<" + NAMESPACE + ">\n" +
                "SELECT * WHERE {\n" +
                "     ns:mmrts147subj ?p ?o .\n" +
                "}";
        TupleQuery tupleQuery = connection.prepareTupleQuery(QueryLanguage.SPARQL, query);
        tupleQuery.evaluate(new PrintTupleHandler());
//        CountTupleHandler tupleHandler = new CountTupleHandler();
//        tupleQuery.evaluate(tupleHandler);
//        assertEquals(2, tupleHandler.getCount());
    }

    public void testMMRTS147PredicatesOverMultipleShards() throws Exception {
        PartitionSail sail = new PartitionSail(connector, TABLE, SHARD_TABLE);

        SailRepository tmpRepo = new SailRepository(sail);
        tmpRepo.initialize();
        SailRepositoryConnection tmpConn = tmpRepo.getConnection();
        String uuid = "mmrts147pred";
        //add for the current date shard
        tmpConn.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "currentPred1"), vf.createLiteral("currentValue1")));

        //add for the old date shard
        connection.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "currentPred2"), vf.createLiteral("currentValue2")));

        tmpConn.commit();
        connection.commit();

        String query = "PREFIX ns:<" + NAMESPACE + ">\n" +
                "SELECT * WHERE {\n" +
                "     ?s ns:currentPred1 'currentValue1' .\n" +
                "     ?s ns:currentPred2 'currentValue2' .\n" +
                "}";
        TupleQuery tupleQuery = connection.prepareTupleQuery(QueryLanguage.SPARQL, query);
//        tupleQuery.evaluate(new PrintTupleHandler());
        CountTupleHandler tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(0, tupleHandler.getCount());
        //MMRTS-147, this is a problem. The same subject has been separated across shards and
        // cannot be queried with this join
    }

    /**
     * Make sure that the shard does not participate in the time range given
     *
     * @throws Exception
     */
    public void testMMRTS151ShardDoesNotMatchTimeRange() throws Exception {
        PartitionSail sail = new PartitionSail(connector, TABLE);

        SailRepository tmpRepo = new SailRepository(sail);
        tmpRepo.initialize();
        SailRepositoryConnection tmpConn = tmpRepo.getConnection();
        String uuid = "mmrts151";

        tmpConn.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(RDF_NS, "type"), vf.createURI(NAMESPACE, "Clicked")));
        tmpConn.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "clickedItem"), vf.createURI(NAMESPACE, objectUuid)));
        tmpConn.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "performedBy"), vf.createURI("urn:system:B")));
        tmpConn.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "perfAt"), vf.createLiteral(DatatypeFactory.newInstance().newXMLGregorianCalendar(2011, 7, 12, 6, 2, 0, 0, 0))));
        tmpConn.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "repAt"), vf.createLiteral(DatatypeFactory.newInstance().newXMLGregorianCalendar(2011, 7, 12, 6, 3, 0, 0, 0))));

        tmpConn.commit();

        String query = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "PREFIX ns:<" + NAMESPACE + ">\n" +
                "PREFIX mvmpart: <urn:mvm.mmrts.partition.rdf/08/2011#>\n" +
                "select * where {\n" +
                "   ?s ns:performedBy ?pb.\n" +
                "   FILTER(mvmpart:timeRange(?s, ns:perfAt, " + START + ", " + END + ", 'XMLDATETIME'))\n" +
                "}\n";
        TupleQuery tupleQuery = tmpConn.prepareTupleQuery(QueryLanguage.SPARQL, query);
//        tupleQuery.evaluate(new PrintTupleHandler());
        CountTupleHandler tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(1, tupleHandler.getCount());

        //now add a query with the shardRange function to make sure nothing comes back
        query = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "PREFIX ns:<" + NAMESPACE + ">\n" +
                "PREFIX mvmpart: <urn:mvm.mmrts.partition.rdf/08/2011#>\n" +
                "select * where {\n" +
                "   ?s ns:performedBy ?pb.\n" +
                "   FILTER(mvmpart:timeRange(?s, ns:perfAt, " + START + ", " + END + ", 'XMLDATETIME'))\n" +
                "   FILTER(mvmpart:shardRange(?s, " + START + ", " + END + "))\n" +
                "}\n";
        tupleQuery = tmpConn.prepareTupleQuery(QueryLanguage.SPARQL, query);
//        tupleQuery.evaluate(new PrintTupleHandler());
        tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(0, tupleHandler.getCount());

        //now make sure the shard range is for the curr shard
        long curr = System.currentTimeMillis();
        query = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "PREFIX ns:<" + NAMESPACE + ">\n" +
                "PREFIX mvmpart: <urn:mvm.mmrts.partition.rdf/08/2011#>\n" +
                "select * where {\n" +
                "   ?s ns:performedBy ?pb.\n" +
                "   FILTER(mvmpart:timeRange(?s, ns:perfAt, " + START + ", " + END + ", 'XMLDATETIME'))\n" +
                "   FILTER(mvmpart:shardRange(?s, " + (curr - (10000l)/***/) + ", " + curr + "))\n" +
                "}\n";
//        System.out.println(query);
        tupleQuery = tmpConn.prepareTupleQuery(QueryLanguage.SPARQL, query);
//        tupleQuery.evaluate(new PrintTupleHandler());
        tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(1, tupleHandler.getCount());
    }
    
    public void testMMRTS150() throws Exception {
        //MMRTS-150
        String query = "PREFIX ns:<" + NAMESPACE + ">\n" +
                "select * where {\n" +
                "?id <urn:bool> \"true\".\n" +
                "?id <urn:sentItem> ?item.\n" +
                "}\n";
        TupleQuery tupleQuery = connection.prepareTupleQuery(QueryLanguage.SPARQL, query);
//        tupleQuery.evaluate(new PrintTupleHandler());
        CountTupleHandler tupleHandler = new CountTupleHandler();
        tupleQuery.evaluate(tupleHandler);
        assertEquals(1, tupleHandler.getCount());
    }

    public void testMikeQuery() throws Exception {
        String query = "PREFIX mvmpart: <urn:mvm.mmrts.partition.rdf/08/2011#>\n" +
                "PREFIX tdp: <" + NAMESPACE + ">\n" +
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "SELECT * WHERE{\n" +
                "?id tdp:performedAt ?timestamp.\n" +
                "FILTER(mvmpart:timeRange(?id, tdp:performedAt, 132370075535, 1324060075534, 'XMLDATETIME')).\n" +
                "?id tdp:performedBy ?sysname.\n" +
//                "?id tdp:performedBy 'thesystemname'.\n" +
                "?id rdf:type ?et\n" +
//                "?id rdf:type tdp:EventType\n" +
                "}\n" +
                "Limit 10";
        TupleQuery tupleQuery = connection.prepareTupleQuery(QueryLanguage.SPARQL, query);
        tupleQuery.evaluate(new PrintTupleHandler());
    }

    private static class PrintTupleHandler implements TupleQueryResultHandler {

        @Override
        public void startQueryResult(List<String> strings) throws TupleQueryResultHandlerException {
        }

        @Override
        public void endQueryResult() throws TupleQueryResultHandlerException {

        }

        @Override
        public void handleSolution(BindingSet bindingSet) throws TupleQueryResultHandlerException {
            System.out.println(bindingSet);
        }
    }

    private static class CountTupleHandler implements TupleQueryResultHandler {

        int count = 0;

        @Override
        public void startQueryResult(List<String> strings) throws TupleQueryResultHandlerException {
        }

        @Override
        public void endQueryResult() throws TupleQueryResultHandlerException {
        }

        @Override
        public void handleSolution(BindingSet bindingSet) throws TupleQueryResultHandlerException {
            count++;
        }

        public int getCount() {
            return count;
        }
    }

}
