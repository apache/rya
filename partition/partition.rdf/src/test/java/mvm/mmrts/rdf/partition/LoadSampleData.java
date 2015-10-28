package mvm.mmrts.rdf.partition;

import cloudbase.core.client.ZooKeeperInstance;
import cloudbase.core.security.ColumnVisibility;
import mvm.mmrts.rdf.partition.converter.ContextColVisConverter;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.sail.SailRepository;

import javax.xml.datatype.DatatypeFactory;

public class LoadSampleData {

    public static final String NAMESPACE = "http://here/2010/tracked-data-provenance/ns#";//44 len
    public static final String RDF_NS = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";

    static ValueFactory vf = ValueFactoryImpl.getInstance();

    /**
     * @param args
     */
    public static void main(String[] args) {
        try {

            final PartitionSail store = new PartitionSail(new ZooKeeperInstance("stratus", "stratus13:2181").getConnector("root", "password".getBytes()), "partTest", "shardIndexTest");
            
            Repository myRepository = new SailRepository(store);
            myRepository.initialize();

            RepositoryConnection conn = myRepository.getConnection();

            String uuid = "uuidAuth1";
            conn.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(RDF_NS, "type"), vf.createURI(NAMESPACE, "Created")));
            conn.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "createdItem"), vf.createURI(NAMESPACE, "objectUuid1")));
            conn.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "performedBy"), vf.createURI("urn:system:A")));
            conn.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "stringLit"), vf.createLiteral("stringLit")));
            conn.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "performedAt"), vf.createLiteral(DatatypeFactory.newInstance().newXMLGregorianCalendar(2011, 7, 12, 6, 0, 0, 0, 0))));
            conn.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "reportedAt"), vf.createLiteral(DatatypeFactory.newInstance().newXMLGregorianCalendar(2011, 7, 12, 6, 1, 0, 0, 0))));
            conn.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "booleanLit"), vf.createLiteral(true)));

            uuid = "uuidAuth4";
            conn.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(RDF_NS, "type"), vf.createURI(NAMESPACE, "Created")));
            conn.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "createdItem"), vf.createURI(NAMESPACE, "objectUuid1")));
            conn.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "performedBy"), vf.createURI("urn:system:A")));
            conn.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "stringLit"), vf.createLiteral("stringLit")));
            conn.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "performedAt"), vf.createLiteral(DatatypeFactory.newInstance().newXMLGregorianCalendar(2011, 7, 12, 6, 0, 0, 0, 0))));
            conn.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "reportedAt"), vf.createLiteral(DatatypeFactory.newInstance().newXMLGregorianCalendar(2011, 7, 12, 6, 1, 0, 0, 0))));
            conn.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "booleanLit"), vf.createLiteral(true)));

            conn.commit();
            conn.close();

            myRepository.shutDown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
