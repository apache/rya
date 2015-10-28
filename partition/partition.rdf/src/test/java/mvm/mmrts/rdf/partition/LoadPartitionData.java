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

public class LoadPartitionData {

    public static final String NAMESPACE = "http://here/2010/tracked-data-provenance/ns#";//44 len
    public static final String RDF_NS = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";

    static ValueFactory vf = ValueFactoryImpl.getInstance();

    /**
     * @param args
     */
    public static void main(String[] args) {
        try {

            final PartitionSail store = new PartitionSail(new ZooKeeperInstance("stratus", "stratus13:2181").getConnector("root", "password".getBytes()), "rdfPartition");
            store.setContextColVisConverter(new ContextColVisConverter() {

                @Override
                public ColumnVisibility convertContexts(Resource... contexts) {
                    if (contexts != null) {
                        StringBuffer sb = new StringBuffer();
                        for (int i = 0; i < contexts.length; i++) {
                            Resource context = contexts[i];
                            if (context instanceof URI) {
                                URI uri = (URI) context;
                                sb.append(uri.getLocalName());
                                if (i != (contexts.length - 1)) {
                                    sb.append("|");
                                }
                            }
                        }
                        return new ColumnVisibility(sb.toString());
                    }
                    return null;
                }
            });
            Repository myRepository = new SailRepository(store);
            myRepository.initialize();

            RepositoryConnection conn = myRepository.getConnection();

            URI A = vf.createURI("urn:colvis#A");
            URI B = vf.createURI("urn:colvis#B");
            URI C = vf.createURI("urn:colvis#C");

            String uuid = "uuidAuth1";
            conn.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(RDF_NS, "type"), vf.createURI(NAMESPACE, "Created")), A, B, C);
            conn.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "createdItem"), vf.createURI(NAMESPACE, "objectUuid1")), A, B);
            conn.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "performedBy"), vf.createURI("urn:system:A")), A, B);
            conn.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "stringLit"), vf.createLiteral("stringLit")), A);
            conn.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "performedAt"), vf.createLiteral(DatatypeFactory.newInstance().newXMLGregorianCalendar(2011, 7, 12, 6, 0, 0, 0, 0))), B, C);
            conn.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "reportedAt"), vf.createLiteral(DatatypeFactory.newInstance().newXMLGregorianCalendar(2011, 7, 12, 6, 1, 0, 0, 0))), C);
            conn.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "booleanLit"), vf.createLiteral(true)));

            conn.commit();
            conn.close();

            myRepository.shutDown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
