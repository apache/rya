package mvm.mmrts.rdf.partition;

import cloudbase.core.client.ZooKeeperInstance;
import cloudbase.core.security.ColumnVisibility;
import mvm.mmrts.rdf.partition.converter.ContextColVisConverter;
import mvm.mmrts.rdf.partition.shard.DateHashModShardValueGenerator;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.sail.SailRepository;

public class LoadPartitionData2 {

    public static final String NAMESPACE = "http://here/2010/tracked-data-provenance/ns#";//44 len
    public static final String RDF_NS = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";

    static ValueFactory vf = ValueFactoryImpl.getInstance();

    /**
     * @param args
     */
    public static void main(String[] args) {
        try {

            DateHashModShardValueGenerator gen = new DateHashModShardValueGenerator();
            gen.setBaseMod(10);
            final PartitionSail store = new PartitionSail(new ZooKeeperInstance("stratus", "stratus13:2181").getConnector("root", "password".getBytes()), "rdfPartition", gen);
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

            conn.add(new StatementImpl(vf.createURI("http://www.Department0.University0.edu/GraduateStudent44"), vf.createURI("urn:lubm:test#specific"), vf.createURI("urn:lubm:test#value")));

            conn.commit();
            conn.close();

            myRepository.shutDown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
