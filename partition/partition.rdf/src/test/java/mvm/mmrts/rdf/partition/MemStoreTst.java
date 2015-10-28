package mvm.mmrts.rdf.partition;

import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResultHandler;
import org.openrdf.query.resultio.sparqlxml.SPARQLResultsXMLWriter;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.memory.MemoryStore;

import javax.xml.datatype.DatatypeFactory;

/**
 * Class MemStoreTst
 * Date: Aug 30, 2011
 * Time: 10:04:02 AM
 */
public class MemStoreTst {
    public static final String NAMESPACE = "http://here/2010/tracked-data-provenance/ns#";//44 len
    public static final String RDF_NS = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";

    static ValueFactory vf = ValueFactoryImpl.getInstance();

    public static void main(String[] args) {

        try {
            MemoryStore store = new MemoryStore();
            Repository myRepository = new SailRepository(store);
            myRepository.initialize();

            RepositoryConnection conn = myRepository.getConnection();

            String uuid = "uuid1";
            conn.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(RDF_NS, "type"), vf.createURI(NAMESPACE, "Created")));
            conn.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "createdItem"), vf.createURI(NAMESPACE, "objectUuid1")));
            conn.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "performedBy"), vf.createURI("urn:system:A")));
            conn.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "stringLit"), vf.createLiteral("stringLit1")));
            conn.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "stringLit"), vf.createLiteral("stringLit2")));
            conn.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "performedAt"), vf.createLiteral(DatatypeFactory.newInstance().newXMLGregorianCalendar(2011, 7, 12, 6, 0, 0, 0, 0))));
            conn.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "reportedAt"), vf.createLiteral(DatatypeFactory.newInstance().newXMLGregorianCalendar(2011, 7, 12, 6, 1, 0, 0, 0))));
            conn.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "booleanLit"), vf.createLiteral(true)));
            conn.add(new StatementImpl(vf.createURI(NAMESPACE, uuid), vf.createURI(NAMESPACE, "booleanLit"), vf.createLiteral(false)));

            conn.commit();

            //query
            String query = "PREFIX tdp:<" + NAMESPACE + ">\n" +
                    "SELECT * WHERE {\n" +
                    "   ?id tdp:createdItem tdp:objectUuid1.\n" +
                    "   ?id tdp:stringLit ?str.\n" +
                    "   ?id tdp:booleanLit ?bl.\n" +
                    "}";

            TupleQuery tupleQuery = conn.prepareTupleQuery(
                    QueryLanguage.SPARQL, query);
            TupleQueryResultHandler writer = new SPARQLResultsXMLWriter(System.out);
            tupleQuery.evaluate(writer);

            conn.close();

            myRepository.shutDown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
