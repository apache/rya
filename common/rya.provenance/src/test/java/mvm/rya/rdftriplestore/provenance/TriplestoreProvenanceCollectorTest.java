package mvm.rya.rdftriplestore.provenance;

import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.Sail;
import org.openrdf.sail.memory.MemoryStore;

public class TriplestoreProvenanceCollectorTest {

	@Test
	public void testCollect() throws ProvenanceCollectionException, RepositoryException, MalformedQueryException, QueryEvaluationException {
		Sail ms = new MemoryStore();
		SailRepository repo = new SailRepository(ms);
		repo.initialize();
		TriplestoreProvenanceCollector coll = new TriplestoreProvenanceCollector(repo, "fakeUser", "SPARQL");
		coll.recordQuery("fakeQuery");
		String queryString = "SELECT ?x ?y WHERE { ?x ?p ?y } ";
		TupleQuery tupleQuery = repo.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		TupleQueryResult result = tupleQuery.evaluate();
		// TODO not asserting on the results.
		assertTrue(result.hasNext());
	}
}
