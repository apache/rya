package mvm.rya.rdftriplestore.provenance.rdf;

import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;
import org.openrdf.model.Statement;

public class BaseProvenanceModelTest {

	@Test
	public void testCreateTriples() {
		BaseProvenanceModel model = new BaseProvenanceModel();
		List<Statement> statements = model.getStatementsForQuery("SELECT ?query where { ?query rdf:type <rya:query>.  }", "fakeuser", "SPARQL");
		assertTrue(!statements.isEmpty());		
	}
}
