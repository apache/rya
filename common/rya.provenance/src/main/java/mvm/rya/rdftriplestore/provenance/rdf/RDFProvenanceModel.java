package mvm.rya.rdftriplestore.provenance.rdf;

import java.util.List;

import org.openrdf.model.Statement;


public interface RDFProvenanceModel {

	List<Statement> getStatementsForQuery(String query, String user, String queryType);

	
}
