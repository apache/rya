package mvm.rya.rdftriplestore.provenance;

import java.util.List;

import mvm.rya.rdftriplestore.provenance.rdf.BaseProvenanceModel;
import mvm.rya.rdftriplestore.provenance.rdf.RDFProvenanceModel;

import org.openrdf.model.Statement;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;

/**
 * Records provenance data to an external rdf triplestore
 */
public class TriplestoreProvenanceCollector implements ProvenanceCollector {

	private RDFProvenanceModel provenanceModel;
	private SailRepository provenanceRepo;
	private String user;
	private String queryType;
	
	/**
	 * @param repo the repository to record to
	 * @param user the user issuing the query
	 * @param queryType the type of query (SPARQL, etc.)
	 */
	public TriplestoreProvenanceCollector(SailRepository repo, String user, String queryType){
		provenanceRepo = repo;
		provenanceModel = new BaseProvenanceModel();
		this.user = user;
		this.queryType = queryType;
	}
	
	/* (non-Javadoc)
	 * @see mvm.rya.rdftriplestore.provenance.ProvenanceCollector#recordQuery(java.lang.String)
	 */
	public void recordQuery(String query) throws ProvenanceCollectionException {
		List<Statement> provenanceTriples = provenanceModel.getStatementsForQuery(query, user, queryType);
		try {
			provenanceRepo.getConnection().add(provenanceTriples);
		} catch (RepositoryException e) {
			throw new ProvenanceCollectionException(e);
		}

	}

	
}
