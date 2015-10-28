package mvm.rya.rdftriplestore.provenance;

import org.openrdf.repository.RepositoryException;

/**
 *  Exception for errors in collecting provenance data
 */
public class ProvenanceCollectionException extends Exception {

	public ProvenanceCollectionException(RepositoryException e) {
		super(e);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

}
