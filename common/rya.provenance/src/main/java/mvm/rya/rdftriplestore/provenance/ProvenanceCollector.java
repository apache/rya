package mvm.rya.rdftriplestore.provenance;

/**
 *  Collects/records provenance data
 */
public interface ProvenanceCollector {

	/**
	 * Records appropriate metadata about a query
	 * @param query the query being recorded.  cannot be null
	 * @throws ProvenanceCollectionException
	 */
	public void recordQuery(String query) throws ProvenanceCollectionException;
}
