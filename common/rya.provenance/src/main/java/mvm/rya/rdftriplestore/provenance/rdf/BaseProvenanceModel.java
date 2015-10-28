package mvm.rya.rdftriplestore.provenance.rdf;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDF;

/**
 * Basic representation of Provenance data capture in RDF.
 */
public class BaseProvenanceModel implements RDFProvenanceModel {
	
	private static final ValueFactory vf = ValueFactoryImpl.getInstance();
	private static final Resource queryEventType = vf.createURI("http://rya.com/provenance#QueryEvent");
	private static final URI atTimeProperty = vf.createURI("http://www.w3.org/ns/prov#atTime");
	private static final URI associatedWithUser = vf.createURI("http://rya.com/provenance#associatedWithUser");
	private static final URI queryTypeProp = vf.createURI("http://rya.com/provenance#queryType");
	private static final URI executedQueryProperty = vf.createURI("http://rya.com/provenance#executedQuery");
	private static final String queryNameSpace = "http://rya.com/provenance#queryEvent";

	/* (non-Javadoc)
	 * @see mvm.rya.rdftriplestore.provenance.rdf.RDFProvenanceModel#getStatementsForQuery(java.lang.String, java.lang.String, java.lang.String)
	 */
	public List<Statement> getStatementsForQuery(String query, String user, String queryType) {
		List<Statement> statements = new ArrayList<Statement>();
		// create some statements for the query
		Resource queryEventResource = vf.createURI(queryNameSpace + UUID.randomUUID().toString());
		Statement queryEventDecl = vf.createStatement(queryEventResource, RDF.TYPE, queryEventType);
		statements.add(queryEventDecl);
		Statement queryEventTime = vf.createStatement(queryEventResource, atTimeProperty, vf.createLiteral(new Date()));
		statements.add(queryEventTime);
		Statement queryUser = vf.createStatement(queryEventResource, associatedWithUser, vf.createLiteral(user));
		statements.add(queryUser);
		Statement executedQuery = vf.createStatement(queryEventResource, executedQueryProperty, vf.createLiteral(query));
		statements.add(executedQuery);
		Statement queryTypeStatement = vf.createStatement(queryEventResource, queryTypeProp, vf.createLiteral(queryType));
		statements.add(queryTypeStatement);
		return statements;
	}

}
