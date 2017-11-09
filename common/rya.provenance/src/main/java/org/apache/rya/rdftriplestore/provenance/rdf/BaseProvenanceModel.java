package org.apache.rya.rdftriplestore.provenance.rdf;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;

/**
 * Basic representation of Provenance data capture in RDF.
 */
public class BaseProvenanceModel implements RDFProvenanceModel {
	
	private static final ValueFactory VF = SimpleValueFactory.getInstance();
	private static final Resource QUERY_EVENT_TYPE = VF.createIRI("http://rya.com/provenance#QueryEvent");
	private static final IRI AT_TIME_PROPERTY = VF.createIRI("http://www.w3.org/ns/prov#atTime");
	private static final IRI ASSOCIATED_WITH_USER = VF.createIRI("http://rya.com/provenance#associatedWithUser");
	private static final IRI QUERY_TYPE_PROP = VF.createIRI("http://rya.com/provenance#queryType");
	private static final IRI EXECUTED_QUERY_PROPERTY = VF.createIRI("http://rya.com/provenance#executedQuery");
	private static final String QUERY_NAMESPACE = "http://rya.com/provenance#queryEvent";

	/* (non-Javadoc)
	 * @see org.apache.rya.rdftriplestore.provenance.rdf.RDFProvenanceModel#getStatementsForQuery(java.lang.String, java.lang.String, java.lang.String)
	 */
	public List<Statement> getStatementsForQuery(String query, String user, String queryType) {
		List<Statement> statements = new ArrayList<Statement>();
		// create some statements for the query
		Resource queryEventResource = VF.createIRI(QUERY_NAMESPACE + UUID.randomUUID().toString());
		Statement queryEventDecl = VF.createStatement(queryEventResource, RDF.TYPE, QUERY_EVENT_TYPE);
		statements.add(queryEventDecl);
		Statement queryEventTime = VF.createStatement(queryEventResource, AT_TIME_PROPERTY, VF.createLiteral(new Date()));
		statements.add(queryEventTime);
		Statement queryUser = VF.createStatement(queryEventResource, ASSOCIATED_WITH_USER, VF.createLiteral(user));
		statements.add(queryUser);
		Statement executedQuery = VF.createStatement(queryEventResource, EXECUTED_QUERY_PROPERTY, VF.createLiteral(query));
		statements.add(executedQuery);
		Statement queryTypeStatement = VF.createStatement(queryEventResource, QUERY_TYPE_PROP, VF.createLiteral(queryType));
		statements.add(queryTypeStatement);
		return statements;
	}

}
