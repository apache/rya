package org.apache.rya.rdftriplestore.provenance;

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


import java.util.List;

import org.apache.rya.rdftriplestore.provenance.rdf.BaseProvenanceModel;
import org.apache.rya.rdftriplestore.provenance.rdf.RDFProvenanceModel;

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
	 * @see org.apache.rya.rdftriplestore.provenance.ProvenanceCollector#recordQuery(java.lang.String)
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
