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
