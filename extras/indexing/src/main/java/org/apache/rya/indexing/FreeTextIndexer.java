package org.apache.rya.indexing;

import java.io.IOException;

import org.openrdf.model.Statement;
import org.openrdf.query.QueryEvaluationException;

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



import info.aduna.iteration.CloseableIteration;
import org.apache.rya.api.persist.index.RyaSecondaryIndexer;

/**
 * A repository to store, index, and retrieve {@link Statement}s based on freetext features.
 */
public interface FreeTextIndexer extends RyaSecondaryIndexer {

	/**
	 * Query the Free Text Index with specific constraints. A <code>null</code> or empty parameters imply no constraint.
	 *
	 * @param query
	 *            the query to perform
	 * @param contraints
	 *            the constraints on the statements returned
	 * @return the set of statements that meet the query and other constraints.
	 * @throws IOException
	 */
	public abstract CloseableIteration<Statement, QueryEvaluationException> queryText(String query, StatementConstraints contraints) throws IOException;
}
