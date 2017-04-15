package org.apache.rya.indexing.accumulo.customfunction;

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
import java.util.HashMap;
import java.util.Set;

import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.accumulo.freetext.SimpleTokenizer;
import org.apache.rya.indexing.accumulo.freetext.Tokenizer;
import org.apache.rya.sail.config.RyaSailFactory;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.Sail;

import org.junit.*;

import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;

public class AccumuloCustomSparqlFunctionTest {
	private AccumuloRdfConfiguration conf;
	private Repository repository = null;
	private Sail sail = null;
	private RepositoryConnection conn = null;
	

	@Before
    public void before() throws Exception {
        conf = new AccumuloRdfConfiguration();
        conf.setBoolean(ConfigUtils.USE_MOCK_INSTANCE, true);
        conf.set(ConfigUtils.CLOUDBASE_USER, "USERNAME");
        conf.set(ConfigUtils.CLOUDBASE_PASSWORD, "PASS");
        conf.set(ConfigUtils.CLOUDBASE_AUTHS, "U");
        conf.setClass(ConfigUtils.TOKENIZER_CLASS, SimpleTokenizer.class, Tokenizer.class);
        conf.setTablePrefix("triplestore_");
        
        this.sail = RyaSailFactory.getInstance(conf);
        this.repository = new SailRepository(this.sail);
        this.conn = this.repository.getConnection();
        
    }
	
	@Test
	public void customFunction() {
		String sql = "" 
				+ "PREFIX fn: <http://example.org#> "
				+ "select (fn:mycustomfucnction() as ?res1) (str(fn:mycustomfucnction('Rya')) as ?res2) "
				+ "where {}";
		try {
			TupleQuery query = this.conn.prepareTupleQuery(QueryLanguage.SPARQL, sql);
		
			TupleQueryResult qres = query.evaluate();
			ArrayList<HashMap<String, Value>> reslist = new ArrayList<HashMap<String, Value>>();
			while (qres.hasNext()) {
				BindingSet b = qres.next();
				Set<String> names = b.getBindingNames();
				HashMap<String, Value> hm = new HashMap<String, Value>();
				for (Object n : names) {
					hm.put(
							(String) n, 
							b.getValue((String) n)
					);
				}
				reslist.add(hm);
				
				if (hm.containsKey("res1")) {
					Assert.assertEquals(hm.get("res1").stringValue(), "Hello");
				}
				else {
					Assert.fail("'res1' is not in result set.");
				}
				
				if (hm.containsKey("res2")) {
					Assert.assertEquals(hm.get("res2").stringValue(), "Hello, Rya");
				}
				else {
					Assert.fail("'res2' is not in result set");
				}
			} 
		}
		catch (Exception ex) {
			Assert.fail(ex.getMessage());
		}
		 
				
	}
}
