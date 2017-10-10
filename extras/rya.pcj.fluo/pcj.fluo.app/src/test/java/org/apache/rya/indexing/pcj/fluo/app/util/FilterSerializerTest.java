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
package org.apache.rya.indexing.pcj.fluo.app.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

public class FilterSerializerTest {

    @Test
    public void nowTest() throws Exception {

        //tests to see if NOW function is correctly serialized and deserialized
        //by FilterSerializer
        String query = "select * {Filter(NOW())}";
        SPARQLParser parser = new SPARQLParser();
        ParsedQuery pq = parser.parseQuery(query, null);
        Filter filter = (Filter) ((Projection) pq.getTupleExpr()).getArg();
        String filterString = FilterSerializer.serialize(filter);
        Filter deserializedFilter = FilterSerializer.deserialize(filterString);
        
        assertEquals(filter, deserializedFilter);

    }
    

}
