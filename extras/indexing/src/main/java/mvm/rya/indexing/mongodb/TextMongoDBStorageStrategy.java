package mvm.rya.indexing.mongodb;

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


import java.util.Set;

import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.resolver.RdfToRyaConversions;
import mvm.rya.indexing.StatementContraints;
import mvm.rya.mongodb.dao.SimpleMongoDBStorageStrategy;

import org.openrdf.model.Statement;
import org.openrdf.model.URI;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;
import com.vividsolutions.jts.io.ParseException;

public class TextMongoDBStorageStrategy extends SimpleMongoDBStorageStrategy{

	private static final String text = "text";
	
	public void createIndices(DBCollection coll){
		BasicDBObject basicDBObject = new BasicDBObject();
		basicDBObject.append(text, "text");
		coll.createIndex(basicDBObject);
		
	}

	public DBObject getQuery(StatementContraints contraints, String textquery) {
		// TODO right now assuming the query string is a valid mongo query, this is 
		// not the case.  Should reuse the Accumulo free text parsing of the query string to make sure 
		// that behavior is consistent across Accumulo vs Mongo
		QueryBuilder queryBuilder = QueryBuilder.start().text(textquery);
		
		if (contraints.hasSubject()){
			queryBuilder.and(new BasicDBObject(SUBJECT, contraints.getSubject().toString()));
		}
		if (contraints.hasPredicates()){
			Set<URI> predicates = contraints.getPredicates();
			if (predicates.size() > 1){
				BasicDBList or = new BasicDBList();
				for (URI pred : predicates){
					DBObject currentPred = new BasicDBObject(PREDICATE, pred.toString());
					or.add(currentPred);
				}
				queryBuilder.or(or);
			}
			else if (!predicates.isEmpty()){
				queryBuilder.and(new BasicDBObject(PREDICATE, predicates.iterator().next().toString()));
			}
		}
		if (contraints.hasContext()){
			queryBuilder.and(new BasicDBObject(CONTEXT, contraints.getContext().toString()));
		}
		return queryBuilder.get();
	}

	public DBObject serialize(Statement statement) throws ParseException{
		
 		RyaStatement ryaStatement = RdfToRyaConversions.convertStatement(statement);
 		BasicDBObject base = (BasicDBObject) super.serialize(ryaStatement);
 		base.append(text, ryaStatement.getObject().getData());
     	return base;
		
	}

}
