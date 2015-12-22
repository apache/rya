package mvm.rya.mongodb.dao;

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


import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.domain.RyaType;
import mvm.rya.api.domain.RyaURI;
import mvm.rya.api.persist.query.RyaQuery;

import org.apache.commons.codec.binary.Hex;
import org.openrdf.model.impl.ValueFactoryImpl;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class SimpleMongoDBStorageStrategy implements MongoDBStorageStrategy {

	protected static final String ID = "_id";
	protected static final String OBJECT_TYPE = "objectType";
	protected static final String CONTEXT = "context";
	protected static final String PREDICATE = "predicate";
	protected static final String OBJECT = "object";
	protected static final String SUBJECT = "subject";
	protected ValueFactoryImpl factory = new ValueFactoryImpl();


	public SimpleMongoDBStorageStrategy() {
	}
	
	@Override
	public void createIndices(DBCollection coll){
		BasicDBObject doc = new BasicDBObject();
	    doc.put(SUBJECT, 1);
	    doc.put(PREDICATE, 1);
		coll.createIndex(doc);
		doc = new BasicDBObject(PREDICATE, 1);
		doc.put(OBJECT, 1);
		doc.put(OBJECT_TYPE, 1);
		coll.createIndex(doc);
		doc = new BasicDBObject(OBJECT, 1);
		doc = new BasicDBObject(OBJECT_TYPE, 1);
		doc.put(SUBJECT, 1);
		coll.createIndex(doc);
	}

	@Override
	public DBObject getQuery(RyaStatement stmt) {
		RyaURI subject = stmt.getSubject();
		RyaURI predicate = stmt.getPredicate();
		RyaType object = stmt.getObject();
		RyaURI context = stmt.getContext();
		BasicDBObject query = new BasicDBObject();
		if (subject != null){
			query.append(SUBJECT, subject.getData());
		}
		if (object != null){
			query.append(OBJECT, object.getData());
			query.append(OBJECT_TYPE, object.getDataType().toString());
		}
		if (predicate != null){
			query.append(PREDICATE, predicate.getData());
		}
		if (context != null){
			query.append(CONTEXT, context.getData());
		}
		
		return query;
	}

	@Override
	public RyaStatement deserializeDBObject(DBObject queryResult) {
		Map result = queryResult.toMap();
		String subject = (String) result.get(SUBJECT);
		String object = (String) result.get(OBJECT);
		String objectType = (String) result.get(OBJECT_TYPE);
		String predicate = (String) result.get(PREDICATE);
		String context = (String) result.get(CONTEXT);
		RyaType objectRya = null;
		if (objectType.equalsIgnoreCase("http://www.w3.org/2001/XMLSchema#anyURI")){
			objectRya = new RyaURI(object);
		}
		else {
			objectRya = new RyaType(factory.createURI(objectType), object);
		}
		
		if (!context.isEmpty()){
			return new RyaStatement(new RyaURI(subject), new RyaURI(predicate), objectRya,
					new RyaURI(context));			
		}
		return new RyaStatement(new RyaURI(subject), new RyaURI(predicate), objectRya);
	}

	@Override
	public DBObject serialize(RyaStatement statement){
		return serializeInternal(statement);		
	}

	public BasicDBObject serializeInternal(RyaStatement statement){
		String context = "";
		if (statement.getContext() != null){
			context = statement.getContext().getData();
		}
		String id = statement.getSubject().getData() + " " + 
				statement.getPredicate().getData() + " " +  statement.getObject().getData() + " " + context;
		byte[] bytes = id.getBytes();
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-1");
			bytes = digest.digest(bytes);
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		BasicDBObject doc = new BasicDBObject(ID, new String(Hex.encodeHex(bytes)))
		.append(SUBJECT, statement.getSubject().getData())
	    .append(PREDICATE, statement.getPredicate().getData())
	    .append(OBJECT, statement.getObject().getData())
	    .append(OBJECT_TYPE, statement.getObject().getDataType().toString())
	    .append(CONTEXT, context);
		return doc;
		
	}

	@Override
	public DBObject getQuery(RyaQuery ryaQuery) {
		return getQuery(ryaQuery.getQuery());
	}

}
