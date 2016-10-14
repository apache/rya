package org.apache.rya.mongodb.dao;

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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;

import org.apache.commons.codec.binary.Hex;
import org.openrdf.model.Namespace;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class SimpleMongoDBNamespaceManager implements MongoDBNamespaceManager {

	public class NamespaceImplementation implements Namespace {

		private String namespace;
		private String prefix;

		public NamespaceImplementation(String namespace, String prefix) {
			this.namespace = namespace;
			this.prefix = prefix;
		}

		@Override
		public int compareTo(Namespace o) {
			if (!namespace.equalsIgnoreCase(o.getName())) return namespace.compareTo(o.getName());
			if (!prefix.equalsIgnoreCase(o.getPrefix())) return prefix.compareTo(o.getPrefix());
			return 0;
		}

		@Override
		public String getName() {
			return namespace;
		}

		@Override
		public String getPrefix() {
			return prefix;
		}

	}

	public class MongoCursorIteration implements
			CloseableIteration<Namespace, RyaDAOException> {
		private DBCursor cursor;

		public MongoCursorIteration(DBCursor cursor2) {
			this.cursor = cursor2;
		}

		@Override
		public boolean hasNext() throws RyaDAOException {
			return cursor.hasNext();
		}

		@Override
		public Namespace next() throws RyaDAOException {
			DBObject ns = cursor.next();
			Map values = ns.toMap();
			String namespace = (String) values.get(NAMESPACE);
			String prefix = (String) values.get(PREFIX);
			
			Namespace temp =  new NamespaceImplementation(namespace, prefix);
			return temp;
		}

		@Override
		public void remove() throws RyaDAOException {
			next();
		}

		@Override
		public void close() throws RyaDAOException {
			cursor.close();
		}

	}

	private static final String ID = "_id";
	private static final String PREFIX = "prefix";
	private static final String NAMESPACE = "namespace";
	private MongoDBRdfConfiguration conf;
	private DBCollection nsColl;


	public SimpleMongoDBNamespaceManager(DBCollection nameSpaceCollection) {
		nsColl = nameSpaceCollection;
	}
	
	@Override
	public void createIndices(DBCollection coll){
		coll.createIndex(PREFIX);
		coll.createIndex(NAMESPACE);
	}


	@Override
	public void setConf(MongoDBRdfConfiguration paramC) {
		this.conf = paramC;
	}

	@Override
	public MongoDBRdfConfiguration getConf() {
		// TODO Auto-generated method stub
		return conf;
	}

	@Override
	public void addNamespace(String prefix, String namespace)
			throws RyaDAOException {
		String id = prefix;
		byte[] bytes = id.getBytes();
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-1");
			bytes = digest.digest(bytes);
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		BasicDBObject doc = new BasicDBObject(ID, new String(Hex.encodeHex(bytes)))
		.append(PREFIX, prefix)
	    .append(NAMESPACE, namespace);
		nsColl.insert(doc);
		
	}

	@Override
	public String getNamespace(String prefix) throws RyaDAOException {
        DBObject query = new BasicDBObject().append(PREFIX, prefix);
        DBCursor cursor = nsColl.find(query);
        String nameSpace = prefix;
        while (cursor.hasNext()){
          DBObject obj = cursor.next();	
          nameSpace = (String) obj.toMap().get(NAMESPACE);
        }
        return nameSpace;
	}

	@Override
	public void removeNamespace(String prefix) throws RyaDAOException {
        DBObject query = new BasicDBObject().append(PREFIX, prefix);
		nsColl.remove(query);
	}

	@Override
	public CloseableIteration<? extends Namespace, RyaDAOException> iterateNamespace()
			throws RyaDAOException {
        DBObject query = new BasicDBObject();
        DBCursor cursor = nsColl.find(query);
		return new MongoCursorIteration(cursor);
	}

}
