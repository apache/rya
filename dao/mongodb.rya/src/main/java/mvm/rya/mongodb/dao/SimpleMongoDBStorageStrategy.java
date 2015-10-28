package mvm.rya.mongodb.dao;

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

	private static final String ID = "_id";
	private static final String OBJECT_TYPE = "objectType";
	private static final String CONTEXT = "context";
	private static final String PREDICATE = "predicate";
	private static final String OBJECT = "object";
	private static final String SUBJECT = "subject";
	private ValueFactoryImpl factory = new ValueFactoryImpl();


	public SimpleMongoDBStorageStrategy() {
	}
	
	@Override
	public void createIndices(DBCollection coll){
		coll.createIndex("subject");
		coll.createIndex("predicate");
		BasicDBObject doc = new BasicDBObject();
	    doc.put(SUBJECT, 1);
	    doc.put(PREDICATE, 1);
		coll.createIndex(doc);
		doc = new BasicDBObject(OBJECT, 1);
		doc.put(OBJECT_TYPE, 1);
		doc.put(PREDICATE, 1);
		coll.createIndex(doc);
		doc = new BasicDBObject(OBJECT, 1);
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
