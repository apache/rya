package mvm.rya.mongodb.dao;

import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.persist.query.RyaQuery;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public interface MongoDBStorageStrategy {

	public DBObject getQuery(RyaStatement stmt);

	public RyaStatement deserializeDBObject(DBObject queryResult);

	public DBObject serialize(RyaStatement statement);

	public DBObject getQuery(RyaQuery ryaQuery);

	public void createIndices(DBCollection coll);

}
