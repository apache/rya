package mvm.rya.mongodb.dao;

import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.persist.RyaNamespaceManager;
import mvm.rya.api.persist.query.RyaQuery;
import mvm.rya.mongodb.MongoDBRdfConfiguration;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public interface MongoDBNamespaceManager extends RyaNamespaceManager<MongoDBRdfConfiguration>{

	public void createIndices(DBCollection coll);

}
