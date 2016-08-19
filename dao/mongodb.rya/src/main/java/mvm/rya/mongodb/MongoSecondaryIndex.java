package mvm.rya.mongodb;

import com.mongodb.MongoClient;

import mvm.rya.api.persist.index.RyaSecondaryIndexer;

public interface MongoSecondaryIndex extends RyaSecondaryIndexer{
    public void init();    

    public void setClient(MongoClient client);
	
}
