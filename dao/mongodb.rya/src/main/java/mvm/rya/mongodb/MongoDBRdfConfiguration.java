package mvm.rya.mongodb;


import java.util.List;

import mvm.rya.api.RdfCloudTripleStoreConfiguration;
import mvm.rya.api.persist.index.RyaSecondaryIndexer;

import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Lists;

public class MongoDBRdfConfiguration extends RdfCloudTripleStoreConfiguration {
    public static final String MONGO_INSTANCE = "mongo.db.instance";
    public static final String MONGO_INSTANCE_PORT = "mongo.db.port";
    public static final String MONGO_GEO_MAXDISTANCE = "mongo.geo.maxdist";
    public static final String MONGO_DB_NAME = "mongo.db.name";
    public static final String MONGO_COLLECTION_PREFIX = "mongo.db.collectionprefix";
    public static final String MONGO_USER = "mongo.db.user";
    public static final String  MONGO_USER_PASSWORD = "mongo.db.userpassword";
    public static final String USE_TEST_MONGO = "mongo.db.test";
    public static final String CONF_ADDITIONAL_INDEXERS = "ac.additional.indexers";

    public MongoDBRdfConfiguration() {
        super();
    }

    public MongoDBRdfConfiguration(Configuration other) {
        super(other);
    }

    @Override
    public MongoDBRdfConfiguration clone() {
        return new MongoDBRdfConfiguration(this);
    }

    public boolean getUseTestMongo() {
        return this.getBoolean(USE_TEST_MONGO, false);
    }

    public void setUseTestMongo(boolean useTestMongo) {
        this.setBoolean(USE_TEST_MONGO, useTestMongo);
    }

    public String getTriplesCollectionName() {
        return this.get(MONGO_COLLECTION_PREFIX, "rya") + "_triples";
    }

    public String getCollectionName() {
        return this.get(MONGO_COLLECTION_PREFIX, "rya");
    }

    public void setCollectionName(String name) {
        this.set(MONGO_COLLECTION_PREFIX, name);
    }

    public String getMongoInstance() {
        return this.get(MONGO_INSTANCE, "localhost");
    }

    public void setMongoInstance(String name) {
        this.set(MONGO_INSTANCE, name);
    }

    public String getMongoPort() {
        return this.get(MONGO_INSTANCE_PORT, "27017");
    }

    public void setMongoPort(String name) {
        this.set(MONGO_INSTANCE_PORT, name);
    }

    public String getMongoDBName() {
        return this.get(MONGO_DB_NAME, "rya");
    }

    public void setMongoDBName(String name) {
        this.set(MONGO_DB_NAME, name);
    }

    public String getNameSpacesCollectionName() {
        return this.get(MONGO_COLLECTION_PREFIX, "rya") + "_ns";
    }
    
    public void setAdditionalIndexers(Class<? extends RyaSecondaryIndexer>... indexers) {
        List<String> strs = Lists.newArrayList();
        for (Class ai : indexers){
            strs.add(ai.getName());
        }
        
        setStrings(CONF_ADDITIONAL_INDEXERS, strs.toArray(new String[]{}));
    }

    public List<RyaSecondaryIndexer> getAdditionalIndexers() {
        return getInstances(CONF_ADDITIONAL_INDEXERS, RyaSecondaryIndexer.class);
    }
    
    
    

}
