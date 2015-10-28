package mvm.rya.indexing.mongodb;

import info.aduna.iteration.CloseableIteration;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.resolver.RyaToRdfConversions;
import mvm.rya.indexing.GeoIndexer;
import mvm.rya.indexing.StatementContraints;
import mvm.rya.indexing.accumulo.ConfigUtils;
import mvm.rya.indexing.accumulo.temporal.AccumuloTemporalIndexer;
import mvm.rya.indexing.mongodb.GeoMongoDBStorageStrategy.GeoQueryType;
import mvm.rya.mongodb.MongoDBRdfConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.opengis.feature.simple.SimpleFeature;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.query.QueryEvaluationException;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;

public class MongoGeoIndexer extends AbstractMongoIndexer implements GeoIndexer{
    
    private static final Logger logger = Logger.getLogger(MongoGeoIndexer.class);
    
    private GeoMongoDBStorageStrategy storageStrategy;
    private MongoClient mongoClient;
    private DB db;
    private DBCollection coll;
    private Set<URI> predicates;
    private Configuration conf;
    private boolean isInit = false;
    private String tableName = "";

    
    
    private void init() throws NumberFormatException, UnknownHostException{
        ServerAddress server = new ServerAddress(conf.get(MongoDBRdfConfiguration.MONGO_INSTANCE),
                Integer.valueOf(conf.get(MongoDBRdfConfiguration.MONGO_INSTANCE_PORT)));
        this.conf = conf;
        if (conf.get(MongoDBRdfConfiguration.MONGO_USER) != null){
            MongoCredential cred = MongoCredential.createCredential(conf.get(MongoDBRdfConfiguration.MONGO_USER), conf.get(MongoDBRdfConfiguration.MONGO_USER_PASSWORD),
                    conf.get(MongoDBRdfConfiguration.MONGO_DB_NAME).toCharArray());
            mongoClient = new MongoClient(server, Arrays.asList(cred));
        }
        else {
            mongoClient = new MongoClient(server);
        }
        predicates = ConfigUtils.getGeoPredicates(conf);
        tableName = conf.get(MongoDBRdfConfiguration.MONGO_DB_NAME);
        db = mongoClient.getDB(tableName);
        coll = db.getCollection(conf.get(MongoDBRdfConfiguration.MONGO_COLLECTION_PREFIX, "rya") + "_geo");
        storageStrategy = new GeoMongoDBStorageStrategy(Double.valueOf(conf.get(MongoDBRdfConfiguration.MONGO_GEO_MAXDISTANCE, "1e-10")));
    }
    

    @Override
    public String getTableName() {
        return tableName;
    }
    
    @Override
    public Configuration getConf() {
        return conf;
    }
    
    //setConf initializes because index is created via reflection
    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        if (!isInit) {
            try {
                init();
                isInit = true;
            } catch (NumberFormatException e) {
                logger.warn("Unable to initialize index.  Throwing Runtime Exception. ", e);
                throw new RuntimeException(e);
            } catch (UnknownHostException e) {
                logger.warn("Unable to initialize index.  Throwing Runtime Exception. ", e);
                throw new RuntimeException(e);
            }
        }
    }
    

    
    private void storeStatement(Statement statement) throws IOException {
        // if this is a valid predicate and a valid geometry
        boolean isValidPredicate = predicates.isEmpty() || predicates.contains(statement.getPredicate());

        if (isValidPredicate && (statement.getObject() instanceof Literal)) {
            
            // add it to the collection
            try {
                DBObject obj = storageStrategy.serialize(statement);
                if (obj != null){
                    coll.insert(obj);
                }
            }
            catch (com.mongodb.MongoException.DuplicateKey exception){
                // ignore
            }
            catch (com.mongodb.DuplicateKeyException exception){
                // ignore
            }
            catch (Exception ex){
                // ignore single exceptions
                ex.printStackTrace();
            }
        }
    }
    
    
    @Override
    public void storeStatement(RyaStatement statement) throws IOException {
        storeStatement(RyaToRdfConversions.convertStatement(statement));
    }
    
    


    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryEquals(
            Geometry query, StatementContraints contraints) {
        DBObject queryObj = storageStrategy.getQuery(contraints, query, GeoQueryType.EQUALS);
        return getIteratorWrapper(queryObj, coll, storageStrategy);
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryDisjoint(
            Geometry query, StatementContraints contraints) {
        throw new UnsupportedOperationException("Disjoint queries are not supported in Mongo DB.");
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryIntersects(
                Geometry query, StatementContraints contraints) {
        DBObject queryObj = storageStrategy.getQuery(contraints, query, GeoQueryType.INTERSECTS);
        return getIteratorWrapper(queryObj, coll, storageStrategy);
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryTouches(
            Geometry query, StatementContraints contraints) {
        throw new UnsupportedOperationException("Touches queries are not supported in Mongo DB.");
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryCrosses(
            Geometry query, StatementContraints contraints) {
        throw new UnsupportedOperationException("Crosses queries are not supported in Mongo DB.");
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryWithin(
            Geometry query, StatementContraints contraints) {
        DBObject queryObj = storageStrategy.getQuery(contraints, query, GeoQueryType.WITHIN);
        return getIteratorWrapper(queryObj, coll, storageStrategy);
    }
    
    
    private CloseableIteration<Statement, QueryEvaluationException> getIteratorWrapper(final DBObject query, final DBCollection coll, final GeoMongoDBStorageStrategy storageStrategy) {

        return new CloseableIteration<Statement, QueryEvaluationException>() {

            private DBCursor cursor = null;

            private DBCursor getIterator() throws QueryEvaluationException {
                if (cursor == null){
                    cursor = coll.find(query);
                }
                return cursor;
            }

            @Override
            public boolean hasNext() throws QueryEvaluationException {
                return getIterator().hasNext();
            }

            @Override
            public Statement next() throws QueryEvaluationException {
                DBObject feature = getIterator().next();
                return storageStrategy.deserializeDBObject(feature);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("Remove not implemented");
            }

            @Override
            public void close() throws QueryEvaluationException {
                getIterator().close();
            }
        };
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryContains(
            Geometry query, StatementContraints contraints) {
        throw new UnsupportedOperationException("Contains queries are not supported in Mongo DB.");
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryOverlaps(
            Geometry query, StatementContraints contraints) {
        throw new UnsupportedOperationException("Overlaps queries are not supported in Mongo DB.");
    }

    @Override
    public Set<URI> getIndexablePredicates() {
        return predicates;
    }

    @Override
    public void flush() throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public void close() throws IOException {
        mongoClient.close();
    }

    
}
