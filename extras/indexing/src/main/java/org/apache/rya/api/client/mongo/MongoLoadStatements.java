package org.apache.rya.api.client.mongo;

import org.apache.rya.api.client.InstanceDoesNotExistException;
import org.apache.rya.api.client.LoadStatements;
import org.apache.rya.api.client.RyaClientException;
import org.openrdf.model.Statement;

import com.mongodb.MongoClient;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * An Mongo implementation of the {@link LoadStatements} command.
 */
@DefaultAnnotation(NonNull.class)
public class MongoLoadStatements extends MongoCommand implements LoadStatements {

    public MongoLoadStatements(MongoConnectionDetails connectionDetails, MongoClient client) {
        super(connectionDetails, client);
    }

    @Override
    public void loadStatements(String ryaInstanceName, Iterable<? extends Statement> statements) throws InstanceDoesNotExistException, RyaClientException {
        // TODO Auto-generated method stub

    }

}
