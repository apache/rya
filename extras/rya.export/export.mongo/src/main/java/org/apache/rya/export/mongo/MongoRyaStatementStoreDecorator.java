package org.apache.rya.export.mongo;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.rya.export.api.store.RyaStatementStore;
import org.apache.rya.export.api.store.RyaStatementStoreDecorator;

import com.mongodb.MongoClient;

/**
 * Ensures the decorator that the decorated store is mongodb backed.
 */
public abstract class MongoRyaStatementStoreDecorator extends RyaStatementStoreDecorator {
    final MongoRyaStatementStore store;

    /**
     * Creates a new {@link MongoRyaStatementStoreDecorator} around the provided {@link RyaStatementStore}.
     * @param store - The {@link RyaStatementStore} to decorate.
     */
    public MongoRyaStatementStoreDecorator(final MongoRyaStatementStore store) {
        super(store);
        this.store = checkNotNull(store);
    }

    protected MongoClient getClient() {
        return store.getClient();
    }
}
