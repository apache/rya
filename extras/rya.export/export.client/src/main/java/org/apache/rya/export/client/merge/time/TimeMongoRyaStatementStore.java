package org.apache.rya.export.client.merge.time;

import static com.google.common.base.Preconditions.checkNotNull;
import static mvm.rya.mongodb.dao.SimpleMongoDBStorageStrategy.TIMESTAMP;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.rya.export.api.store.RyaStatementStoreDecorator;
import org.apache.rya.export.mongo.MongoRyaStatementStore;

import com.mongodb.BasicDBObject;
import com.mongodb.Cursor;
import com.mongodb.DB;
import com.mongodb.MongoClient;

import mvm.rya.api.domain.RyaStatement;
import mvm.rya.mongodb.dao.SimpleMongoDBStorageStrategy;

public class TimeMongoRyaStatementStore extends RyaStatementStoreDecorator {
    private final Date time;
    private final DB db;

    private final SimpleMongoDBStorageStrategy adapter;

    /**
     * @param client
     * @param ryaInstance
     * @param time
     */
    public TimeMongoRyaStatementStore(final MongoRyaStatementStore store, final MongoClient client, final Date time, final String ryaInstanceName) {
        super(store);
        this.time = checkNotNull(time);
        db = client.getDB(ryaInstanceName);
        adapter = new SimpleMongoDBStorageStrategy();
    }

    /**
     * @return
     * @see org.apache.rya.export.mongo.MongoRyaStatementStore#fetchStatements()
     */
    @Override
    public Iterator<RyaStatement> fetchStatements() {
        final BasicDBObject dbo = new BasicDBObject(TIMESTAMP, time);
        final Cursor cur = db.getCollection(MongoRyaStatementStore.TRIPLES_COLLECTION).find(dbo);
        final List<RyaStatement> statements = new ArrayList<>();
        while(cur.hasNext()) {
            final RyaStatement statement = adapter.deserializeDBObject(cur.next());
            statements.add(statement);
        }
        return statements.iterator();
    }

    @Override
    public boolean equals(final Object obj) {
        if(obj instanceof TimeMongoRyaStatementStore) {
            final TimeMongoRyaStatementStore other = (TimeMongoRyaStatementStore) obj;
            final EqualsBuilder builder = new EqualsBuilder()
                    .appendSuper(super.equals(obj))
                    .append(time, other.time);
            return builder.isEquals();
        }
        return false;
    }


    @Override
    public int hashCode() {
        final HashCodeBuilder builder = new HashCodeBuilder()
            .appendSuper(super.hashCode())
            .append(time);
        return builder.toHashCode();
    }
}
