package org.apache.rya.export.client.merge.future;

import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.export.api.store.FetchStatementException;
import org.apache.rya.export.api.store.RyaStatementStore;
import org.apache.rya.export.client.merge.future.RepSynchRunnableFactory.StatementListener;

/**
 * Replicates a rya instance.
 */
class Replication extends RepSynchRunnable {
    private static final Logger LOG = Logger.getLogger(Replication.class);

    /**
     * Constructs a new {@link Replication}.
     * @param sourceStore - The source {@link RyaStatementStore} where statements are pulled from. (not null)
     * @param destStore - The destination {@link RyaStatementStore} where statements are put to. (not null)
     * @param listeners - The {@link StatementListener}s notified as statements are added/removed from {@link RyaStatementStore}s. (not null)
     */
    public Replication(final RyaStatementStore sourceStore, final RyaStatementStore destStore, final List<StatementListener> listeners) {
        super(sourceStore, destStore, listeners);
    }


    @Override
    public void run() {
        //fetch all statements after timestamp from the parent
        Iterator<RyaStatement> statements;
        try {
            statements = sourceStore.fetchStatements();
            LOG.info("Exporting statements.");
            while(statements.hasNext() && !isCanceled) {
                final RyaStatement statement = statements.next();
                super.add(destStore, statement);
            }
        } catch (final FetchStatementException e) {
            e.printStackTrace();
        }
    }
}