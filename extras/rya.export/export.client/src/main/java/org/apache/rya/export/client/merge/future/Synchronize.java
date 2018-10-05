package org.apache.rya.export.client.merge.future;

import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.export.api.store.ContainsStatementException;
import org.apache.rya.export.api.store.FetchStatementException;
import org.apache.rya.export.api.store.RyaStatementStore;
import org.apache.rya.export.client.merge.future.RepSynchRunnableFactory.StatementListener;

/**
 * Synchronizes an existing Rya instance with it's parent.
 */
class Synchronize extends RepSynchRunnable {
    private static final Logger LOG = Logger.getLogger(Synchronize.class);
    private final long timeOffset;

    /**
     * Constructs a new {@link Synchronize}.
     * @param sourceStore - The source {@link RyaStatementStore} where statements are pulled from. (not null)
     * @param destStore - The destination {@link RyaStatementStore} to synchronize. (not null)
     * @param timeOffset - The offset between the 2 {@link RyaStatementStore}s time.
     * @param listeners - The {@link StatementListener}s notified as statements are added/removed from {@link RyaStatementStore}s. (not null)
     */
    public Synchronize(final RyaStatementStore sourceStore, final RyaStatementStore destStore, final long timeOffset, final List<StatementListener> listeners) {
        super(sourceStore, destStore, listeners);
        this.timeOffset = timeOffset;
    }


    @Override
    public void run() {
        LOG.info("Synching " + destStore.getRyaInstanceName() + " child store.");
        try {
            final Iterator<RyaStatement> sourceStatements = sourceStore.fetchStatements();
            final Iterator<RyaStatement> destinationStatements = destStore.fetchStatements();

            //Remove statements that were removed from the parent.
            while(destinationStatements.hasNext() && !isCanceled) {
                final RyaStatement statement = destinationStatements.next();
                if(!sourceStore.containsStatement(statement)) {
                    remove(destStore, statement);
                } else {
                    updateCount();
                }
            }

            //Add all of the new statements in the parent store that aren't in the child
            while(sourceStatements.hasNext() && !isCanceled) {
                final RyaStatement statement = sourceStatements.next();

                if(!destStore.containsStatement(statement)) {
                    statement.setTimestamp(statement.getTimestamp() - timeOffset);
                    add(destStore, statement);
                } else {
                    updateCount();
                }
            }
        } catch (final FetchStatementException e) {
            LOG.error("Failed to fetch statements from the Rya Store.", e);
        } catch (final ContainsStatementException e) {
            LOG.error("Unable to detect statement in the Rya Store.", e);
        }
    }
}