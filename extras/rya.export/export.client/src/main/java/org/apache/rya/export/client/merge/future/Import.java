package org.apache.rya.export.client.merge.future;

import static java.util.Objects.requireNonNull;

import java.util.Iterator;
import java.util.List;

import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.export.api.metadata.MergeParentMetadata;
import org.apache.rya.export.api.store.ContainsStatementException;
import org.apache.rya.export.api.store.FetchStatementException;
import org.apache.rya.export.api.store.RyaStatementStore;
import org.apache.rya.export.client.merge.future.RepSynchRunnableFactory.StatementListener;

/**
 * Synchronizes an existing Rya instance with it's parent.
 */
class Import extends RepSynchRunnable {
    private final MergeParentMetadata metadata;
    private final long timeOffset;

    /**
     * Constructs a new {@link Import}.
     * @param sourceStore - The source {@link RyaStatementStore} where statements are pulled from. (not null)
     * @param destStore - The destination {@link RyaStatementStore} to synchronize. (not null)
     * @param metadata - The {@link MergeParentMetadata} defining the destination store. (not null)
     * @param timeOffset - The offset between the 2 {@link RyaStatementStore}s time.
     * @param listeners - The {@link StatementListener}s notified as statements are added/removed from {@link RyaStatementStore}s. (not null)
     */
    public Import(final RyaStatementStore sourceStore, final RyaStatementStore destStore, final MergeParentMetadata metadata, final long timeOffset, final List<StatementListener> listeners) {
        super(sourceStore, destStore, listeners);
        this.timeOffset = timeOffset;
        this.metadata = requireNonNull(metadata);
    }


    @Override
    public void run() {
        try {
            //statements are in order by timestamp.
            final Iterator<RyaStatement> parentStatements = sourceStore.fetchStatements();
            final Iterator<RyaStatement> childStatements = destStore.fetchStatements();

            //Remove statements that were removed in the child.
            //after the timestamp has passed, there is no need to keep checking the parent
            while(childStatements.hasNext() && !isCanceled) {
                final RyaStatement statement = childStatements.next();
                if(statement.getTimestamp() > metadata.getTimestamp().getTime()) {
                    break;
                }
                if(!sourceStore.containsStatement(statement)) {
                    remove(destStore, statement);
                } else {
                    updateCount();
                }
            }

            //Add all of the child statements that are not in the parent
            while(parentStatements.hasNext() && !isCanceled) {
                final RyaStatement statement = parentStatements.next();
                if(!destStore.containsStatement(statement)) {
                    statement.setTimestamp(statement.getTimestamp() - timeOffset);
                    add(destStore, statement);
                } else {
                    updateCount();
                }
            }
        } catch (final FetchStatementException e) {
            e.printStackTrace();
        } catch (final ContainsStatementException e) {
            e.printStackTrace();
        }
    }
}