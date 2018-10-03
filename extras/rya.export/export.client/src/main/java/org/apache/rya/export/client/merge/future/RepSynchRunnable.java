package org.apache.rya.export.client.merge.future;

import static java.util.Objects.requireNonNull;

import java.util.List;

import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.export.api.store.AddStatementException;
import org.apache.rya.export.api.store.RemoveStatementException;
import org.apache.rya.export.api.store.RyaStatementStore;
import org.apache.rya.export.client.merge.future.RepSynchRunnableFactory.StatementListener;

/**
 * A {@link Runable} used by the rep/synch tool.
 * All cancel/done functionality is the same across
 * all rep/synch futures.
 */
public abstract class RepSynchRunnable implements Runnable {
    protected boolean isCanceled;
    protected final RyaStatementStore sourceStore;
    protected final RyaStatementStore destStore;

    private final List<StatementListener> listeners;

    /**
     * Constructs a new {@link RepSynchRunnable}.
     * @param sourceStore - The source {@link RyaStatementStore} where statements are pulled from. (not null)
     * @param destStore - The destination {@link RyaStatementStore} where statements are put to. (not null)
     * @param listeners - The {@link StatementListener}s notified as statements are added/removed from {@link RyaStatementStore}s. (not null)
     */
    public RepSynchRunnable(final RyaStatementStore sourceStore, final RyaStatementStore destStore, final List<StatementListener> listeners) {
        this.sourceStore = requireNonNull(sourceStore);
        this.destStore = requireNonNull(destStore);
        this.listeners = requireNonNull(listeners);

        isCanceled = false;
    }

    /**
     * Cancels the current rep/synch task.
     */
    public void cancel() {
        isCanceled = true;
    }

    @Override
    public abstract void run();

    /**
     * Adds and notifies the listeners that a statement was added.
     *
     * @param store - The {@link RyaStatementStore} to add a statement to. (not null)
     * @param stmnt - The {@link RyaStatement} to add. (not null)
     */
    protected void add(final RyaStatementStore store, final RyaStatement stmnt) {
        try {
            store.addStatement(stmnt);
            for(final StatementListener listen : listeners) {
                listen.notifyAddStatement(store, stmnt);
            }
        } catch (final AddStatementException e) {
            e.printStackTrace();
        }
    }

    /**
     * Removes and notifies the listeners that a statement was removed.
     *
     * @param store - The {@link RyaStatementStore} to remove a statement from. (not null)
     * @param stmnt - The {@link RyaStatement} to remove. (not null)
     */
    protected void remove(final RyaStatementStore store, final RyaStatement stmnt) {
        requireNonNull(store);
        requireNonNull(stmnt);
        try {
            store.removeStatement(stmnt);
            for(final StatementListener listen : listeners) {
                listen.notifyRemoveStatement(store, stmnt);
            }
        } catch (final RemoveStatementException e) {
            e.printStackTrace();
        }
    }

    /**
     * Updates the count.  A statement has been processed and may or may not have been added/removed
     */
    protected void updateCount() {
        for(final StatementListener listen : listeners) {
            listen.updateCount();
        }
    }
}