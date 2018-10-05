package org.apache.rya.export.client.merge.future;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.export.api.metadata.MergeParentMetadata;
import org.apache.rya.export.api.store.RyaStatementStore;

/**
 * Factory for creating various {@link Future}s used by the Rep/Synch tool.
 */
public class RepSynchRunnableFactory {
    private final List<StatementListener> listeners;

    /**
     * Constructs a new {@link RepSynchRunnableFactory}.
     */
    private RepSynchRunnableFactory() {
        listeners = new ArrayList<>();
    }

    /**
     * @return - an instance of {@link RepSynchRunnableFactory}.
     */
    public static RepSynchRunnableFactory getInstance() {
        return new RepSynchRunnableFactory();
    }

    /**
     * Creates and returns a {@link Replication} runnable.
     * @param sourceStore - The {@link RyaStatementStore} to replicate. (not null)
     * @param destStore - The location to replicate to. (not null)
     * @return The created {@link Replication}.
     */
    public RepSynchRunnable getReplication(final RyaStatementStore sourceStore, final RyaStatementStore destStore) {
        requireNonNull(sourceStore);
        requireNonNull(destStore);
        final Replication rep = new Replication(sourceStore, destStore, listeners);
        return rep;
    }

    /**
     * Creates and returns an {@link Import} runnable.
     *
     * @param sourceStore - The {@link RyaStatementStore} the destination will be synchronized to. (not null)
     * @param destStore - The {@link RyaStatementStore} to be synchronized. (not null)
     * @param metadata - The {@link MergeParentMetadata} used to import {@link RyaStatement}s back into a Rya. (not null)
     * @param timeOffset - The time difference between the different {@link RyaStatementStore}s.
     * @return The created {@link Import}.
     */
    public RepSynchRunnable getImport(final RyaStatementStore sourceStore, final RyaStatementStore destStore, final MergeParentMetadata metadata, final long timeOffset) {
        requireNonNull(sourceStore);
        requireNonNull(destStore);
        requireNonNull(metadata);
        final Import imp = new Import(sourceStore, destStore, metadata, timeOffset, listeners);
        return imp;
    }

    /**
     * Creates and returns a {@link Synchronize} runnable.
     *
     * @param sourceStore - The {@link RyaStatementStore} the destination will be synchronized to. (not null)
     * @param destStore - The {@link RyaStatementStore} to be synchronized. (not null)
     * @param timeOffset - The time difference between the different {@link RyaStatementStore}s.
     * @return The created {@link Synchronize}.
     */
    public RepSynchRunnable getSynchronize(final RyaStatementStore sourceStore, final RyaStatementStore destStore, final long timeOffset) {
        requireNonNull(sourceStore);
        requireNonNull(destStore);
        final Synchronize synch = new Synchronize(sourceStore, destStore, timeOffset, listeners);
        return synch;
    }
    /**
     * @param listener - The listener added to be notified when a statement has been rep/synched. (not null)
     */
    public void addStatementListener(final StatementListener listener) {
        requireNonNull(listener);
        listeners.add(listener);
    }

    /**
     * @param listener - The listener to remove. (not null)
     */
    public void removeStatementListener(final StatementListener listener) {
        requireNonNull(listener);
        listeners.remove(listener);
    }

    /**
     * Listener that notifies when a statement has been replicated.
     */
    public interface StatementListener {
        /**
         * @param store - The store that a statement is being added to. (not null)
         * @param statement - The statement that has been replicated. (not null)
         */
        public void notifyAddStatement(final RyaStatementStore store, final RyaStatement statement);

        /**
         * @param store - The store that a statement is being removed from. (not null)
         * @param statement - The statmenet to remove. (not null)
         */
        public void notifyRemoveStatement(final RyaStatementStore store, final RyaStatement statement);

        /**
         * Updates the progress.
         */
        public void updateCount();
    }
}
