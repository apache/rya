package org.apache.rya.export.api;

import java.util.Iterator;

import mvm.rya.api.domain.RyaStatement;

/**
 * Allows specific CRUD operations on {@link RyaStatement} storage systems.
 * <p>
 * The operations specifically:
 * <li>fetch all rya statements in the store</li>
 * <li>add a rya statement to the store</li>
 * <li>remove a rya statement from the store</li>
 * <li>update an existing rya statement with a new one</li>
 *
 * One would use this {@link RyaStatementStore} when they have a database or
 * some storage system that is used when merging in data or exporting data.
 */
public interface RyaStatementStore {
    /**
     * @return an {@link Iterator} contianing all {@link RyaStatement}s found
     * in this {@link RyaStatementStore}.
     */
    public Iterator<RyaStatement> fetchStatements();

    /**
     * @param statement - The {@link RyaStatement} to add to this {@link RyaStatementStore}.
     */
    public void addStatement(final RyaStatement statement);

    /**
     * @param statement - The {@link RyaStatement} to remove from this {@link RyaStatementStore}.
     */
    public void removeStatement(final RyaStatement statement);

    /**
     * Updates the original {@link RyaStatement} with a new one.
     * @param originial - The {@link RyaStatement} to update.
     * @param update - The new {@link RyaStatement} to replace the original one.
     */
    public void updateStatement(final RyaStatement originial, final RyaStatement update);
}
