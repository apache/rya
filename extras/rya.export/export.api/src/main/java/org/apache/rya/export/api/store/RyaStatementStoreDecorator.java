package org.apache.rya.export.api.store;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;

import mvm.rya.api.domain.RyaStatement;

/**
 *
 */
public abstract class RyaStatementStoreDecorator implements RyaStatementStore {
    final RyaStatementStore store;

    /**
     * Creates a new {@link RyaStatementStoreDecorator} around the provided {@link RyaStatementStore}.
     * @param store - The {@link RyaStatementStore} to decorate.
     */
    public RyaStatementStoreDecorator(final RyaStatementStore store) {
        this.store = checkNotNull(store);
    }

    @Override
    public Iterator<RyaStatement> fetchStatements() {
        return store.fetchStatements();
    }

    @Override
    public void addStatement(final RyaStatement statement) throws AddStatementException {
        store.addStatement(statement);
    }

    @Override
    public void removeStatement(final RyaStatement statement) throws RemoveStatementException {
        store.removeStatement(statement);
    }

    @Override
    public boolean containsStatement(final RyaStatement statement) throws ContainsStatementException {
        return store.containsStatement(statement);
    }
}
