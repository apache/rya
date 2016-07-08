package org.apache.rya.export.api;

import com.google.common.base.Optional;

import mvm.rya.api.domain.RyaStatement;

/**
 * Defines how 2 {@link RyaStatement}s will merge.
 */
public interface StatementManager {
    /**
     * Merges the child statement into the parent statement.
     * @param parent - The parent {@link RyaStatement}.
     * @param child - The child {@link RyaStatement}
     * @return The merged {@link RyaStatement}.
     */
    public Optional<RyaStatement> merge(final Optional<RyaStatement> parent, Optional<RyaStatement> child);
}
