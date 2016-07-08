package org.apache.rya.export.api.store;

/**
 * Thrown when an exception occurs in the {@link RyaStatementStore}.
 */
class StatementStoreException extends Exception {
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new {@link StatementStoreException} with a message.
     * @param message The error message.
     */
    public StatementStoreException(final String message) {
        super(message);
    }

    /**
     * Creates a new {@link StatementStoreException} with a message and cause.
     * @param message The error message.
     * @param cause The cause of this exception.
     */
    public StatementStoreException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
