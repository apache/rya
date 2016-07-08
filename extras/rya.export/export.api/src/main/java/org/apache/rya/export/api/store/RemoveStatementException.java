package org.apache.rya.export.api.store;

public class RemoveStatementException extends StatementStoreException {
    private static final long serialVersionUID = 1L;

    public RemoveStatementException(final String message) {
        super(message);
    }

    public RemoveStatementException(final String message, final Throwable cause) {
        super(message, cause);
    }

}
