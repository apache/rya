package org.apache.rya.export.api.store;

public class AddStatementException extends StatementStoreException {
    private static final long serialVersionUID = 1L;

    public AddStatementException(final String message) {
        super(message);
    }

    public AddStatementException(final String message, final Throwable cause) {
        super(message, cause);
    }

}
