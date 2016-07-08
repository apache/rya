package org.apache.rya.export.api.store;

public class FetchStatementException extends StatementStoreException {
    private static final long serialVersionUID = 1L;

    public FetchStatementException(final String message) {
        super(message);
    }

    public FetchStatementException(final String message, final Throwable cause) {
        super(message, cause);
    }

}
