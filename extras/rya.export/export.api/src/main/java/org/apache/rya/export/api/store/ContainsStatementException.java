package org.apache.rya.export.api.store;

public class ContainsStatementException extends StatementStoreException {
    private static final long serialVersionUID = 1L;

    public ContainsStatementException(final String message) {
        super(message);
    }

    public ContainsStatementException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
