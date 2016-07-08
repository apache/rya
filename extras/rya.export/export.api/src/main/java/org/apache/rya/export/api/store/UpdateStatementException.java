package org.apache.rya.export.api.store;

public class UpdateStatementException extends StatementStoreException {
    private static final long serialVersionUID = 1L;

    public UpdateStatementException(final String message) {
        super(message);
    }

    public UpdateStatementException(final String message, final Throwable cause) {
        super(message, cause);
    }

}
