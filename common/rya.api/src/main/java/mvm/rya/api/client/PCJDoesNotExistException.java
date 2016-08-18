package mvm.rya.api.client;

import javax.annotation.ParametersAreNonnullByDefault;

/**
 * One of the {@link RyaClient} commands could not execute because the connected
 * instance of Rya does not have a PCJ matching the provided PCJ ID.
 */
@ParametersAreNonnullByDefault
public class PCJDoesNotExistException extends RyaClientException {
    private static final long serialVersionUID = 1L;

    public PCJDoesNotExistException(final String message) {
        super(message);
    }

    public PCJDoesNotExistException(final String message, final Throwable cause) {
        super(message, cause);
    }
}