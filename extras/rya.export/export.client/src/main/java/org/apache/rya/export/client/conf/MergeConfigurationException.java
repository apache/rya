package org.apache.rya.export.client.conf;

/**
 * Exception thrown when a problem occurs configuratin the merge tool.
 */
public class MergeConfigurationException extends Exception {
    private static final long serialVersionUID = 1L;

    /**
     * @param message - The error message.
     */
    public MergeConfigurationException(final String message) {
        super(message);
    }

    /**
     * @param cause - The cause of the exception
     */
    public MergeConfigurationException(final Throwable cause) {
        super(cause);
    }

    /**
     * @param message - The error message.
     * @param cause - The cause of the exception
     */
    public MergeConfigurationException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
