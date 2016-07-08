package org.apache.rya.export.api.conf;

/**
 * An exception to be used when there is a problem configuring the Merge Tool.
 */
public class MergeConfigurationException extends Exception {
    private static final long serialVersionUID = 1L;

    public MergeConfigurationException(final String message) {
        super(message);
    }

    public MergeConfigurationException(final String message, final Throwable source) {
        super(message, source);
    }
}
