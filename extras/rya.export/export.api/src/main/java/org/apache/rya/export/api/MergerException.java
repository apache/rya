package org.apache.rya.export.api;

/**
 * An exception to be used when there is a problem running the Merge Tool.
 */
public class MergerException extends Exception {
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance of {@link MergerException}.
     */
    public MergerException() {
        super();
    }

    /**
     * Creates a new instance of {@link MergerException}.
     * @param message the detail message.
     */
    public MergerException(final String message) {
        super(message);
    }

    /**
     * Creates a new instance of {@link MergerException}.
     * @param message the detail message.
     * @param throwable the {@link Throwable} source.
     */
    public MergerException(final String message, final Throwable source) {
        super(message, source);
    }

    /**
     * Creates a new instance of {@link MergerException}.
     * @param source the {@link Throwable} source.
     */
    public MergerException(Throwable source) {
        super(source);
    }
}