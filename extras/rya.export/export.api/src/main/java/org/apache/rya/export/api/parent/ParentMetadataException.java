package org.apache.rya.export.api.parent;

/**
 * Thrown when the {@link ParentMetadataRepository} attempts to fetch
 * the {@link MergeParentMetadata} and it does not exist.   /
 */
class ParentMetadataException extends Exception {
    private static final long serialVersionUID = 1L;

    public ParentMetadataException(final String message) {
        super(message);
    }

    public ParentMetadataException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
