package org.apache.rya.export.api.parent;

/**
 * Thrown when the {@link ParentMetadataRepository} attempts to fetch
 * the {@link MergeParentMetadata} and it does not exist.   /
 */
public class ParentMetadataDoesNotExistException extends ParentMetadataException {
    private static final long serialVersionUID = 1L;

    public ParentMetadataDoesNotExistException(final String message) {
        super(message);
    }

    public ParentMetadataDoesNotExistException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
