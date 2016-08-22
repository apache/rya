package org.apache.rya.export.api.parent;

/**
 * Thrown when the {@link ParentMetadataRepository} attempts to set the
 * {@link MergeParentMetadata} and it already exists.
 */
public class ParentMetadataExistsException extends ParentMetadataException {
    private static final long serialVersionUID = 1L;

    public ParentMetadataExistsException(final String message) {
        super(message);
    }

    public ParentMetadataExistsException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
