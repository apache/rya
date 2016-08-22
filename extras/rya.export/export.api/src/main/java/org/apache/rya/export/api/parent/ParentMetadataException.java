package org.apache.rya.export.api.parent;

import org.apache.rya.export.api.MergerException;

/**
 * Thrown when the {@link ParentMetadataRepository} attempts to fetch
 * the {@link MergeParentMetadata} and it does not exist.
 */
class ParentMetadataException extends MergerException {
    private static final long serialVersionUID = 1L;

    public ParentMetadataException(final String message) {
        super(message);
    }

    public ParentMetadataException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
