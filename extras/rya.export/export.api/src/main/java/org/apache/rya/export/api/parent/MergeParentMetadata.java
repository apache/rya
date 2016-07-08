package org.apache.rya.export.api.parent;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Date; 

/** 
 * The parent database identifying information.  Use the {@link ParentMetadataRepository}
 * to retrieve this information
 */
public class MergeParentMetadata {
    private final String ryaInstanceName;
    private final Date timestamp;

    /**
     * Creates a new {@link MergeParentMetadata}.
     * @param ryaInstanceName - The Rya Instance Name of the parent database.
     * @param timestamp - The timestamp used to export triples.
     */
    private MergeParentMetadata(final String ryaInstanceName, final Date timestamp) {
        this.ryaInstanceName = checkNotNull(ryaInstanceName);
        this.timestamp = checkNotNull(timestamp);
    }

    /**
     * @return - The Rya Instance Name of the parent database.
     */
    public String getRyaInstanceName() {
        return ryaInstanceName;
    }

    /**
     * @return - The timestamp used when exporting triples.
     */
    public Date getTimestamp() {
        return timestamp;
    }

    public static class Builder {
        private String name;
        private Date timestamp;

        public Builder setRyaInstanceName(final String name) {
            this.name = checkNotNull(name);
            return this;
        }

        public Builder setTimestamp(final Date timestamp) {
            this.timestamp = checkNotNull(timestamp);
            return this;
        }

        public MergeParentMetadata build() {
            return new MergeParentMetadata(name, timestamp);
        }
    }
}