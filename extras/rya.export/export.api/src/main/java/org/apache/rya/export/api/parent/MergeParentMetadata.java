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
    private final Date filterTimestamp;
    private final Long parentTimeOffset;

    /**
     * Creates a new {@link MergeParentMetadata}.
     * @param ryaInstanceName - The Rya Instance Name of the parent database.
     * @param timestamp - The timestamp of when the copy tool ran.
     * @param filterTimestamp - The timestamp used by the copy tool to filter
     * which data was included when copying.
     * @param parentTimeOffset - The parent time offset metadata key for the
     * table.
     */
    public MergeParentMetadata(final String ryaInstanceName, final Date timestamp, final Date filterTimestamp, final Long parentTimeOffset) {
        this.ryaInstanceName = checkNotNull(ryaInstanceName);
        this.timestamp = checkNotNull(timestamp);
        this.filterTimestamp = filterTimestamp;
        this.parentTimeOffset = checkNotNull(parentTimeOffset);
    }

    /**
     * @return - The Rya Instance Name of the parent database.
     */
    public String getRyaInstanceName() {
        return ryaInstanceName;
    }

    /**
     * @return - The timestamp of when the copy tool ran.
     */
    public Date getTimestamp() {
        return timestamp;
    }

    /**
     * @return - The timestamp used by the copy tool to filter which data was
     * included when copying.
     */
    public Date getFilterTimestamp() {
        return filterTimestamp;
    }

    /**
     * @return - The parent time offset metadata key for the table.
     */
    public Long getParentTimeOffset() {
        return parentTimeOffset;
    }

    public static class Builder {
        private String name;
        private Date timestamp;
        private Date filterTimestamp;
        private Long parentTimeOffset;

        public Builder setRyaInstanceName(final String name) {
            this.name = checkNotNull(name);
            return this;
        }

        public Builder setTimestamp(final Date timestamp) {
            this.timestamp = checkNotNull(timestamp);
            return this;
        }

        public Builder setFilterTimestmap(final Date filterTimestamp) {
            this.filterTimestamp = checkNotNull(filterTimestamp);
            return this;
        }

        public Builder getParentTimeOffset(final Long parentTimeOffset) {
            this.parentTimeOffset = parentTimeOffset;
            return this;
        }

        public MergeParentMetadata build() {
            return new MergeParentMetadata(name, timestamp, filterTimestamp, parentTimeOffset);
        }
    }
}