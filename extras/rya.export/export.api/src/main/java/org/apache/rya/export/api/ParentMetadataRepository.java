package org.apache.rya.export.api;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Date;

/**
 * Repository for metadata pertaining to the parent database.  This will contain
 * all information to identify where any data was exported from.
 * <p>
 * The data found here is:
 * <li>Parent database Rya Instance Name</li>
 * <li>Timestamp used as the lower cutoff for the export</li>
 */
public interface ParentMetadataRepository {
    /**
     * @return The metadata for identifying the parent.
     */
    public MergeParentMetadata get();

    /**
     * @param metadata - The identifying metadata for the parent.
     */
    public void set(MergeParentMetadata metadata);

    /**
     * The parent database identifying information.  Use the {@link ParentMetadataRepository}
     * to retrieve this information
     */
    public class MergeParentMetadata {
        private final String ryaInstanceName;
        private final Date timestamp;

        private Date copyToolInputTime;
        private Date copyToolRunTime;

        private Long parentTimeOffset = 0L;
        private Long childTimeOffset = 0L;

        private boolean useTimeSync = false;
        private boolean useMergeFileInput = false;
        private boolean usesStartTime = false;

        /**
         * Creates a new {@link MergeParentMetadata}.
         * @param ryaInstanceName - The Rya Instance Name of the parent database.
         * @param timestamp - The timestamp used to export triples.
         */
        public MergeParentMetadata(final String ryaInstanceName, final Date timestamp) {
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

        public Date getCopyToolInputTime() {
            return copyToolInputTime;
        }

        public Date getCopyToolRunTime() {
            return copyToolRunTime;
        }

        public Long getParentTimeOffset() {
            return parentTimeOffset;
        }

        public Long getChildTimeOffset() {
            return childTimeOffset;
        }

        public boolean isUseTimeSync() {
            return useTimeSync;
        }

        public boolean isUseMergeFileInput() {
            return useMergeFileInput;
        }

        public boolean isUsesStartTime() {
            return usesStartTime;
        }
    }
}
