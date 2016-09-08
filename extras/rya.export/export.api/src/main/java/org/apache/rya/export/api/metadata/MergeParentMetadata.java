/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.export.api.metadata;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Date;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

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

    @Override
    public boolean equals(final Object obj) {
        if(!(obj instanceof MergeParentMetadata)) {
            return false;
        }
        final MergeParentMetadata other = (MergeParentMetadata) obj;
        final EqualsBuilder builder = new EqualsBuilder()
            .append(getRyaInstanceName(), other.getRyaInstanceName())
            .append(getTimestamp(), other.getTimestamp())
            .append(getFilterTimestamp(), other.getFilterTimestamp())
            .append(getParentTimeOffset(), other.getParentTimeOffset());
        return builder.isEquals();
    }

    @Override
    public int hashCode() {
        final HashCodeBuilder builder = new HashCodeBuilder()
            .append(getRyaInstanceName())
            .append(getTimestamp())
            .append(getFilterTimestamp())
            .append(getParentTimeOffset());
        return builder.toHashCode();
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

        public Builder setParentTimeOffset(final Long parentTimeOffset) {
            this.parentTimeOffset = parentTimeOffset;
            return this;
        }

        public MergeParentMetadata build() {
            return new MergeParentMetadata(name, timestamp, filterTimestamp, parentTimeOffset);
        }
    }
}