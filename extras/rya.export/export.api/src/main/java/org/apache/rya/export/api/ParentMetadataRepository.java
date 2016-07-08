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

        private final Long parentTimeOffset = 0L;

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
    }
}
