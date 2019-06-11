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
package org.apache.rya.export.mongo.parent;

import java.util.Date;

import org.apache.rya.export.api.metadata.MergeParentMetadata;
import org.bson.Document;

/**
 * Adapter for converting {@link MergeParentMetadata} to and from mongo
 * {@link Document}s.
 */
public class ParentMetadataRepositoryAdapter {
    public static final String RYANAME_KEY = "ryaInstanceName";
    public static final String TIMESTAMP_KEY = "timestamp";
    public static final String FILTER_TIMESTAMP_KEY = "filterTimestamp";
    public static final String PARENT_TIME_OFFSET_KEY = "parentTimeOffset";

    /**
     * Serializes the {@link MergeParentMetadata} into a mongoDB document.
     * @param metadata - The {@link MergeParentMetadata} to serialize.
     * @return The MongoDB {@link Document}
     */
    public Document serialize(final MergeParentMetadata metadata) {
        final Document doc = new Document()
            .append(RYANAME_KEY, metadata.getRyaInstanceName())
            .append(TIMESTAMP_KEY, metadata.getTimestamp())
            .append(FILTER_TIMESTAMP_KEY, metadata.getFilterTimestamp())
            .append(PARENT_TIME_OFFSET_KEY, metadata.getParentTimeOffset());
        return doc;
    }

    /**
     * Deserialize the mongoBD document into {@link MergeParentMetadata}.
     * @param doc - The mongo {@link Document} to deserialize.
     * @return The {@link MergeParentMetadata}
     */
    public MergeParentMetadata deserialize(final Document doc) {
        final Date timestamp = (Date) doc.get(TIMESTAMP_KEY);
        final String ryaInstance = (String) doc.get(RYANAME_KEY);
        final Date filterTimestamp = (Date) doc.get(FILTER_TIMESTAMP_KEY);
        final Long offset = (Long) doc.get(PARENT_TIME_OFFSET_KEY);
        return new MergeParentMetadata.Builder()
            .setRyaInstanceName(ryaInstance)
            .setTimestamp(timestamp)
            .setFilterTimestmap(filterTimestamp)
            .setParentTimeOffset(offset)
            .build();
    }
}
