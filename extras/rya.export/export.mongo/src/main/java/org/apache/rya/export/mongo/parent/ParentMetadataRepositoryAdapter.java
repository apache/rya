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

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;

/**
 * Adapter for converting {@link MergeParentMetadata} to and from mongo
 * {@link DBObject}s.
 */
public class ParentMetadataRepositoryAdapter {
    public static final String RYANAME_KEY = "ryaInstanceName";
    public static final String TIMESTAMP_KEY = "timestamp";
    public static final String FILTER_TIMESTAMP_KEY = "filterTimestamp";
    public static final String PARENT_TIME_OFFSET_KEY = "parentTimeOffset";

    /**
     * Serializes the {@link MergeParentMetadata} into a mongoDB object.
     * @param metadata - The {@link MergeParentMetadata} to serialize.
     * @return The MongoDB object
     */
    public DBObject serialize(final MergeParentMetadata metadata) {
        final BasicDBObjectBuilder builder = BasicDBObjectBuilder.start()
            .add(RYANAME_KEY, metadata.getRyaInstanceName())
            .add(TIMESTAMP_KEY, metadata.getTimestamp())
            .add(FILTER_TIMESTAMP_KEY, metadata.getFilterTimestamp())
            .add(PARENT_TIME_OFFSET_KEY, metadata.getParentTimeOffset());
        return builder.get();
    }

    /**
     * Deserialize the mongoBD object into {@link MergeParentMetadata}.
     * @param dbo - The mongo {@link DBObject} to deserialize.
     * @return The {@link MergeParentMetadata}
     */
    public MergeParentMetadata deserialize(final DBObject dbo) {
        final Date timestamp = (Date) dbo.get(TIMESTAMP_KEY);
        final String ryaInstance = (String) dbo.get(RYANAME_KEY);
        final Date filterTimestamp = (Date) dbo.get(FILTER_TIMESTAMP_KEY);
        final Long offset = (Long) dbo.get(PARENT_TIME_OFFSET_KEY);
        return new MergeParentMetadata.Builder()
            .setRyaInstanceName(ryaInstance)
            .setTimestamp(timestamp)
            .setFilterTimestmap(filterTimestamp)
            .setParentTimeOffset(offset)
            .build();
    }
}
