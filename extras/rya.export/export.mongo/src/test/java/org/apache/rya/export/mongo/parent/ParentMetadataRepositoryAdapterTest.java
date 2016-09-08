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

import static org.apache.rya.export.mongo.parent.ParentMetadataRepositoryAdapter.FILTER_TIMESTAMP_KEY;
import static org.apache.rya.export.mongo.parent.ParentMetadataRepositoryAdapter.PARENT_TIME_OFFSET_KEY;
import static org.apache.rya.export.mongo.parent.ParentMetadataRepositoryAdapter.RYANAME_KEY;
import static org.apache.rya.export.mongo.parent.ParentMetadataRepositoryAdapter.TIMESTAMP_KEY;
import static org.junit.Assert.assertEquals;

import java.util.Date;

import org.apache.rya.export.api.parent.MergeParentMetadata;
import org.junit.Test;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;

public class ParentMetadataRepositoryAdapterTest {
    private final static String TEST_INSTANCE = "test_instance";
    private final static Date TEST_TIMESTAMP = new Date(8675309L);
    private final static Date TEST_FILTER_TIMESTAMP = new Date(1234567L);
    private final static long TEST_TIME_OFFSET = 123L;
    private final ParentMetadataRepositoryAdapter adapter = new ParentMetadataRepositoryAdapter();

    @Test
    public void deserializeTest() {
        final DBObject dbo = BasicDBObjectBuilder.start()
            .add(RYANAME_KEY, TEST_INSTANCE)
            .add(TIMESTAMP_KEY, TEST_TIMESTAMP)
            .add(FILTER_TIMESTAMP_KEY, TEST_FILTER_TIMESTAMP)
            .add(PARENT_TIME_OFFSET_KEY, TEST_TIME_OFFSET)
            .get();

        final MergeParentMetadata expected = new MergeParentMetadata.Builder()
            .setRyaInstanceName(TEST_INSTANCE)
            .setTimestamp(TEST_TIMESTAMP)
            .setFilterTimestmap(TEST_FILTER_TIMESTAMP)
            .setParentTimeOffset(TEST_TIME_OFFSET)
            .build();
        final MergeParentMetadata actual = adapter.deserialize(dbo);
        assertEquals(expected, actual);
    }

    @Test(expected=NullPointerException.class)
    public void deserializeTest_missingTime() {
        final DBObject dbo = BasicDBObjectBuilder.start()
            .add(RYANAME_KEY, TEST_INSTANCE)
            .get();
        adapter.deserialize(dbo);
    }

    @Test(expected=NullPointerException.class)
    public void deserializeTest_missingName() {
        final DBObject dbo = BasicDBObjectBuilder.start()
            .add(TIMESTAMP_KEY, TEST_TIMESTAMP)
            .get();
        adapter.deserialize(dbo);
    }

    @Test
    public void serializeTest() {
        final MergeParentMetadata merge = new MergeParentMetadata.Builder()
            .setRyaInstanceName(TEST_INSTANCE)
            .setTimestamp(TEST_TIMESTAMP)
            .setFilterTimestmap(TEST_FILTER_TIMESTAMP)
            .setParentTimeOffset(TEST_TIME_OFFSET)
            .build();

        final DBObject expected = BasicDBObjectBuilder.start()
            .add(RYANAME_KEY, TEST_INSTANCE)
            .add(TIMESTAMP_KEY, TEST_TIMESTAMP)
            .add(FILTER_TIMESTAMP_KEY, TEST_FILTER_TIMESTAMP)
            .add(PARENT_TIME_OFFSET_KEY, TEST_TIME_OFFSET)
            .get();
        final DBObject actual = adapter.serialize(merge);
        assertEquals(expected, actual);
    }
}
