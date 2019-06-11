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

import org.apache.rya.export.api.metadata.MergeParentMetadata;
import org.bson.Document;
import org.junit.Test;

public class ParentMetadataRepositoryAdapterTest {
    private final static String TEST_INSTANCE = "test_instance";
    private final static Date TEST_TIMESTAMP = new Date(8675309L);
    private final static Date TEST_FILTER_TIMESTAMP = new Date(1234567L);
    private final static long TEST_TIME_OFFSET = 123L;
    private final ParentMetadataRepositoryAdapter adapter = new ParentMetadataRepositoryAdapter();

    @Test
    public void deserializeTest() {
        final Document doc = new Document()
            .append(RYANAME_KEY, TEST_INSTANCE)
            .append(TIMESTAMP_KEY, TEST_TIMESTAMP)
            .append(FILTER_TIMESTAMP_KEY, TEST_FILTER_TIMESTAMP)
            .append(PARENT_TIME_OFFSET_KEY, TEST_TIME_OFFSET);

        final MergeParentMetadata expected = new MergeParentMetadata.Builder()
            .setRyaInstanceName(TEST_INSTANCE)
            .setTimestamp(TEST_TIMESTAMP)
            .setFilterTimestmap(TEST_FILTER_TIMESTAMP)
            .setParentTimeOffset(TEST_TIME_OFFSET)
            .build();
        final MergeParentMetadata actual = adapter.deserialize(doc);
        assertEquals(expected, actual);
    }

    @Test(expected=NullPointerException.class)
    public void deserializeTest_missingTime() {
        final Document doc = new Document(RYANAME_KEY, TEST_INSTANCE);
        adapter.deserialize(doc);
    }

    @Test(expected=NullPointerException.class)
    public void deserializeTest_missingName() {
        final Document doc = new Document(TIMESTAMP_KEY, TEST_TIMESTAMP);
        adapter.deserialize(doc);
    }

    @Test
    public void serializeTest() {
        final MergeParentMetadata merge = new MergeParentMetadata.Builder()
            .setRyaInstanceName(TEST_INSTANCE)
            .setTimestamp(TEST_TIMESTAMP)
            .setFilterTimestmap(TEST_FILTER_TIMESTAMP)
            .setParentTimeOffset(TEST_TIME_OFFSET)
            .build();

        final Document expected = new Document()
            .append(RYANAME_KEY, TEST_INSTANCE)
            .append(TIMESTAMP_KEY, TEST_TIMESTAMP)
            .append(FILTER_TIMESTAMP_KEY, TEST_FILTER_TIMESTAMP)
            .append(PARENT_TIME_OFFSET_KEY, TEST_TIME_OFFSET);
        final Document actual = adapter.serialize(merge);
        assertEquals(expected, actual);
    }
}
