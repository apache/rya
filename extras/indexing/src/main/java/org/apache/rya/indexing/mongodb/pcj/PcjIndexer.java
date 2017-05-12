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
package org.apache.rya.indexing.mongodb.pcj;

import org.apache.hadoop.conf.Configuration;
import org.apache.rya.api.persist.index.RyaSecondaryIndexer;
import org.apache.rya.indexing.pcj.storage.mongo.MongoPcjDocuments;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Updates the PCJs that are in a {@link MongoPcjDocuments}.
 */
public interface PcjIndexer extends RyaSecondaryIndexer {
    /**
     * Creates the {@link MongoPcjDocuments} that will be used by the indexer.
     *
     * @param conf - Indicates how the {@link MongoPcjDocuments} is initialized. (not null)
     * @return The {@link MongoPcjDocuments} that will be used by this indexer.
     */
    public @Nullable MongoPcjDocuments getPcjStorage(Configuration conf);
}
