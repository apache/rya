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
package org.apache.rya.export.api.policy;

import org.apache.rya.export.api.store.RyaStatementStore;
import org.apache.rya.export.api.store.RyaStatementStorePolicy;

/**
 * Statement Store decorated to fetch statements based on a timestamp.
 */
public class TimestampPolicyStatementStore extends RyaStatementStorePolicy {

    protected final long timestamp;

    /**
     * Creates a new {@link TimestampPolicyStatementStore}
     * @param store - The {@link RyaStatementStore} to decorate
     * @param timestamp - The timestamp from which all parent statements will be merged into the child.
     */
    public TimestampPolicyStatementStore(final RyaStatementStore store, final long timestamp) {
        super(store);

        this.timestamp = timestamp;
    }
}
