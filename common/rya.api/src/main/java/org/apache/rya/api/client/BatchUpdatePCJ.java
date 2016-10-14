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
package org.apache.rya.api.client;

import javax.annotation.ParametersAreNonnullByDefault;

/**
 * Batch update a PCJ index.
 */
@ParametersAreNonnullByDefault
public interface BatchUpdatePCJ {

    /**
     * Batch update a specific PCJ index using the {@link Statement}s that are
     * currently in the Rya instance.
     *
     * @param ryaInstanceName - The Rya instance whose PCJ will be updated. (not null)
     * @param pcjId - Identifies the PCJ index to update. (not null)
     * @throws InstanceDoesNotExistException No instance of Rya exists for the provided name.
     * @throws PCJDoesNotExistException No PCJ exists for the provided PCJ ID.
     * @throws RyaClientException Something caused the command to fail.
     */
    public void batchUpdate(String ryaInstanceName, String pcjId) throws InstanceDoesNotExistException, PCJDoesNotExistException, RyaClientException;
}