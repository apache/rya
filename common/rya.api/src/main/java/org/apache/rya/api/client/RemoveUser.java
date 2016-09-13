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

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Revokes a user's access to an instance of Rya.
 */
@DefaultAnnotation(NonNull.class)
public interface RemoveUser {

    /**
     * Revokes access to a specific Rya instance from a user of the storage system.
     *
     * @param instanceName - Indicates which Rya instance will be revoked from the user. (not null)
     * @param username - The user whose access will be revoked. (not null)
     * @throws InstanceDoesNotExistException No instance of Rya exists for the provided name.
     * @throws RyaClientException Something caused the command to fail.
     */
    public void removeUser(String instanceName, String username) throws InstanceDoesNotExistException, RyaClientException;
}