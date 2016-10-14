/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import com.google.common.base.Optional;

import org.apache.rya.api.instance.RyaDetails;

/**
 * Get configuration and maintenance information about a specific instance of Rya.
 */
@ParametersAreNonnullByDefault
public interface GetInstanceDetails {

    /**
     * Get configuration and maintenance information about a specific instance of Rya.
     *
     * @param instanceName - Indicates which Rya instance to fetch the details from. (not null)
     * @return The {@link RyaDetails} that describe the instance of Rya. If this is
     *   an older version of Rya, then there may not be any details to fetch. If
     *   this is the case, empty is returned.
     * @throws InstanceDoesNotExistException No instance of Rya exists for the provided name.
     * @throws RyaClientException Something caused the command to fail.
     */
    public Optional<RyaDetails> getDetails(final String instanceName) throws InstanceDoesNotExistException, RyaClientException;
}