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

import org.apache.rya.api.instance.RyaDetails.RyaStreamsDetails;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Update which Rya Streams subsystem a Rya instance is connected to.
 */
@DefaultAnnotation(NonNull.class)
public interface SetRyaStreamsConfiguration {

    /**
     * Update which Rya Streams subsystem a Rya instance is connected to.
     *
     * @param instanceName - Indicates which Rya instance will have a Rya Streams subsystem assigned to it. (not null)
     * @param streamsDetails - Indicates which Rya Streams subsystem the instance will use. (not null)
     * @throws InstanceDoesNotExistException No instance of Rya exists for the provided name.
     * @throws RyaClientException Something caused the command to fail.
     */
    public void setRyaStreamsConfiguration(String ryaInstance, RyaStreamsDetails streamsDetails) throws InstanceDoesNotExistException, RyaClientException;
}