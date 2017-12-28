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
package org.apache.rya.api.client.mongo;

import static java.util.Objects.requireNonNull;

import org.apache.rya.api.client.GetInstanceDetails;
import org.apache.rya.api.client.InstanceDoesNotExistException;
import org.apache.rya.api.client.InstanceExists;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.api.instance.RyaDetails;
import org.apache.rya.api.instance.RyaDetailsRepository;
import org.apache.rya.api.instance.RyaDetailsRepository.NotInitializedException;
import org.apache.rya.api.instance.RyaDetailsRepository.RyaDetailsRepositoryException;
import org.apache.rya.mongodb.instance.MongoRyaInstanceDetailsRepository;

import com.google.common.base.Optional;
import com.mongodb.MongoClient;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * An Mongo implementation of the {@link GetInstanceDetails} command.
 */
@DefaultAnnotation(NonNull.class)
public class MongoGetInstanceDetails implements GetInstanceDetails {

    private final MongoClient adminClient;
    private final InstanceExists instanceExists;

    /**
     * Constructs an instance of {@link MongoGetInstanceDetails}.
     *
     * @param adminClient - Provides programmatic access to the instance of Mongo that hosts Rya instances. (not null)
     * @param instanceExists - The interactor used to check if a Rya instance exists. (not null)
     */
    public MongoGetInstanceDetails(final MongoClient adminClient, MongoInstanceExists instanceExists) {
        this.adminClient = requireNonNull(adminClient);
        this.instanceExists = requireNonNull(instanceExists);
    }

    @Override
    public Optional<RyaDetails> getDetails(final String ryaInstanceName) throws InstanceDoesNotExistException, RyaClientException {
        requireNonNull(ryaInstanceName);

        // Ensure the Rya instance exists.
        if (!instanceExists.exists(ryaInstanceName)) {
            throw new InstanceDoesNotExistException(String.format("There is no Rya instance named '%s'.", ryaInstanceName));
        }

        // If the instance has details, then return them.
        final RyaDetailsRepository detailsRepo = new MongoRyaInstanceDetailsRepository(adminClient, ryaInstanceName);
        try {
            return Optional.of(detailsRepo.getRyaInstanceDetails());
        } catch (final NotInitializedException e) {
            return Optional.absent();
        } catch (final RyaDetailsRepositoryException e) {
            throw new RyaClientException("Could not fetch the Rya instance's details.", e);
        }
    }
}