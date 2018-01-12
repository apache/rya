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

import java.util.Set;

import org.apache.rya.api.client.CreatePCJ;
import org.apache.rya.api.client.InstanceDoesNotExistException;
import org.apache.rya.api.client.InstanceExists;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage.PCJStorageException;
import org.apache.rya.indexing.pcj.storage.mongo.MongoPcjStorage;

import com.google.common.collect.Sets;
import com.mongodb.MongoClient;

/**
 * A Mongo implementation of {@link CreatePCJ}.
 */
public class MongoCreatePCJ implements CreatePCJ {
    private final InstanceExists instanceExists;
    private final MongoClient mongoClient;

    /**
     * Constructs an instance of {@link MongoCreatePCJ}.
     *
     * @param mongoClient - The {@link MongoClient} used to create a new PCJ. (not null)
     * @param instanceExists - The interactor used to check if a Rya instance exists. (not null)
     */
    public MongoCreatePCJ(
            final MongoClient mongoClient,
            final MongoInstanceExists instanceExists) {
        this.mongoClient = requireNonNull(mongoClient);
        this.instanceExists = requireNonNull(instanceExists);
    }

    @Override
    public String createPCJ(final String ryaInstanceName, final String sparql, final Set<ExportStrategy> strategies) throws InstanceDoesNotExistException, RyaClientException {
        requireNonNull(ryaInstanceName);
        requireNonNull(sparql);

        // Ensure the Rya Instance exists.
        if (!instanceExists.exists(ryaInstanceName)) {
            throw new InstanceDoesNotExistException(String.format("There is no Rya instance named '%s'.", ryaInstanceName));
        }

        try(final MongoPcjStorage pcjStore = new MongoPcjStorage(mongoClient, ryaInstanceName)) {
        	return pcjStore.createPcj(sparql);
        } catch (final PCJStorageException e) {
            throw new RyaClientException("Unable to create PCJ for: " + sparql, e);
        }
    }

    @Override
    public String createPCJ(final String instanceName, final String sparql) throws InstanceDoesNotExistException, RyaClientException {
        return createPCJ(instanceName, sparql, Sets.newHashSet(ExportStrategy.RYA));
    }

}
