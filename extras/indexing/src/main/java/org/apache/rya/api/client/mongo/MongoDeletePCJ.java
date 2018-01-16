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

import java.util.Optional;

import org.apache.rya.api.client.DeletePCJ;
import org.apache.rya.api.client.InstanceDoesNotExistException;
import org.apache.rya.api.client.InstanceExists;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage.PCJStorageException;
import org.apache.rya.indexing.pcj.storage.mongo.MongoPcjStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

/**
 * A Mongo implementation of {@link DeletePCJ}.
 */
public class MongoDeletePCJ implements DeletePCJ {
    private static final Logger log = LoggerFactory.getLogger(MongoDeletePCJ.class);

    private final MongoConnectionDetails connectionDetails;
    private final InstanceExists instanceExists;

    /**
     * Constructs an instance of {@link MongoDeletePCJ}.
     *
     * @param connectionDetails - Details to connect to the server. (not null)
     * @param instanceExists - The interactor used to check if a Rya instance exists. (not null)
     */
    public MongoDeletePCJ(
            final MongoConnectionDetails connectionDetails,
            final MongoInstanceExists instanceExists) {
        this.connectionDetails = requireNonNull(connectionDetails);
        this.instanceExists = requireNonNull(instanceExists);
    }

    @Override
    public void deletePCJ(final String ryaInstanceName, final String pcjId) throws InstanceDoesNotExistException, RyaClientException {
        requireNonNull(ryaInstanceName);
        requireNonNull(pcjId);

        // Ensure the Rya Instance exists.
        if (!instanceExists.exists(ryaInstanceName)) {
            throw new InstanceDoesNotExistException(String.format("There is no Rya instance named '%s'.", ryaInstanceName));
        }

        final Optional<String> username = connectionDetails.getUsername();
        final MongoCredential cred;
        final MongoClient client;

        final ServerAddress addr = new ServerAddress(connectionDetails.getHostname(), connectionDetails.getPort());
        if(username.isPresent()) {
            cred = MongoCredential.createCredential(username.get(), ryaInstanceName, connectionDetails.getPassword().get());
            client = new MongoClient(addr, Lists.newArrayList(cred));
        } else {
            client = new MongoClient(addr);
        }


        //pcjStore doesn't need to be closed since it doesn't need to release any resources.
        @SuppressWarnings("resource")
        final MongoPcjStorage pcjStore = new MongoPcjStorage(client, ryaInstanceName);
        try {
            pcjStore.dropPcj(pcjId);
        } catch (final PCJStorageException e) {
            throw new RyaClientException("Unable to drop PCJ : " + pcjId, e);
        } finally {
            client.close();
        }
    }

}
