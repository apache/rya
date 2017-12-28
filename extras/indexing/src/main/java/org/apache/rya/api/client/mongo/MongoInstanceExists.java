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

import org.apache.rya.api.client.InstanceExists;

import com.mongodb.MongoClient;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * An Mongo implementation of the {@link InstanceExists} command.
 */
@DefaultAnnotation(NonNull.class)
public class MongoInstanceExists implements InstanceExists {

    private final MongoClient adminClient;

    /**
     * Constructs an insatnce of {@link MongoInstanceExists}.
     *
     * @param adminClient - Provides programmatic access to the instance of Mongo that hosts Rya instances. (not null)
     */
    public MongoInstanceExists(final MongoClient adminClient) {
        this.adminClient = requireNonNull(adminClient);
    }

    @Override
    public boolean exists(final String instanceName) {
        requireNonNull( instanceName );
        for(final String dbName : adminClient.listDatabaseNames()) {
            if(dbName.equals(instanceName)) {
                return true;
            }
        }
        return false;
    }
}