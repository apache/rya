/**
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
package org.apache.rya.kafka.connect.api.sink;

import static java.util.Objects.requireNonNull;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.sail.Sail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jcabi.manifests.Manifests;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Handles the common components required to write {@link Statement}s to Rya.
 * <p/>
 * Implementations of this class only need to specify functionality that is specific to the
 * Rya implementation.
 */
@DefaultAnnotation(NonNull.class)
public abstract class RyaSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(RyaSinkTask.class);

    @Nullable
    private SailRepository sailRepo = null;

    @Nullable
    private SailRepositoryConnection conn = null;

    /**
     * Throws an exception if the configured Rya Instance is not already installed
     * within the configured database.
     *
     * @param taskConfig - The configuration values that were provided to the task. (not null)
     * @throws ConnectException The configured Rya Instance is not installed to the configured database
     *   or we were unable to figure out if it is installed.
     */
    protected abstract void checkRyaInstanceExists(final Map<String, String> taskConfig) throws ConnectException;

    /**
     * Creates an initialized {@link Sail} object that may be used to write {@link Statement}s to the configured
     * Rya Instance.
     *
     * @param taskConfig - Configures how the Sail object will be created. (not null)
     * @return The created Sail object.
     * @throws ConnectException The Sail object could not be made.
     */
    protected abstract Sail makeSail(final Map<String, String> taskConfig) throws ConnectException;

    @Override
    public String version() {
        return Manifests.exists("Build-Version") ? Manifests.read("Build-Version"): "UNKNOWN";
    }

    @Override
    public void start(final Map<String, String> props) throws ConnectException {
        requireNonNull(props);

        // Ensure the configured Rya Instance is installed within the configured database.
        checkRyaInstanceExists(props);

        // Create the Sail object that is connected to the Rya Instance.
        final Sail sail = makeSail(props);
        sailRepo = new SailRepository( sail );
        conn = sailRepo.getConnection();
    }

    @Override
    public void put(final Collection<SinkRecord> records) {
        requireNonNull(records);

        // Return immediately if there are no records to handle.
        if(records.isEmpty()) {
            return;
        }

        // If a transaction has not been started yet, then start one.
        if(!conn.isActive()) {
            conn.begin();
        }

        // Iterate through the records and write them to the Sail object.
        for(final SinkRecord record : records) {
            // If everything has been configured correctly, then the record's value will be a Set<Statement>.
            conn.add((Set<? extends Statement>) record.value());
        }
    }

    @Override
    public void flush(final Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        requireNonNull(currentOffsets);
        // Flush the current transaction.
        conn.commit();
    }

    @Override
    public void stop() {
        try {
            if(conn != null) {
                conn.close();
            }
        } catch(final Exception e) {
            log.error("Could not close the Sail Repository Connection.", e);
        }

        try {
            if(sailRepo != null) {
                sailRepo.shutDown();
            }
        } catch(final Exception e) {
            log.error("Could not shut down the Sail Repository.", e);
        }
    }
}