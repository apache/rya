package org.apache.rya.accumulo.instance;

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

import static java.util.Objects.requireNonNull;

import java.util.Map.Entry;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.ConditionalWriter.Result;
import org.apache.accumulo.core.client.ConditionalWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Condition;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

import org.apache.rya.api.instance.RyaDetails;
import org.apache.rya.api.instance.RyaDetailsRepository;

/**
 * An implementation of {@link RyaDetailsRepository} that stores a Rya
 * instance's {@link RyaDetails} in an Accumulo table.
 * </p>
 * XXX
 * This implementation writes the details object as a serialized byte array to
 * a row in Accumulo. Storing the entire structure within a single value is
 * attractive because Accumulo's conditional writer will let us do checkAndSet
 * style operations to synchronize writes to the object. On the downside, only
 * Java clients will work.
 */
@ParametersAreNonnullByDefault
public class AccumuloRyaInstanceDetailsRepository implements RyaDetailsRepository {

    public static final String INSTANCE_DETAILS_TABLE_NAME = "instance_details";

    private static final Text ROW_ID = new Text("instance metadata");
    private static final Text COL_FAMILY = new Text("instance");
    private static final Text COL_QUALIFIER = new Text("details");

    private final RyaDetailsSerializer serializer = new RyaDetailsSerializer();

    private final Connector connector;
    private final String instanceName;
    private final String detailsTableName;


    /**
     * Constructs an instance of {@link AccumuloRyaInstanceDetailsRepository}.
     *
     * @param connector - Connects to the instance of Accumulo that hosts the Rya instance. (not null)
     * @param instanceName - The name of the Rya instance this repository represents. (not null)
     */
    public AccumuloRyaInstanceDetailsRepository(final Connector connector, final String instanceName) {
        this.connector = requireNonNull( connector );
        this.instanceName = requireNonNull( instanceName );
        this.detailsTableName = instanceName + INSTANCE_DETAILS_TABLE_NAME;
    }

    @Override
    public boolean isInitialized() throws RyaDetailsRepositoryException {
        try {
            final Scanner scanner = connector.createScanner(detailsTableName, new Authorizations());
            scanner.fetchColumn(COL_FAMILY, COL_QUALIFIER);
            return scanner.iterator().hasNext();
        } catch (final TableNotFoundException e) {
            return false;
        }
    }

    @Override
    public void initialize(final RyaDetails details) throws AlreadyInitializedException, RyaDetailsRepositoryException {
        // Preconditions.
        requireNonNull( details );

        if(!details.getRyaInstanceName().equals( instanceName )) {
            throw new RyaDetailsRepositoryException("The instance name that was in the provided 'details' does not match " +
                    "the instance name that this repository is connected to. Make sure you're connected to the" +
                    "correct Rya instance.");
        }

        if(isInitialized()) {
            throw new AlreadyInitializedException("The repository has already been initialized for the Rya instance named '" +
                    instanceName + "'.");
        }

        // Create the table that hosts the details if it has not been created yet.
        final TableOperations tableOps = connector.tableOperations();
        if(!tableOps.exists(detailsTableName)) {
            try {
                tableOps.create(detailsTableName);
            } catch (AccumuloException | AccumuloSecurityException | TableExistsException e) {
                throw new RyaDetailsRepositoryException("Could not initialize the Rya instance details for the instance named '" +
                        instanceName + "' because the the table that holds that information could not be created.");
            }
        }

        // Write the details to the table.
        BatchWriter writer = null;
        try {
            writer = connector.createBatchWriter(detailsTableName, new BatchWriterConfig());

            final byte[] bytes = serializer.serialize(details);
            final Mutation mutation = new Mutation(ROW_ID);
            mutation.put(COL_FAMILY, COL_QUALIFIER, new Value(bytes));
            writer.addMutation( mutation );

        } catch (final TableNotFoundException | MutationsRejectedException e) {
            throw new RyaDetailsRepositoryException("Could not initialize the Rya instance details for the instance named '" + instanceName + "'.", e);
        } finally {
            if(writer != null) {
                try {
                    writer.close();
                } catch (final MutationsRejectedException e) {
                    throw new RyaDetailsRepositoryException("Could not initialize the Rya instance details for the instance named '" + instanceName + "'.", e);
                }
            }
        }
    }

    @Override
    public RyaDetails getRyaInstanceDetails() throws NotInitializedException, RyaDetailsRepositoryException {
        // Preconditions.
        if(!isInitialized()) {
            throw new NotInitializedException("Could not fetch the details for the Rya instanced named '" +
                    instanceName + "' because it has not been initialized yet.");
        }

        // Read it from the table.
        try {
            // Fetch the value from the table.
            final Scanner scanner = connector.createScanner(detailsTableName, new Authorizations());
            scanner.fetchColumn(COL_FAMILY, COL_QUALIFIER);
            final Entry<Key, Value> entry = scanner.iterator().next();

            // Deserialize it.
            final byte[] bytes = entry.getValue().get();
            return serializer.deserialize( bytes );

        } catch (final TableNotFoundException e) {
            throw new RyaDetailsRepositoryException("Could not get the details from the table.", e);
        }
    }

    @Override
    public void update(final RyaDetails oldDetails, final RyaDetails newDetails)
            throws NotInitializedException, ConcurrentUpdateException, RyaDetailsRepositoryException {
        // Preconditions.
        requireNonNull(oldDetails);
        requireNonNull(newDetails);

        if(!newDetails.getRyaInstanceName().equals( instanceName )) {
            throw new RyaDetailsRepositoryException("The instance name that was in the provided 'newDetails' does not match " +
                    "the instance name that this repository is connected to. Make sure you're connected to the" +
                    "correct Rya instance.");
        }

        if(!isInitialized()) {
            throw new NotInitializedException("Could not update the details for the Rya instanced named '" +
                    instanceName + "' because it has not been initialized yet.");
        }

        // Use a conditional writer so that we can detect when the old details
        // are no longer the currently stored ones.
        ConditionalWriter writer = null;
        try {
            // Setup the condition that ensures the details have not changed since the edits were made.
            final byte[] oldDetailsBytes = serializer.serialize(oldDetails);
            final Condition condition = new Condition(COL_FAMILY, COL_QUALIFIER);
            condition.setValue( oldDetailsBytes );

            // Create the mutation that only performs the update if the details haven't changed.
            final ConditionalMutation mutation = new ConditionalMutation(ROW_ID);
            mutation.addCondition( condition );
            final byte[] newDetailsBytes = serializer.serialize(newDetails);
            mutation.put(COL_FAMILY, COL_QUALIFIER, new Value(newDetailsBytes));

            // Do the write.
            writer = connector.createConditionalWriter(detailsTableName, new ConditionalWriterConfig());
            final Result result = writer.write(mutation);
            switch(result.getStatus()) {
                case REJECTED:
                case VIOLATED:
                    throw new ConcurrentUpdateException("Could not update the details for the Rya instance named '" +
                            instanceName + "' because the old value is out of date.");
                case UNKNOWN:
                case INVISIBLE_VISIBILITY:
                    throw new RyaDetailsRepositoryException("Could not update the details for the Rya instance named '" + instanceName + "'.");
            }
        } catch (final TableNotFoundException | AccumuloException | AccumuloSecurityException e) {
            throw new RyaDetailsRepositoryException("Could not update the details for the Rya instance named '" + instanceName + "'.");
        } finally {
            if(writer != null) {
                writer.close();
            }
        }
    }
}