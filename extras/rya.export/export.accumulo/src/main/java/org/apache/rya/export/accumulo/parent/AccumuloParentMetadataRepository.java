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
package org.apache.rya.export.accumulo.parent;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.lexicoder.DateLexicoder;
import org.apache.accumulo.core.client.lexicoder.Lexicoder;
import org.apache.accumulo.core.client.lexicoder.LongLexicoder;
import org.apache.accumulo.core.client.lexicoder.StringLexicoder;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.rya.export.api.MergerException;
import org.apache.rya.export.api.parent.MergeParentMetadata;
import org.apache.rya.export.api.parent.ParentMetadataDoesNotExistException;
import org.apache.rya.export.api.parent.ParentMetadataExistsException;
import org.apache.rya.export.api.parent.ParentMetadataRepository;

import mvm.rya.accumulo.AccumuloRyaDAO;
import mvm.rya.accumulo.mr.MRUtils;

/**
 * Accumulo repository for metadata pertaining to the parent database.  This
 * will contain all information to identify where any data was exported from.
 * <p>
 * The data found here is:
 * <li>Parent database Rya Instance Name</li>
 * <li>Timestamp used as the lower cutoff for the export</li>
 */
public class AccumuloParentMetadataRepository implements ParentMetadataRepository {
    private static final Logger log = Logger.getLogger(AccumuloParentMetadataRepository.class);

    /**
     * The Row ID of all {@link MergeParentMetadata} entries that are stored in Accumulo.
     */
    private static final Text MERGE_PARENT_METADATA_ROW_ID = new Text("MergeParentMetadata");

    /**
     * The Column Family for all MergeParentMetadata entries.
     */
    private static final Text MERGE_PARENT_METADATA_FAMILY = new Text("mergeParentMetadata");

    /**
     * The Column Qualifier for the Rya instance name.
     */
    private static final Text MERGE_PARENT_METADATA_RYA_INSTANCE_NAME = new Text("ryaInstanceName");

    /**
     * The Column Qualifier for the copy tool timestamp.
     */
    private static final Text MERGE_PARENT_METADATA_TIMESTAMP = new Text("timestamp");

    /**
     * The Column Qualifier for the copy tool filter timestamp.
     */
    private static final Text MERGE_PARENT_METADATA_FILTER_TIMESTAMP = new Text("filterTimestamp");

    /**
     * The Column Qualifier for the parent time offset.
     */
    private static final Text MERGE_PARENT_METADATA_PARENT_TIME_OFFSET = new Text("parentTimeOffset");

    // Lexicoders used to read/write MergeParentMetadata to/from Accumulo.
    private static final LongLexicoder LONG_LEXICODER = new LongLexicoder();
    private static final StringLexicoder STRING_LEXICODER = new StringLexicoder();
    private static final DateLexicoder DATE_LEXICODER = new DateLexicoder();

    private static final String BASE_MERGE_METADATA_TABLE_NAME = "merge_metadata";

    private final AccumuloRyaDAO accumuloRyaDao;
    private final Connector connector;
    private final String tablePrefix;
    private final String mergeParentMetadataTableName;
    private MergeParentMetadata metadata = null;

    /**
     * Creates a new instance of {@link AccumuloParentMetadataRepository}.
     * @param accumuloRyaDao the {@link AccumuloRyaDAO}. (not {@code null})
     */
    public AccumuloParentMetadataRepository(final AccumuloRyaDAO accumuloRyaDao) {
        this.accumuloRyaDao = checkNotNull(accumuloRyaDao);
        connector = accumuloRyaDao.getConnector();
        tablePrefix = accumuloRyaDao.getConf().getTablePrefix();
        mergeParentMetadataTableName = tablePrefix + BASE_MERGE_METADATA_TABLE_NAME;
    }

    @Override
    public MergeParentMetadata get() throws ParentMetadataDoesNotExistException {
        if (metadata == null) {
            metadata = getMetadataFromTable();
        }
        return metadata;
    }

    private MergeParentMetadata getMetadataFromTable() throws ParentMetadataDoesNotExistException {
        try {
            // Create an Accumulo scanner that iterates through the metadata entries.
            final Scanner scanner = connector.createScanner(mergeParentMetadataTableName, new Authorizations());
            final Iterator<Entry<Key, Value>> entries = scanner.iterator();

            // No metadata has been stored in the table yet.
            if (!entries.hasNext()) {
                log.error("Could not find any MergeParentMetadata metadata in the table named: " + mergeParentMetadataTableName);
            }

            // Fetch the metadata from the entries.
            String ryaInstanceName = null;
            Date timestamp = null;
            Date filterTimestamp = null;
            Long parentTimeOffset = null;

            while (entries.hasNext()) {
                final Entry<Key, Value> entry = entries.next();
                final Text columnQualifier = entry.getKey().getColumnQualifier();
                final byte[] value = entry.getValue().get();

                if (columnQualifier.equals(MERGE_PARENT_METADATA_RYA_INSTANCE_NAME)) {
                    ryaInstanceName = STRING_LEXICODER.decode(value);
                } else if (columnQualifier.equals(MERGE_PARENT_METADATA_TIMESTAMP)) {
                    timestamp = DATE_LEXICODER.decode(value);
                } else if (columnQualifier.equals(MERGE_PARENT_METADATA_FILTER_TIMESTAMP)) {
                    filterTimestamp = DATE_LEXICODER.decode(value);
                } else if (columnQualifier.equals(MERGE_PARENT_METADATA_PARENT_TIME_OFFSET)) {
                    parentTimeOffset = LONG_LEXICODER.decode(value);
                }
            }

            return new MergeParentMetadata(ryaInstanceName, timestamp, filterTimestamp, parentTimeOffset);
        } catch (final TableNotFoundException e) {
            throw new ParentMetadataDoesNotExistException("Could not add results to a MergeParentMetadata because the MergeParentMetadata table does not exist.", e);
        } catch (final Exception e) {
            throw new ParentMetadataDoesNotExistException("Error occurred while getting merge parent metadata.", e);
        }
    }

    @Override
    public void set(final MergeParentMetadata metadata) throws ParentMetadataExistsException {
        try {
            createTableIfNeeded();
            writeMetadata(metadata);
        } catch (final MergerException e) {
            throw new ParentMetadataExistsException("Unable to set MergeParentMetadata", e);
        }
    }

    private void createTableIfNeeded() throws MergerException {
        try {
            if (!doesMetadataTableExist()) {
                log.debug("Creating table: " + mergeParentMetadataTableName);
                connector.tableOperations().create(mergeParentMetadataTableName);
                log.debug("Created table: " + mergeParentMetadataTableName);
                log.debug("Granting authorizations to table: " + mergeParentMetadataTableName);
                final String username = accumuloRyaDao.getConf().get(MRUtils.AC_USERNAME_PROP);
                connector.securityOperations().grantTablePermission(username, mergeParentMetadataTableName, TablePermission.WRITE);
                log.debug("Granted authorizations to table: " + mergeParentMetadataTableName);
            }
        } catch (final TableExistsException | AccumuloException | AccumuloSecurityException e) {
            throw new MergerException("Could not create a new MergeParentMetadata table named: " + mergeParentMetadataTableName, e);
        }
    }

    private boolean doesMetadataTableExist() {
        return connector.tableOperations().exists(mergeParentMetadataTableName);
    }

    /**
     * Create the {@link Mutation}s required to write a
     * {@link MergerParentMetadata} object to an Accumulo table.
     * @param metadata - The {@link MergeParentMetadata} to write.
     * (not {@code null})
     * @return An ordered list of mutations that write the metadata to an
     * Accumulo table.
     */
    private static List<Mutation> makeWriteMetadataMutations(final MergeParentMetadata metadata) {
        checkNotNull(metadata);

        final List<Mutation> mutations = new LinkedList<>();

        // Rya Instance Name
        final Mutation ryaInstanceNameMutation = makeFieldMutation(metadata.getRyaInstanceName(), STRING_LEXICODER, MERGE_PARENT_METADATA_RYA_INSTANCE_NAME);
        mutations.add(ryaInstanceNameMutation);

        // Timestamp
        final Mutation timestampMutation= makeFieldMutation(metadata.getTimestamp(), DATE_LEXICODER, MERGE_PARENT_METADATA_TIMESTAMP);
        mutations.add(timestampMutation);

        // Filter Timestamp
        if (metadata.getFilterTimestamp() != null) {
            final Mutation filterTimestampMutation = makeFieldMutation(metadata.getFilterTimestamp(), DATE_LEXICODER, MERGE_PARENT_METADATA_FILTER_TIMESTAMP);
            mutations.add(filterTimestampMutation);
        }

        // Parent Time Offset
        if (metadata.getParentTimeOffset() != null) {
            final Mutation parentTimeOffsetMutation = makeFieldMutation(metadata.getParentTimeOffset(), LONG_LEXICODER, MERGE_PARENT_METADATA_PARENT_TIME_OFFSET);
            mutations.add(parentTimeOffsetMutation);
        }

        return mutations;
    }

    private static <T> Mutation makeFieldMutation(final T object, final Lexicoder<T> lexicoder, final Text columnQualifer) {
        final Mutation mutation = new Mutation(MERGE_PARENT_METADATA_ROW_ID);
        final Value value = new Value(lexicoder.encode(object));
        mutation.put(MERGE_PARENT_METADATA_FAMILY, columnQualifer, value);
        return mutation;
    }

    private void writeMetadata(final MergeParentMetadata metadata) throws MergerException {
        BatchWriter writer = null;
        try{
            // Write each result.
            final List<Mutation> mutations = makeWriteMetadataMutations(metadata);

            writer = connector.createBatchWriter(mergeParentMetadataTableName, new BatchWriterConfig());
            writer.addMutations(mutations);
        } catch (final AccumuloException | TableNotFoundException e) {
            throw new MergerException("Unable to set MergeParentMetadata in Accumulo", e);
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (final MutationsRejectedException e) {
                    throw new MergerException("Could not add results to a MergeParentMetadata table because some of the mutations were rejected.", e);
                }
            }
        }
    }
}