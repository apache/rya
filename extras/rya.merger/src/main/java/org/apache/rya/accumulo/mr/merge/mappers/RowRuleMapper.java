/*
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
package org.apache.rya.accumulo.mr.merge.mappers;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;

import org.apache.rya.accumulo.mr.MRUtils;
import org.apache.rya.accumulo.mr.merge.MergeTool;
import org.apache.rya.accumulo.mr.merge.util.AccumuloRyaUtils;
import org.apache.rya.accumulo.mr.merge.util.GroupedRow;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.persist.RyaDAOException;

/**
 * Rule mapper that outputs statements as serialized Accumulo rows along with their destination
 * table names, using {@link GroupedRow} as a composite Writable to represent an output
 * of <(table, Key), (Key, Value)>.
 *
 * In order to ensure proper indexing, the statements are first
 * inserted into a mock Rya instance in memory, and the tables that make up that instance are
 * periodically flushed to the output.
 */
public class RowRuleMapper extends BaseRuleMapper<GroupedRow, GroupedRow> {
    /**
     * The number of statements each mapper will hold in memory before flushing the serialized rows to the output.
     */
    public static final String MAX_STATEMENTS_PROP = "ac.copy.cache.statements.max";

    /**
     * The default number of statements each mapper will hold in memory before flushing the serialized rows to the output.
     */
    public static final int MAX_STATEMENTS_DEFAULT = 10000;

    private static final Logger log = Logger.getLogger(RowRuleMapper.class);
    private final GroupedRow compositeKey = new GroupedRow();
    private final GroupedRow compositeVal = new GroupedRow();
    private int cachedStatements = 0;
    private int maxStatements;

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {
        super.setup(context);
        // Set up a mock child DAO for caching serialized statements
        childAccumuloRdfConfiguration.setBoolean(MRUtils.AC_MOCK_PROP, true);
        childUser = "root";
        childAccumuloRdfConfiguration.set(MRUtils.AC_USERNAME_PROP, childUser);
        childAccumuloRdfConfiguration.set(MRUtils.AC_PWD_PROP, "");
        MergeTool.setDuplicateKeys(childAccumuloRdfConfiguration);
        childConnector = AccumuloRyaUtils.setupConnector(childAccumuloRdfConfiguration);
        childDao = AccumuloRyaUtils.setupDao(childConnector, childAccumuloRdfConfiguration);
        copyAuthorizations();
        addMetadataKeys(context); // call in super.setup() does nothing, has to be done after DAO is initialized
        // Determine the size of the cache
        maxStatements = childAccumuloRdfConfiguration.getInt(MAX_STATEMENTS_PROP, MAX_STATEMENTS_DEFAULT);
    }

    @Override
    protected void cleanup(final Context context) throws IOException, InterruptedException {
        flush(context);
        super.cleanup(context);
    }

    /**
     * Add a statement to an in-memory Accumulo instance, serialized as spo/pos/osp and any applicable secondary
     * indexes, and flush the in-memory rows through to the output if enough statements have been cached.
     * @param rstmt RyaStatement to copy to the child
     * @param context Context to use for writing
     * @throws InterruptedException if Hadoop is interrupted while writing output.
     * @throws IOException if an error is encountered serializing and storing the statement in memory, or
     *      if Hadoop reports an error writing the in-memory tables to the output.
     */
    @Override
    protected void copyStatement(final RyaStatement rstmt, final Context context) throws IOException, InterruptedException {
        try {
            childDao.add(rstmt);
            cachedStatements++;
        } catch (final RyaDAOException e) {
            throw new IOException("Error serializing RyaStatement", e);
        }
        if (cachedStatements >= maxStatements) {
            flush(context);
        }
    }

    /**
     * Copy a row directly -- just pass it through to the map output.
     * @param Key Row's key
     * @param Value Row's value
     * @param context Context to use for writing
     * @throws InterruptedException If Hadoop is interrupted during map output
     * @throws IOException If Hadoop reports an error writing the output
     */
    @Override
    protected void copyRow(final Key key, final Value value, final Context context) throws IOException, InterruptedException {
        compositeKey.setGroup(childTableName);
        compositeKey.setKey(key);
        compositeVal.setKey(key);
        compositeVal.setValue(value);
        context.write(compositeKey, compositeVal);
    }

    /**
     * Insert copy tool metadata, if the in-memory instance has been configured.
     */
    @Override
    protected void addMetadataKeys(final Context context) throws IOException {
        try {
            if (childDao != null && childDao.isInitialized()) {
                if (runTime != null) {
                    final RyaStatement ryaStatement = AccumuloRyaUtils.createCopyToolRunTimeRyaStatement(runTime);
                    copyStatement(ryaStatement, context);
                }
                if (startTime != null) {
                    final RyaStatement ryaStatement = AccumuloRyaUtils.createCopyToolSplitTimeRyaStatement(startTime);
                    copyStatement(ryaStatement, context);
                }
                if (timeOffset != null) {
                    final RyaStatement ryaStatement = AccumuloRyaUtils.createTimeOffsetRyaStatement(timeOffset);
                    copyStatement(ryaStatement, context);
                }
            }
        } catch (RyaDAOException | IOException | InterruptedException e) {
            throw new IOException("Failed to write metadata key", e);
        }
    }

    private void flush(final Context context) throws IOException, InterruptedException {
        try {
            childDao.flush();
        } catch (final RyaDAOException e) {
            throw new IOException("Error writing to in-memory table", e);
        }
        final TableOperations ops = childConnector.tableOperations();
        final SecurityOperations secOps = childConnector.securityOperations();
        Authorizations childAuths;
        try {
            childAuths = secOps.getUserAuthorizations(childUser);
        } catch (AccumuloException | AccumuloSecurityException e) {
            throw new IOException("Error connecting to mock instance", e);
        }
        for (final String table : ops.list()) {
            // Only copy Rya tables (skip system tables)
            if (!table.startsWith(childTablePrefix)) {
                continue;
            }
            compositeKey.setGroup(table);
            try {
                // Output every row in this mock table
                int rows = 0;
                final Scanner scanner = childDao.getConnector().createScanner(table, childAuths);
                for (final Map.Entry<Key, Value> row : scanner) {
                    compositeKey.setKey(row.getKey());
                    compositeVal.setKey(row.getKey());
                    compositeVal.setValue(row.getValue());
                    context.write(compositeKey, compositeVal);
                    rows++;
                }
                log.info("Flushed " + rows + " in-memory rows to output (" + table + ").");
                // Then clear the table
                if (rows > 0) {
                    ops.deleteRows(table, null, null);
                }
            } catch (TableNotFoundException | AccumuloException | AccumuloSecurityException e) {
                throw new IOException("Error flushing in-memory table", e);
            }
        }
        // All tables should be empty
        cachedStatements = 0;
    }
}
