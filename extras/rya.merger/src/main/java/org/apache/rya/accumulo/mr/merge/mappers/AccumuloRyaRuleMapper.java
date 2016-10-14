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

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.persist.RyaDAOException;

/**
 * Rule mapper that inserts any copied statements into a child Accumulo Rya instance, and outputs
 * any raw rows as <Text, Mutation> pairs.
 */
public class AccumuloRyaRuleMapper extends BaseRuleMapper<Text, Mutation> {
    /**
     * Insert a statement into the child Accumulo instance via the child DAO.
     * @param rstmt RyaStatement to add to the child
     * @param context Map context, not used
     * @throws IOException if the DAO encounters an error adding the statement to Accumulo
     */
    @Override
    protected void copyStatement(final RyaStatement rstmt, final Context context) throws IOException {
        try {
            childDao.add(rstmt);
        }
        catch (final RyaDAOException e) {
            throw new IOException("Error inserting statement into child Rya DAO", e);
        }
    }

    /**
     * Output a row via the Hadoop framework (rather than a Rya interface) to the Mapper's configured
     * child table.
     * @param key Row's key
     * @param value Row's value
     * @param context Context to use for writing
     * @throws InterruptedException if the framework is interrupted writing output
     * @throws IOException if the framework encounters an error writing the row to Accumulo
     */
    @Override
    protected void copyRow(final Key key, final Value value, final Context context) throws IOException, InterruptedException {
        final Mutation mutation = new Mutation(key.getRow().getBytes());
        mutation.put(key.getColumnFamily(), key.getColumnQualifier(), key.getColumnVisibilityParsed(), key.getTimestamp(), value);
        context.write(childTableNameText, mutation);
    }
}
