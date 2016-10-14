package org.apache.rya.accumulo;

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



import org.apache.rya.api.RdfCloudTripleStoreConstants;
import org.apache.rya.api.resolver.triple.TripleRow;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import static org.apache.rya.api.RdfCloudTripleStoreConstants.EMPTY_BYTES;

/**
 * Class AccumuloRdfUtils
 * Date: Mar 1, 2012
 * Time: 7:15:54 PM
 */
public class AccumuloRdfUtils {
    private static final Log logger = LogFactory.getLog(AccumuloRdfUtils.class);

    public static void createTableIfNotExist(TableOperations tableOperations, String tableName) throws AccumuloException, AccumuloSecurityException, TableExistsException {
        boolean tableExists = tableOperations.exists(tableName);
        if (!tableExists) {
            logger.debug("Creating accumulo table: " + tableName);
            tableOperations.create(tableName);
        }
    }

    public static Key from(TripleRow tripleRow) {
        return new Key(defaultTo(tripleRow.getRow(), EMPTY_BYTES),
                defaultTo(tripleRow.getColumnFamily(), EMPTY_BYTES),
                defaultTo(tripleRow.getColumnQualifier(), EMPTY_BYTES),
                defaultTo(tripleRow.getColumnVisibility(), EMPTY_BYTES),
                defaultTo(tripleRow.getTimestamp(), Long.MAX_VALUE));
    }

    public static Value extractValue(TripleRow tripleRow) {
        return new Value(defaultTo(tripleRow.getValue(), EMPTY_BYTES));
    }

    private static byte[] defaultTo(byte[] bytes, byte[] def) {
        return bytes != null ? bytes : def;
    }

    private static Long defaultTo(Long l, Long def) {
        return l != null ? l : def;
    }
}
