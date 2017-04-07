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
package org.apache.rya.indexing.pcj.fluo.app;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.DELIM;
import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.TYPE_DELIM;
import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.URI_TYPE;

import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.Snapshot;
import org.apache.fluo.api.client.SnapshotBase;
import org.apache.fluo.api.client.Transaction;
import org.apache.fluo.api.client.scanner.CellScanner;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumnValue;
import org.apache.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.resolver.triple.TripleRow;
import org.apache.rya.api.resolver.triple.TripleRowResolverException;
import org.apache.rya.api.resolver.triple.impl.WholeRowTripleResolver;

public class IncUpdateDAO {

    private static final WholeRowTripleResolver tr = new WholeRowTripleResolver();

    public static RyaStatement deserializeTriple(final Bytes row) {
        final byte[] rowArray = row.toArray();

        RyaStatement rs = null;
        try {
            rs = tr.deserialize(TABLE_LAYOUT.SPO, new TripleRow(rowArray, new byte[0], new byte[0]));
        } catch (final TripleRowResolverException e) {
            e.printStackTrace();
        }

        return rs;
    }

    public static String getTripleString(final RyaStatement rs) {
        final String subj = rs.getSubject().getData() + TYPE_DELIM + URI_TYPE;
        final String pred = rs.getPredicate().getData() + TYPE_DELIM + URI_TYPE;
        final String objData = rs.getObject().getData();
        final String objDataType = rs.getObject().getDataType().stringValue();

        return subj + DELIM + pred + DELIM + objData + TYPE_DELIM + objDataType;
    }

    public static String getTripleString(final Bytes row) {
        return getTripleString(deserializeTriple(row));
    }

    /**
     * Add a row, creating and closing a transaction.
     *
     * @param fluoClient - Creates connections to the Fluo table that will be written to. (not null)
     * @param row - The Row ID.
     * @param col - The Column.
     * @param val - The value.
     */
    public static void addRow(final FluoClient fluoClient, final String row, final Column col, final String val) {
        checkNotNull(fluoClient);
        try (Transaction tx = fluoClient.newTransaction()) {
            addRow(tx, row, col, val);
            tx.commit();
        }
    }

    /**
     * Writes an entry to the Fluo table.
     *
     * @param tx - The transaction that will be used. (not null)
     * @param row - The Row ID.
     * @param col - The Column.
     * @param val - The value.
     */
    public static void addRow(final Transaction tx, final String row, final Column col, final String val) {
        checkNotNull(tx);
        tx.set(row, col, val);
    }

    /**
     * Print all statements in the repo for demo and diagnostic purposes.
     * @param fluoClient
     * @throws Exception
     */
    public static void printTriples(final FluoClient fluoClient) throws Exception {
        try (Snapshot snapshot = fluoClient.newSnapshot()) {
        	final CellScanner cscanner = snapshot.scanner().fetch(new Column("triples", "SPO")).build();
        	for (final RowColumnValue rcv : cscanner) {
        		System.out.println("Triple: "+rcv.getsRow());
			}
        }
    }

    /**
     * Print all rows in the Fluo table for diagnostics.
     * </p>
     * Consider using {@code FluoITHelper.printFluoTable(FluoClient client)} instead.
     */
    @Deprecated
    public static void printAll(final SnapshotBase sx) {
        final String FORMAT = "%-30s | %-10s | %-10s | %-40s\n";
        System.out.println("Printing all tables.  Showing unprintable bytes and braces as {ff} and {{} and {}} where ff is the value in hexadecimal.");
        System.out.format(FORMAT, "--Row--", "--Column Family--", "--Column Qual--", "--Value--");
        final CellScanner cscanner = sx.scanner().build();
        for (final RowColumnValue rcv : cscanner) {
            System.out.format(FORMAT, to_String(rcv.getRow()),
                    to_String(rcv.getColumn().getFamily()),
                    to_String(rcv.getColumn().getQualifier()),
                    to_String(rcv.getValue()));
        }
    }

    /**
     * Print all rows in the Fluo table for diagnostics.
     * </p>
     * Consider using {@code FluoITHelper.printFluoTable(FluoClient client)} instead.
     */
    @Deprecated
    public static void printAll(final FluoClient fluoClient) throws Exception {
        try(Snapshot sx = fluoClient.newSnapshot()) {
            printAll(sx);
        }
    }

    /**
     * convert a non-utf8 to string and show unprintable bytes as {xx} where x
     * is hex.
     *
     * @param value
     * @return
     */
    static String to_String(final Bytes bytes) {
        return to_String(bytes.toArray());
    }

    static String to_String(final byte[] bytes) {
        final StringBuilder sb = new StringBuilder();
        for (final byte b : bytes) {
            if ((b > 0x7e) || (b < 32)) {
                sb.append("{");
                sb.append(Integer.toHexString(b & 0xff)); // Lop off the sign extended ones.
                sb.append("}");
            } else if (b == '{' || b == '}') { // Escape the literal braces.
                sb.append("{");
                sb.append((char) b);
                sb.append("}");
            } else {
                sb.append((char) b);
            }
        }
        return sb.toString();
    }
}