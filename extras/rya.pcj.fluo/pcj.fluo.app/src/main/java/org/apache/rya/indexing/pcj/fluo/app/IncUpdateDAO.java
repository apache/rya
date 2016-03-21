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

import java.util.Map.Entry;

import io.fluo.api.client.FluoClient;
import io.fluo.api.client.Snapshot;
import io.fluo.api.config.ScannerConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.iterator.ColumnIterator;
import io.fluo.api.iterator.RowIterator;
import io.fluo.api.types.TypedTransaction;
import mvm.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.resolver.triple.TripleRow;
import mvm.rya.api.resolver.triple.TripleRowResolverException;
import mvm.rya.api.resolver.triple.impl.WholeRowTripleResolver;

public class IncUpdateDAO {

    private static final StringTypeLayer stl = new StringTypeLayer();
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

    private static String getTripleString(final RyaStatement rs) {
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
        try (TypedTransaction tx = stl.wrap(fluoClient.newTransaction())) {
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
    public static void addRow(final TypedTransaction tx, final String row, final Column col, final String val) {
        checkNotNull(tx);
        tx.mutate().row(row).col(col).set(val);
    }

    /**
     * Print all statements in the repo for demo and diagnostic purposes.
     * @param fluoClient
     * @throws Exception
     */
    public static void printTriples(final FluoClient fluoClient) throws Exception {
        try (Snapshot snapshot = fluoClient.newSnapshot()) {
            final ScannerConfiguration scanConfig = new ScannerConfiguration();
            scanConfig.fetchColumn(Bytes.of("triples"), Bytes.of("SPO"));

            final RowIterator rowIter = snapshot.get(scanConfig);

            while (rowIter.hasNext()) {
                final Entry<Bytes, ColumnIterator> row = rowIter.next();
                System.out.println("Triple: " + row.getKey().toString());
            }
        }
    }

//    /**
//     * Print all bindings for the given queries. For demo's and diagnostics.
//     * @param fluoClient
//     * @param queryNames
//     * @throws Exception
//     */
//    public static void printQueryResults(final FluoClient fluoClient,
//            final Map<String, String> queryNames) throws Exception {
//        try (Snapshot snapshot = fluoClient.newSnapshot();
//                TypedTransaction tx1 = stl.wrap(fluoClient.newTransaction())) {
//
//            final ScannerConfiguration scanConfig = new ScannerConfiguration();
//            scanConfig.fetchColumn(Bytes.of("query"), Bytes.of("bindingSet"));
//
//            final RowIterator rowIter = snapshot.get(scanConfig);
//            String sparqlRow = "";
//            System.out.println("*********************************************************");
//
//            while (rowIter.hasNext()) {
//                final Entry<Bytes, ColumnIterator> row = rowIter.next();
//                final String[] joinInfo = row.getKey().toString()
//                        .split(NODEID_BS_DELIM);
//                final String sparql = joinInfo[0];
//                final String bs = joinInfo[1];
//                if (!sparqlRow.equals(sparql)) {
//                    sparqlRow = sparql;
//                    System.out.println();
//                    System.out.println();
//                    System.out.println(queryNames.get(sparqlRow)
//                            + " has bindings: ");
//                    System.out.println();
//                }
//
//                final String variables = tx1.get().row(sparqlRow).col(NODE_VARS).toString();
//                final String[] vars = variables.split(";");
//                final String[] bsVals = bs.split(DELIM);
//                System.out.print("Bindingset:  ");
//                for (int i = 0; i < vars.length; i++) {
//                    System.out.print(vars[i] + " = " + bsVals[i] + "   ");
//                }
//                System.out.println();
//
//            }
//
//            System.out.println("*********************************************************");
//        }
//    }

    /**
     * Print all rows in the Fluo table for diagnostics.
     * @param fluoClient
     * @throws Exception
     */
    public static void printAll(final FluoClient fluoClient) throws Exception {
        final String FORMAT = "%-30s | %-10s | %-10s | %-40s\n";
        System.out
        .println("Printing all tables.  Showing unprintable bytes and braces as {ff} and {{} and {}} where ff is the value in hexadecimal.");
        System.out.format(FORMAT, "--Row--", "--Column Family--",
                "--Column Qual--", "--Value--");
        // Use try with resource to ensure snapshot is closed.
        try (Snapshot snapshot = fluoClient.newSnapshot()) {
            final ScannerConfiguration scanConfig = new ScannerConfiguration();
            // scanConfig.setSpan(Span.prefix("word:"));

            final RowIterator rowIter = snapshot.get(scanConfig);

            while (rowIter.hasNext()) {
                final Entry<Bytes, ColumnIterator> row = rowIter.next();
                final ColumnIterator colIter = row.getValue();
                while (colIter.hasNext()) {
                    final Entry<Column, Bytes> column = colIter.next();
                    // System.out.println(row.getKey() + " " +
                    // column.getKey().getFamily()+ " " +
                    // column.getKey().getQualifier() );
                    System.out.format(FORMAT, to_String(row.getKey()),
                            to_String(column.getKey().getFamily()),
                            to_String(column.getKey().getQualifier()),
                            to_String(column.getValue()));
                }
            }
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