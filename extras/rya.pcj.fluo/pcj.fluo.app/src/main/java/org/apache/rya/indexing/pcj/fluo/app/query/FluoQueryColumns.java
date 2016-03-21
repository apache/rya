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
package org.apache.rya.indexing.pcj.fluo.app.query;

import io.fluo.api.data.Column;

/**
 * Holds {@link Column} objects that represent where each piece of metadata is stored.
 * <p>
 * See the table bellow for information specific to each metadata model.
 * <p>
 *   <b>Query Metadata</b>
 *    <table border="1" style="width:100%">
 *     <tr> <th>Fluo Row</td>  <th>Fluo Column</td>  <th>Fluo Value</td> </tr>
 *     <tr> <td>Node ID</td> <td>queryMetadata:nodeId</td> <td>The Node ID of the Query.</td> </tr>
 *     <tr> <td>Node ID</td> <td>queryMetadata:variableOrder</td> <td>The Variable Order binding sets are emitted with.</td> </tr>
 *     <tr> <td>Node ID</td> <td>queryMetadata:sparql</td> <td>The original SPARQL query that is being computed by this query..</td> </tr>
 *     <tr> <td>Node ID</td> <td>queryMetadata:childNodeId</td> <td>The Node ID of the child who feeds this node.</td> </tr>
 *     <tr> <td>Node ID + DELIM + Binding Set String</td> <td>queryMetadata:bindingSet</td> <td>A Binding Set that matches the query.</td> </tr>
 *    </table>
 * </p>
 * <p>
 *   <b>Filter Metadata</b>
 *   <table border="1" style="width:100%">
 *     <tr> <th>Fluo Row</td>  <th>Fluo Column</td>  <th>Fluo Value</td> </tr>
 *     <tr> <td>Node ID</td> <td>filterMetadata:nodeId</td> <td>The Node ID of the Filter.</td> </tr>
 *     <tr> <td>Node ID</td> <td>filterMetadata:veriableOrder</td> <td>The Variable Order binding sets are emitted with.</td> </tr>
 *     <tr> <td>Node ID</td> <td>filterMetadata:originalSparql</td> <td>The original SPRAQL query this filter was derived from.</td> </tr>
 *     <tr> <td>Node ID</td> <td>filterMetadata:filterIndexWithinSparql</td> <td>Indicates which filter within the original SPARQL query this represents.</td> </tr>
 *     <tr> <td>Node ID</td> <td>filterMetadata:parentNodeId</td> <td>The Node ID this filter emits Binding Sets to.</td> </tr>
 *     <tr> <td>Node ID</td> <td>filterMetadata:childNodeId</td> <td>The Node ID of the node that feeds this node Binding Sets.</td> </tr>
 *     <tr> <td>Node ID + DELIM + Binding set String </td> <td>filterMetadata:bindingSet</td> <td>A Binding Set that matches the Filter.</td> </tr>
 *   </table>
 * <p>
 * <p>
 *   <b>Join Metadata</b>
 *   <table border="1" style="width:100%">
 *     <tr> <th>Fluo Row</td>  <th>Fluo Column</td>  <th>Fluo Value</td> </tr>
 *     <tr> <td>Node ID</td> <td>joinMetadata:nodeId</td> <td>The Node ID of the Join.</td> </tr>
 *     <tr> <td>Node ID</td> <td>joinMetadata:variableOrder</td> <td>The Variable Order binding sets are emitted with.</td> </tr>
 *     <tr> <td>Node ID</td> <td>joinMetadata:joinType</td> <td>The Join algorithm that will be used when computing join results.</td> </tr>
 *     <tr> <td>Node ID</td> <td>joinMetadata:parentNodeId</td> <td>The Node ID this join emits Binding Sets to.</td> </tr>
 *     <tr> <td>Node ID</td> <td>joinMetadata:leftChildNodeId</td> <td>A Node ID of the node that feeds this node Binding Sets.</td> </tr>
 *     <tr> <td>Node ID</td> <td>joinMetadata:rightChildNodeId</td> <td>A Node ID of the node that feeds this node Binding Sets.</td> </tr>
 *     <tr> <td>Node ID + DELIM + Binding set String</td> <td>joinMetadata:bindingSet</td> <td>A Binding Set that matches the Join.</td> </tr>
 *   </table>
 * <p>
 * <p>
 *   <b>Statement Pattern Metadata</b>
 *   <table border="1" style="width:100%">
 *     <tr> <th>Fluo Row</td>  <th>Fluo Column</td>  <th>Fluo Value</td> </tr>
 *     <tr> <td>Node ID</td> <td>statementPatternMetadata:nodeId</td> <td>The Node ID of the Statement Pattern.</td> </tr>
 *     <tr> <td>Node ID</td> <td>statementPatternMetadata:variableOrder</td> <td>The Variable Order binding sets are emitted with.</td> </tr>
 *     <tr> <td>Node ID</td> <td>statementPatternMetadata:pattern</td> <td>The pattern that defines which Statements will be matched.</td> </tr>
 *     <tr> <td>Node ID</td> <td>statementPatternMetadata:parentNodeId</td> <td>The Node ID this statement pattern emits Binding Sets to.</td> </tr>
 *     <tr> <td>Node ID + DELIM + Binding set String</td> <td>statementPatternMetadata:bindingSet</td> <td>A Binding Set that matches the Statement Pattern.</td> </tr>
 *   </table>
 * <p>
 */
public class FluoQueryColumns {

    // Column families used to store query metadata.
    public static final String QUERY_METADATA_CF = "queryMetadata";
    public static final String FILTER_METADATA_CF = "filterMetadata";
    public static final String JOIN_METADATA_CF = "joinMetadata";
    public static final String STATEMENT_PATTERN_METADATA_CF = "statementPatternMetadata";

   /**
    * New triples that have been added to Rya are written as a row in this
    * column so that any queries that include them in their results will be
    * updated.
    * <p>
    * <table border="1" style="width:100%">
    *   <tr> <th>Fluo Row</td>  <th>Fluo Column</td>  <th>Fluo Value</td> </tr>
    *   <tr> <td>Core Rya SPO formatted triple</td> <td>triples:SPO</td> <td>visibility</td> </tr>
    * </table>
    */
   public static final Column TRIPLES = new Column("triples", "SPO");

   /**
    * Stores the name of the Accumulo table the query's results will be stored.
    * The table's structure is defined by Rya's.
    * </p>
    * <table border="1" style="width:100%">
    *   <tr> <th>Fluo Row</td>  <th>Fluo Column</td>  <th>Fluo Value</td> </tr>
    *   <tr> <td>Query ID</td> <td>query:ryaExportTableName</td>
    *        <td>The name of the Accumulo table the results will be exported to using
    *            the Rya PCJ table structure.</td> </tr>
    * </table>
    */
   public static final Column QUERY_RYA_EXPORT_TABLE_NAME = new Column("query", "ryaExportTableName");


   // Sparql to Query ID used to list all queries that are in the system.
   public static final Column QUERY_ID = new Column("sparql", "queryId");

    // Query Metadata columns.
    public static final Column QUERY_NODE_ID = new Column(QUERY_METADATA_CF, "nodeId");
    public static final Column QUERY_VARIABLE_ORDER = new Column(QUERY_METADATA_CF, "variableOrder");
    public static final Column QUERY_SPARQL = new Column(QUERY_METADATA_CF, "sparql");
    public static final Column QUERY_CHILD_NODE_ID = new Column(QUERY_METADATA_CF, "childNodeId");
    public static final Column QUERY_BINDING_SET = new Column(QUERY_METADATA_CF, "bindingSet");

    // Filter Metadata columns.
    public static final Column FILTER_NODE_ID = new Column(FILTER_METADATA_CF, "nodeId");
    public static final Column FILTER_VARIABLE_ORDER = new Column(FILTER_METADATA_CF, "veriableOrder");
    public static final Column FILTER_ORIGINAL_SPARQL = new Column(FILTER_METADATA_CF, "originalSparql");
    public static final Column FILTER_INDEX_WITHIN_SPARQL = new Column(FILTER_METADATA_CF, "filterIndexWithinSparql");
    public static final Column FILTER_PARENT_NODE_ID = new Column(FILTER_METADATA_CF, "parentNodeId");
    public static final Column FILTER_CHILD_NODE_ID = new Column(FILTER_METADATA_CF, "childNodeId");
    public static final Column FILTER_BINDING_SET = new Column(FILTER_METADATA_CF, "bindingSet");

    // Join Metadata columns.
    public static final Column JOIN_NODE_ID = new Column(JOIN_METADATA_CF, "nodeId");
    public static final Column JOIN_VARIABLE_ORDER = new Column(JOIN_METADATA_CF, "variableOrder");
    public static final Column JOIN_TYPE = new Column(JOIN_METADATA_CF, "joinType");
    public static final Column JOIN_PARENT_NODE_ID = new Column(JOIN_METADATA_CF, "parentNodeId");
    public static final Column JOIN_LEFT_CHILD_NODE_ID = new Column(JOIN_METADATA_CF, "leftChildNodeId");
    public static final Column JOIN_RIGHT_CHILD_NODE_ID = new Column(JOIN_METADATA_CF, "rightChildNodeId");
    public static final Column JOIN_BINDING_SET = new Column(JOIN_METADATA_CF, "bindingSet");

    // Statement Pattern Metadata columns.
    public static final Column STATEMENT_PATTERN_NODE_ID = new Column(STATEMENT_PATTERN_METADATA_CF, "nodeId");
    public static final Column STATEMENT_PATTERN_VARIABLE_ORDER = new Column(STATEMENT_PATTERN_METADATA_CF, "variableOrder");
    public static final Column STATEMENT_PATTERN_PATTERN = new Column(STATEMENT_PATTERN_METADATA_CF, "pattern");
    public static final Column STATEMENT_PATTERN_PARENT_NODE_ID = new Column(STATEMENT_PATTERN_METADATA_CF, "parentNodeId");
    public static final Column STATEMENT_PATTERN_BINDING_SET = new Column(STATEMENT_PATTERN_METADATA_CF, "bindingSet");
}