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
package org.apache.rya.indexing.pcj.fluo.client.util;

import static com.google.common.base.Preconditions.checkNotNull;

import java.text.NumberFormat;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.apache.rya.indexing.pcj.fluo.client.util.Report.ReportItem;
import org.apache.rya.indexing.pcj.storage.PcjMetadata;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;
import org.openrdf.queryrender.sparql.SPARQLQueryRenderer;

/**
 * Pretty renders the state of a query's {@link PcjMetadata}.
 */
public class PcjMetadataRenderer {

    /**
     * Pretty render the state of a PCJ.
     *
     * @param queryId - The ID of the query the metadata is from. (not null)
     * @param metadata - Metadata about one of the PCJs. (not null)
     * @return A pretty render of a PCJ's state.
     * @throws Exception The SPARQL within the metadata could not be parsed or
     *  it could not be rendered.
     */
    public String render(final String queryId, final PcjMetadata metadata) throws Exception {
        checkNotNull(metadata);

        // Pretty format the cardinality.
        final String cardinality = NumberFormat.getInstance().format( metadata.getCardinality() );

        // Pretty format and split the SPARQL query into lines.
        final SPARQLParser parser = new SPARQLParser();
        final SPARQLQueryRenderer renderer = new SPARQLQueryRenderer();
        final ParsedQuery pq = parser.parseQuery(metadata.getSparql(), null);
        final String prettySparql = renderer.render(pq);
        final String[] sparqlLines = StringUtils.split(prettySparql, '\n');

        // Split the variable orders into lines.
        final String[] varOrderLines = new String[ metadata.getVarOrders().size() ];
        int i = 0;
        for(final VariableOrder varOrder : metadata.getVarOrders()) {
            varOrderLines[i++] = varOrder.toString();
        }

        // Create the report.
        final Report.Builder builder = Report.builder();
        builder.appendItem( new ReportItem("Query ID", queryId) );
        builder.appendItem( new ReportItem("Cardinality", cardinality) );
        builder.appendItem( new ReportItem("Export Variable Orders", varOrderLines));
        builder.appendItem( new ReportItem("SPARQL",  sparqlLines) );
        return builder.build().toString();
    }

    /**
     * Pretty render the state of many PCJs.
     *
     * @param metadata - The PCJ states to render. The key within the map is
     *   the statem's Query Id. (not null)
     * @return A pretty render of a PCJs' state.
     * @throws Exception A SPARQL within the metadata could not be parsed or
     *  it could not be rendered.
     */
    public String render(final Map<String, PcjMetadata> metadata) throws Exception {
        checkNotNull(metadata);

        final StringBuilder formatted = new StringBuilder();

        for(final Entry<String, PcjMetadata> entry : metadata.entrySet()) {
            final String formattedQuery = render(entry.getKey(), entry.getValue());
            formatted.append(formattedQuery).append("\n");
        }

        return formatted.toString();
    }
}