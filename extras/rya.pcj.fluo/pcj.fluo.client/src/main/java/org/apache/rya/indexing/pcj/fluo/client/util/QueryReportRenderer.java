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

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

import org.apache.commons.lang3.StringUtils;
import org.apache.rya.indexing.pcj.fluo.api.GetQueryReport.QueryReport;
import org.apache.rya.indexing.pcj.fluo.app.query.FilterMetadata;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQuery;
import org.apache.rya.indexing.pcj.fluo.app.query.JoinMetadata;
import org.apache.rya.indexing.pcj.fluo.app.query.QueryMetadata;
import org.apache.rya.indexing.pcj.fluo.app.query.StatementPatternMetadata;
import org.apache.rya.indexing.pcj.fluo.client.util.Report.ReportItem;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;
import org.openrdf.queryrender.sparql.SPARQLQueryRenderer;

/**
 * Pretty renders a {@link QueryReport}.
 */
@DefaultAnnotation(NonNull.class)
public class QueryReportRenderer {

    /**
     * Pretty render a {@link QueryReport}.
     *
     * @param queryReport - The report that will be rendered. (not null)
     * @return A pretty render of the report.
     * @throws Exception Indicates the SPARQL could not be rendered for some reason.
     */
    public String render(final QueryReport queryReport) throws Exception {
        checkNotNull(queryReport);

        final Report.Builder builder = Report.builder();

        final FluoQuery metadata = queryReport.getFluoQuery();

        final QueryMetadata queryMetadata = metadata.getQueryMetadata();
        builder.appendItem( new ReportItem("QUERY NODE") );
        builder.appendItem( new ReportItem("Node ID", queryMetadata.getNodeId()) );
        builder.appendItem( new ReportItem("Variable Order", queryMetadata.getVariableOrder().toString()) );
        builder.appendItem( new ReportItem("SPARQL", prettyFormatSparql( queryMetadata.getSparql()) ) );
        builder.appendItem( new ReportItem("Child Node ID", queryMetadata.getChildNodeId()) );
        builder.appendItem( new ReportItem("Count", "" + queryReport.getCount(queryMetadata.getNodeId())) );

        for(final FilterMetadata filterMetadata : metadata.getFilterMetadata()) {
            builder.appendItem( new ReportItem("") );

            builder.appendItem( new ReportItem("FILTER NODE") );
            builder.appendItem( new ReportItem("Node ID", filterMetadata.getNodeId()) );
            builder.appendItem( new ReportItem("Variable Order", filterMetadata.getVariableOrder().toString()) );
            builder.appendItem( new ReportItem("Original SPARQL", prettyFormatSparql(  filterMetadata.getOriginalSparql()) ) );
            builder.appendItem( new ReportItem("Filter Index", "" + filterMetadata.getFilterIndexWithinSparql()) );
            builder.appendItem( new ReportItem("Parent Node ID", filterMetadata.getParentNodeId()) );
            builder.appendItem( new ReportItem("Child Node ID", filterMetadata.getChildNodeId()) );
            builder.appendItem( new ReportItem("Count", "" + queryReport.getCount(filterMetadata.getNodeId())) );
        }

        for(final JoinMetadata joinMetadata : metadata.getJoinMetadata()) {
            builder.appendItem( new ReportItem("") );

            builder.appendItem( new ReportItem("JOIN NODE") );
            builder.appendItem( new ReportItem("Node ID", joinMetadata.getNodeId()) );
            builder.appendItem( new ReportItem("Variable Order", joinMetadata.getVariableOrder().toString()) );
            builder.appendItem( new ReportItem("Parent Node ID", joinMetadata.getParentNodeId()) );
            builder.appendItem( new ReportItem("Left Child Node ID", joinMetadata.getLeftChildNodeId()) );
            builder.appendItem( new ReportItem("Right Child Node ID", joinMetadata.getRightChildNodeId()) );
            builder.appendItem( new ReportItem("Count", "" + queryReport.getCount(joinMetadata.getNodeId())) );
        }

        for(final StatementPatternMetadata spMetadata : metadata.getStatementPatternMetadata()) {
            builder.appendItem( new ReportItem("") );

            builder.appendItem( new ReportItem("STATEMENT PATTERN NODE") );
            builder.appendItem( new ReportItem("Node ID", spMetadata.getNodeId()) );
            builder.appendItem( new ReportItem("Variable Order", spMetadata.getVariableOrder().toString()) );
            builder.appendItem( new ReportItem("Statement Pattern", spMetadata.getStatementPattern()) );
            builder.appendItem( new ReportItem("Parent Node ID", spMetadata.getParentNodeId()) );
            builder.appendItem( new ReportItem("Count", "" + queryReport.getCount(spMetadata.getNodeId())) );
        }

        return builder.build().toString();
    }

    private String[] prettyFormatSparql(final String sparql) throws Exception {
        final SPARQLParser parser = new SPARQLParser();
        final SPARQLQueryRenderer renderer = new SPARQLQueryRenderer();
        final ParsedQuery pq = parser.parseQuery(sparql, null);
        final String prettySparql = renderer.render(pq);
        final String[] sparqlLines = StringUtils.split(prettySparql, '\n');
        return sparqlLines;
    }
}