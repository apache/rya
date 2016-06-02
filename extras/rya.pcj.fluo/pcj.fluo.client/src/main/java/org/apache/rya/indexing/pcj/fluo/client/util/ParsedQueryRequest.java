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

import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;

/**
 * Represents a request to create a new PCJ in the Fluo app.
 */
@Immutable
@ParametersAreNonnullByDefault
public class ParsedQueryRequest {

    private final String sparql;
    private final Set<VariableOrder> varOrders;

    /**
     * Constructs an instance of {@link CratePcjRequest}.
     *
     * @param sparql - The SPARQL query to load into the Fluo app. (not null)
     * @param varOrders - The variable orders to export the query's results to. (not null)
     */
    public ParsedQueryRequest(final String sparql, final Set<VariableOrder> varOrders) {
        this.sparql = checkNotNull(sparql);
        this.varOrders = checkNotNull(varOrders);
    }

    /**
     * @return The variable orders to export the query's results to. (not null)
     */
    public Set<VariableOrder> getVarOrders() {
        return varOrders;
    }

    /**
     * @return The SPARQL query to load into the Fluo app.
     */
    public String getQuery() {
        return sparql;
    }

    @Override
    public int hashCode() {
        return Objects.hash(sparql, varOrders);
    }

    @Override
    public boolean equals(final Object o) {
        if(this == o) {
            return true;
        }
        if(o instanceof ParsedQueryRequest) {
        final ParsedQueryRequest request = (ParsedQueryRequest)o;
            return new EqualsBuilder()
                    .append(sparql, request.sparql)
                    .append(varOrders, request.varOrders)
                    .isEquals();
        }
        return false;
    }

    private static final Pattern varOrdersPattern = Pattern.compile("^#prefix (.*?)$", Pattern.MULTILINE);

    /**
     * Create a {@link ParsedQueryRequest} from formatted text.
     * <p>
     * The file may contain a list of variable orders at the head as comments.
     * The file must contain a SPARQL query to loAd into the application.
     * <p>
     * Example file:
     * <pre>
     *     #prefix a, b, c
     *     #prefix b, c, a
     *     SELECT *
     *     WHERE {
     *         ?a &lt;http://talksTo> ?b.
     *         ?b &lt;http://talksTo> ?c.
     *     }
     * </pre>
     * @throws IOException
     */
    public static ParsedQueryRequest parse(final String requestText) throws IOException {
        checkNotNull(requestText);

        // Shift this pointer to the end of each match to find where the SPARQL starts.
        int startOfSparql = 0;

        // Scan for variable orders.
        final Set<VariableOrder> varOrders = new HashSet<>();

        final Matcher matcher = varOrdersPattern.matcher(requestText);
        while(matcher.find()) {
            final String varOrder = matcher.group(1);
            varOrders.add( new VariableOrder( varOrder.split(",\\s*") ) );

            startOfSparql = matcher.end();
        }

        // Pull the SPARQL out.
        final String sparql = requestText.substring(startOfSparql).trim();

        return new ParsedQueryRequest(sparql, varOrders);
    }
}